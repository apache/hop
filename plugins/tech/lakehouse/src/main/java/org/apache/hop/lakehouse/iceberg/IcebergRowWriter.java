/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lakehouse.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.Types;

/**
 * Writes Hop rows into new data files of an Iceberg table. It doesn't commit anything: {@link
 * #complete()} returns the files, and a {@link CommitCoordinator} decides whether they become part
 * of a snapshot. One writer is used per transform copy.
 */
public class IcebergRowWriter implements Closeable {

  /** Iceberg's default target data file size. */
  public static final long DEFAULT_TARGET_FILE_SIZE = 512L * 1024 * 1024;

  private final Schema schema;
  private final List<Types.NestedField> columns;
  private final int[] rowIndexes;
  private final IValueMeta[] valueMetas;
  private final TaskWriter<Record> writer;
  private boolean finished;

  /**
   * @param table the target table
   * @param rowMeta layout of the incoming Hop rows
   * @param copyNr transform copy number, used to keep file names unique across copies
   * @param runId unique id of this pipeline run, also used in file names
   * @param targetFileSize data files are rolled over at about this size
   */
  public IcebergRowWriter(
      Table table, IRowMeta rowMeta, int copyNr, long runId, long targetFileSize)
      throws HopException {
    this.schema = table.schema();
    this.columns = schema.columns();
    this.rowIndexes = new int[columns.size()];
    this.valueMetas = new IValueMeta[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      String name = columns.get(i).name();
      int index = rowMeta.indexOfValue(name);
      if (index < 0 && columns.get(i).isRequired()) {
        throw new HopException(
            "Required column '" + name + "' of table " + table.name() + " is missing in the input");
      }
      rowIndexes[i] = index;
      valueMetas[i] = index < 0 ? null : rowMeta.getValueMeta(index);
    }

    FileIO io = table.io();
    PartitionSpec spec = table.spec();
    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, copyNr, runId).format(FileFormat.PARQUET).build();

    if (spec.isUnpartitioned()) {
      writer =
          new UnpartitionedWriter<>(
              spec, FileFormat.PARQUET, writerFactory, fileFactory, io, targetFileSize);
    } else {
      PartitionKey partitionKey = new PartitionKey(spec, schema);
      InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
      writer =
          new PartitionedFanoutWriter<>(
              spec, FileFormat.PARQUET, writerFactory, fileFactory, io, targetFileSize) {
            @Override
            protected PartitionKey partition(Record row) {
              partitionKey.partition(wrapper.wrap(row));
              return partitionKey;
            }
          };
    }
  }

  public void write(Object[] row) throws HopException {
    GenericRecord record = GenericRecord.create(schema);
    for (int i = 0; i < columns.size(); i++) {
      if (rowIndexes[i] >= 0) {
        Object value = row[rowIndexes[i]];
        record.set(
            i, IcebergTypeMapper.toIcebergValue(valueMetas[i], value, columns.get(i).type()));
      }
    }
    try {
      writer.write(record);
    } catch (IOException e) {
      throw new HopException("Error writing a row to the Iceberg table", e);
    }
  }

  /** Closes the open files and returns every data file written. */
  public List<DataFile> complete() throws HopException {
    finished = true;
    try {
      return Arrays.asList(writer.dataFiles());
    } catch (IOException e) {
      throw new HopException("Error finishing the Iceberg data files", e);
    }
  }

  /** Closes the writer and deletes the files it wrote. Used when the pipeline fails. */
  public void abort() throws HopException {
    finished = true;
    try {
      writer.abort();
    } catch (IOException e) {
      throw new HopException("Error cleaning up Iceberg data files", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      writer.abort();
      finished = true;
    }
  }
}
