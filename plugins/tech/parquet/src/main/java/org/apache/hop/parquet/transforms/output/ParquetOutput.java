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

package org.apache.hop.parquet.transforms.output;

import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ParquetOutput extends BaseTransform<ParquetOutputMeta, ParquetOutputData>
    implements ITransform<ParquetOutputMeta, ParquetOutputData> {

  public ParquetOutput(
      TransformMeta transformMeta,
      ParquetOutputMeta meta,
      ParquetOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    // Pre-calculate some values...
    //
    data.pageSize =
        Const.toInt(resolve(meta.getDataPageSize()), ParquetProperties.DEFAULT_PAGE_SIZE);
    data.dictionaryPageSize =
        Const.toInt(
            resolve(meta.getDictionaryPageSize()), ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
    data.rowGroupSize =
        Const.toInt(
            resolve(meta.getRowGroupSize()), ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
    data.maxSplitSizeRows = Const.toLong(resolve(meta.getFileSplitSize()), -1);

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      closeFile();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.sourceFieldIndexes = new ArrayList<>();
      for (int i = 0; i < meta.getFields().size(); i++) {
        ParquetField field = meta.getFields().get(i);
        int index = getInputRowMeta().indexOfValue(field.getSourceFieldName());
        if (index < 0) {
          throw new HopException("Unable to find source field '" + field.getSourceFieldName());
        }
        data.sourceFieldIndexes.add(index);
      }

      openNewFile();
    }

    // See if we don't need to create a new file split into parts...
    //
    if (meta.isFilenameIncludingSplitNr()
        && data.maxSplitSizeRows > 0
        && data.splitRowCount >= data.maxSplitSizeRows) {
      // Close file and start a new one...
      //
      closeFile();
      openNewFile();
    }

    // Write the row, handled by class ParquetWriteSupport
    //
    try {
      data.writer.write(new RowMetaAndData(getInputRowMeta(), row));
      incrementLinesOutput();
      data.splitRowCount++;
    } catch (Exception e) {
      throw new HopException("Error writing row to parquet file", e);
    }

    putRow(getInputRowMeta(), row);
    return true;
  }

  private void openNewFile() throws HopException {

    data.splitRowCount = 0;
    data.split++;

    // Hadoop configuration
    //
    data.conf = new Configuration();

    // Parquet Properties
    //
    ParquetProperties.Builder builder = ParquetProperties.builder();
    switch (meta.getVersion()) {
      case Version1:
        builder = builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0);
        break;
      case Version2:
        builder = builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);
        break;
    }
    data.props = builder.build();

    List<Type> types = new ArrayList<>();

    // Build the Parquet Schema
    //
    for (int i = 0; i < meta.getFields().size(); i++) {
      ParquetField field = meta.getFields().get(i);
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.sourceFieldIndexes.get(i));

      PrimitiveType.PrimitiveTypeName typeName = PrimitiveType.PrimitiveTypeName.BINARY;

      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_DATE:
        case IValueMeta.TYPE_INTEGER:
          typeName = PrimitiveType.PrimitiveTypeName.INT64;
          break;
        case IValueMeta.TYPE_NUMBER:
          typeName = PrimitiveType.PrimitiveTypeName.DOUBLE;
          break;
        case IValueMeta.TYPE_BOOLEAN:
          typeName = PrimitiveType.PrimitiveTypeName.BOOLEAN;
          break;
        case IValueMeta.TYPE_BINARY:
          typeName = PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
          break;
      }
      Type type = new PrimitiveType(Type.Repetition.REQUIRED, typeName, field.getTargetFieldName());
      types.add(type);
    }
    MessageType messageType = new MessageType("HopParquetSchema", types);

    // Calculate the filename...
    //
    data.filename = buildFilename(getPipeline().getExecutionStartDate());

    try {
      FileObject fileObject = HopVfs.getFileObject(data.filename);

      // See if we need to create the parent folder(s)...
      //
      if (meta.isFilenameCreatingParentFolders()) {
        FileObject parentFolder = fileObject.getParent();
        if (parentFolder != null && !parentFolder.exists()) {
          // Try to create the parent folder...
          //
          parentFolder.createFolder();
        }
      }

      data.outputStream = HopVfs.getOutputStream(data.filename, false);
      data.outputFile = new ParquetOutputFile(data.outputStream);

      data.writer =
          new ParquetWriterBuilder(
                  messageType, data.outputFile, data.sourceFieldIndexes, meta.getFields())
              .withPageSize(data.pageSize)
              .withDictionaryPageSize(data.dictionaryPageSize)
              .withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
              .withCompressionCodec(meta.getCompressionCodec())
              .withRowGroupSize(data.rowGroupSize)
              .withWriterVersion(data.props.getWriterVersion())
              .withWriteMode(ParquetFileWriter.Mode.CREATE)
              .build();

    } catch (Exception e) {
      throw new HopException("Unable to create output file '" + data.filename + "'", e);
    }
  }

  private String buildFilename(Date date) {
    String filename = resolve(meta.getFilenameBase());
    if (meta.isFilenameIncludingDate()) {
      filename += "-" + new SimpleDateFormat("yyyyMMdd").format(date);
    }
    if (meta.isFilenameIncludingTime()) {
      filename += "-" + new SimpleDateFormat("HHmmss").format(date);
    }
    if (meta.isFilenameIncludingDateTime()) {
      filename +=
          "-" + new SimpleDateFormat(resolve(meta.getFilenameDateTimeFormat())).format(date);
    }
    if (meta.isFilenameIncludingCopyNr()) {
      filename += "-" + new DecimalFormat("00").format(getCopyNr());
    }
    if (meta.isFilenameIncludingSplitNr()) {
      filename += "-" + new DecimalFormat("0000").format(data.split);
    }
    filename += "." + Const.NVL(resolve(meta.getFilenameExtension()), "parquet");
    filename += meta.getCompressionCodec().getExtension();
    return filename;
  }

  private void closeFile() throws HopException {
    try {
      data.writer.close();
    } catch (Exception e) {
      throw new HopException("Error closing file " + data.filename, e);
    }
  }
}
