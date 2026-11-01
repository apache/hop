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

package org.apache.hop.beam.core.fn;

import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;

/**
 * Reads the Parquet file named by one incoming field and appends the configured columns.
 *
 * <p>{@code ParquetIO.readFiles} returns records without the path, so the source row would be lost.
 * This reads the file on the worker that holds the row, with the same Avro projection {@code
 * ParquetIO.read(schema)} would use.
 */
@RequiredArgsConstructor
public class ReadParquetFileFn extends DoFn<HopRow, HopRow> {
  private final String transformName;
  private final String rowMetaJson;
  private final String filenameField;
  private final String schemaJson;
  private final List<ParquetColumnMapping> columns;

  private transient IRowMeta rowMeta;
  private transient int filenameIndex;
  private transient int inputSize;
  private transient Schema schema;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  @Setup
  public void setup() {
    readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);
    outputCounter = Metrics.counter(Pipeline.METRIC_NAME_OUTPUT, transformName);
    errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName);
    try {
      BeamHop.init();
      rowMeta = JsonRowMeta.fromJson(rowMetaJson);
      filenameIndex = rowMeta.indexOfValue(filenameField);
      if (filenameIndex < 0) {
        throw new HopRuntimeException("Filename field '" + filenameField + "' was not found");
      }
      inputSize = rowMeta.size();
      schema = new Schema.Parser().parse(schemaJson);
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to initialize Parquet input " + transformName, e);
    }
  }

  @ProcessElement
  public void process(@Element HopRow row, OutputReceiver<HopRow> output) {
    try {
      String filename = rowMeta.getString(row.getRow(), filenameIndex);
      if (filename == null || filename.isEmpty()) {
        return;
      }
      Configuration configuration = new Configuration(false);
      AvroReadSupport.setRequestedProjection(configuration, schema);
      try (ParquetReader<GenericRecord> reader =
          AvroParquetReader.<GenericRecord>builder(
                  new LocalInputFile(java.nio.file.Path.of(filename)))
              .withConf(configuration)
              .build()) {
        GenericRecord record;
        while ((record = reader.read()) != null) {
          Object[] copy = Arrays.copyOf(row.getRow(), inputSize + columns.size());
          for (int i = 0; i < columns.size(); i++) {
            copy[inputSize + i] =
                hopValue(record.get(columns.get(i).getColumnName()), columns.get(i));
          }
          output.output(new HopRow(copy));
          readCounter.inc();
          outputCounter.inc();
        }
      }
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException(
          "Unable to read a Parquet file for transform " + transformName, e);
    }
  }

  private static Object hopValue(Object value, ParquetColumnMapping column) {
    if (value == null) {
      return null;
    }
    if (column.getHopType() == IValueMeta.TYPE_STRING) {
      return value.toString();
    }
    if (column.getHopType() == IValueMeta.TYPE_INTEGER && value instanceof Number number) {
      return number.longValue();
    }
    if (column.getHopType() == IValueMeta.TYPE_NUMBER && value instanceof Number number) {
      return number.doubleValue();
    }
    return value;
  }
}
