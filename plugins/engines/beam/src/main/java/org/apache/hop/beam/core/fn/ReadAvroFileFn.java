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

import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;

/**
 * Reads the Avro data file named by one incoming field and appends each record to that row.
 *
 * <p>{@code AvroIO.readFiles} returns records without the path, so it cannot put the record back on
 * the row that named the file. This reads the file with the same {@code DataFileStream} AvroIO
 * uses, on the worker that holds the row. A positive row limit is the most records this worker
 * emits, across every file it sees.
 */
@RequiredArgsConstructor
public class ReadAvroFileFn extends DoFn<HopRow, HopRow> {
  private final String transformName;
  private final String rowMetaJson;
  private final String filenameField;
  private final int rowsLimit;

  private transient IRowMeta rowMeta;
  private transient int filenameIndex;
  private transient int inputSize;
  private transient int emitted;
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
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to initialize Avro file input " + transformName, e);
    }
  }

  @ProcessElement
  public void process(@Element HopRow row, OutputReceiver<HopRow> output) {
    if (rowsLimit > 0 && emitted >= rowsLimit) {
      return;
    }
    try {
      String filename = rowMeta.getString(row.getRow(), filenameIndex);
      if (filename == null || filename.isEmpty()) {
        return;
      }
      MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(filename);
      try (InputStream in = Channels.newInputStream(FileSystems.open(metadata.resourceId()));
          DataFileStream<GenericRecord> stream =
              new DataFileStream<>(in, new GenericDatumReader<>())) {
        while (stream.hasNext() && (rowsLimit <= 0 || emitted < rowsLimit)) {
          GenericRecord record = stream.next();
          Object[] source = row.getRow();
          Object[] copy = Arrays.copyOf(source, inputSize + 1);
          copy[inputSize] = record;
          output.output(new HopRow(copy));
          emitted++;
          readCounter.inc();
          outputCounter.inc();
        }
      }
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException(
          "Unable to read an Avro file for transform " + transformName, e);
    }
  }
}
