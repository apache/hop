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

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;

/** Maps one Hop row onto a flat Avro record. Nested paths are rejected before this runs. */
@RequiredArgsConstructor
public class HopRowToAvroFn extends DoFn<HopRow, GenericRecord> {
  static final int TYPE_BOOLEAN = 1;
  static final int TYPE_DOUBLE = 2;
  static final int TYPE_FLOAT = 3;
  static final int TYPE_INT = 4;
  static final int TYPE_LONG = 5;
  static final int TYPE_STRING = 6;

  private final String transformName;
  private final String rowMetaJson;
  private final String schemaJson;
  private final List<AvroFieldMapping> fields;

  private transient Schema schema;
  private transient IRowMeta rowMeta;
  private transient int[] indexes;
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
      schema = new Schema.Parser().parse(schemaJson);
      rowMeta = JsonRowMeta.fromJson(rowMetaJson);
      indexes = new int[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        indexes[i] = rowMeta.indexOfValue(fields.get(i).getHopName());
        if (indexes[i] < 0) {
          throw new HopRuntimeException(
              "Field [" + fields.get(i).getHopName() + "] couldn't be found in the input stream");
        }
      }
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to initialize Avro output " + transformName, e);
    }
  }

  @ProcessElement
  public void process(@Element HopRow row, OutputReceiver<GenericRecord> output) {
    readCounter.inc();
    try {
      GenericRecord record = new GenericData.Record(schema);
      Object[] values = row.getRow();
      for (int i = 0; i < fields.size(); i++) {
        AvroFieldMapping field = fields.get(i);
        record.put(field.getAvroName(), value(values, indexes[i], field.getAvroType()));
      }
      output.output(record);
      outputCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to convert a row for Avro output " + transformName, e);
    }
  }

  private Object value(Object[] values, int index, int avroType) throws Exception {
    if (rowMeta.isNull(values, index)) {
      return null;
    }
    return switch (avroType) {
      case TYPE_INT -> rowMeta.getInteger(values, index).intValue();
      case TYPE_LONG -> rowMeta.getInteger(values, index);
      case TYPE_FLOAT -> rowMeta.getNumber(values, index).floatValue();
      case TYPE_DOUBLE -> rowMeta.getNumber(values, index);
      case TYPE_BOOLEAN -> rowMeta.getBoolean(values, index);
      case TYPE_STRING -> rowMeta.getString(values, index);
      default -> throw new HopRuntimeException("Unsupported Avro type " + avroType);
    };
  }
}
