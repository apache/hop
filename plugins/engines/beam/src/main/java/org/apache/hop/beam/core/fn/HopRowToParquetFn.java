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

import java.util.Date;
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
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;

/** Maps one Hop row onto the flat Avro record ParquetIO writes. */
@RequiredArgsConstructor
public class HopRowToParquetFn extends DoFn<HopRow, GenericRecord> {
  private final String transformName;
  private final String rowMetaJson;
  private final String schemaJson;
  private final List<String> sourceFields;

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
      indexes = new int[sourceFields.size()];
      for (int i = 0; i < sourceFields.size(); i++) {
        indexes[i] = rowMeta.indexOfValue(sourceFields.get(i));
        if (indexes[i] < 0) {
          throw new HopRuntimeException("Field [" + sourceFields.get(i) + "] was not found");
        }
      }
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to initialize Parquet output " + transformName, e);
    }
  }

  @ProcessElement
  public void process(@Element HopRow row, OutputReceiver<GenericRecord> output) {
    readCounter.inc();
    try {
      GenericRecord record = new GenericData.Record(schema);
      Object[] values = row.getRow();
      for (int i = 0; i < schema.getFields().size(); i++) {
        Schema.Field field = schema.getFields().get(i);
        record.put(field.name(), value(values, indexes[i], field.schema()));
      }
      output.output(record);
      outputCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException(
          "Unable to convert a row for Parquet output " + transformName, e);
    }
  }

  private Object value(Object[] values, int index, Schema fieldSchema) throws Exception {
    if (rowMeta.isNull(values, index)) {
      return null;
    }
    Schema type = fieldSchema;
    if (type.getType() == Schema.Type.UNION) {
      for (Schema candidate : type.getTypes()) {
        if (candidate.getType() != Schema.Type.NULL) {
          type = candidate;
          break;
        }
      }
    }
    return switch (type.getType()) {
      case LONG -> longValue(values, index);
      case DOUBLE -> rowMeta.getNumber(values, index);
      case BOOLEAN -> rowMeta.getBoolean(values, index);
      case STRING -> rowMeta.getString(values, index);
      case BYTES -> rowMeta.getBinary(values, index);
      default ->
          throw new HopRuntimeException("Parquet output cannot write Avro type " + type.getType());
    };
  }

  private Object longValue(Object[] values, int index) throws Exception {
    if (rowMeta.getValueMeta(index).getType() == IValueMeta.TYPE_DATE
        || rowMeta.getValueMeta(index).getType() == IValueMeta.TYPE_TIMESTAMP) {
      Date date = rowMeta.getDate(values, index);
      return date == null ? null : date.getTime();
    }
    return rowMeta.getInteger(values, index);
  }
}
