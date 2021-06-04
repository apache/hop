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

package org.apache.hop.beam.transforms.bigtable;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HopToBigtableFn extends DoFn<HopRow, KV<ByteString, Iterable<Mutation>>> {

  private final int keyIndex;
  private final String counterName;
  private final String rowMetaJson;
  private final String columnsJson;
  private final List<String> transformPluginClasses;
  private final List<String> xpPluginClasses;

  private transient List<BigtableColumn> columns;
  private transient IRowMeta rowMeta;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  private transient List<Integer> sourceFieldIndexes;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger(HopToBigtableFn.class);

  public HopToBigtableFn(
      int keyIndex,
      String columnsJson,
      String counterName,
      String rowMetaJson,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses) {
    this.keyIndex = keyIndex;
    this.columnsJson = columnsJson;
    this.counterName = counterName;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      Counter initCounter = Metrics.counter(Pipeline.METRIC_NAME_INIT, counterName);
      readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, counterName);
      outputCounter = Metrics.counter(Pipeline.METRIC_NAME_OUTPUT, counterName);
      errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, counterName);

      // Initialize Hop Beam
      //
      BeamHop.init(transformPluginClasses, xpPluginClasses);
      rowMeta = JsonRowMeta.fromJson(rowMetaJson);

      // De-serialize the columns...
      //
      JSONParser parser = new JSONParser();
      JSONArray array = (JSONArray) parser.parse(columnsJson);
      columns = new ArrayList<>();
      sourceFieldIndexes = new ArrayList<>();
      for (int i = 0; i < array.size(); i++) {
        JSONObject jc = (JSONObject) array.get(i);
        String qualifier = (String) jc.get("qualifier");
        String family = (String) jc.get("family");
        String sourceField = (String) jc.get("field");
        columns.add(new BigtableColumn(qualifier, family, sourceField));
        sourceFieldIndexes.add(rowMeta.indexOfValue(sourceField));
      }

      initCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      LOG.info("Parse error on setup of Hop data to Bigtable KV : " + e.getMessage());
      throw new RuntimeException("Error on setup of converting Hop data to Bigtable KV", e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    HopRow inputRow = processContext.element();
    readCounter.inc();

    try {
      String key = rowMeta.getString(inputRow.getRow(), keyIndex);
      List<Mutation> mutations = new ArrayList<>();

      for (int i = 0; i < columns.size(); i++) {
        BigtableColumn column = columns.get(i);
        int sourceIndex = sourceFieldIndexes.get(i);
        IValueMeta valueMeta = rowMeta.getValueMeta(sourceIndex);
        Object valueData = valueMeta.getNativeDataType(inputRow.getRow()[sourceIndex]);

        if (valueData != null) {
          Mutation.SetCell setCell =
              Mutation.SetCell.newBuilder()
                  .setFamilyName(column.getFamily())
                  .setColumnQualifier(toByteString(column.getName()))
                  .setTimestampMicros(System.currentTimeMillis())
                  .setValue(toByteString(valueMeta, valueData))
                  .build();
          Mutation mutation = Mutation.newBuilder().setSetCell(setCell).build();
          mutations.add(mutation);
        }
      }

      KV<ByteString, Iterable<Mutation>> kv = KV.of(ByteString.copyFromUtf8(key), mutations);

      processContext.output(kv);
      outputCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      LOG.info("Conversion error HopRow to Bigtable KV : " + e.getMessage());
      throw new RuntimeException("Error converting HopRow to Bigtable KV", e);
    }
  }

  static ByteString toByteString(String string) {
    return ByteString.copyFrom(string.getBytes(StandardCharsets.UTF_8));
  }

  static ByteString toByteString(IValueMeta valueMeta, Object value) {
    byte[] bytes = new byte[0];
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_INTEGER:
        {
          ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
          buffer.putLong((Long) value);
          bytes = buffer.array();
        }
        break;
      case IValueMeta.TYPE_DATE:
        {
          ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
          buffer.putLong(((Date) value).getTime());
          bytes = buffer.array();
        }
        break;
      case IValueMeta.TYPE_BOOLEAN:
        {
          boolean b = (boolean) value;
          if (b) {
            bytes = new byte[] {1};
          } else {
            bytes = new byte[] {0};
          }
        }
        break;
      case IValueMeta.TYPE_NUMBER:
        {
          ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
          buffer.putDouble((double) value);
          bytes = buffer.array();
        }
        break;
        // Everything else is a String for now...
        //
      case IValueMeta.TYPE_STRING:
      default:
        // Already converted to a native value
        bytes = value.toString().getBytes(Charset.forName("UTF-8"));
        break;
    }
    return ByteString.copyFrom(bytes);
  }
}
