/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.transforms.bigtable;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.protobuf.Descriptors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.Pipeline;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableRowToHopRowFn extends DoFn<Row, HopRow> {
  private final String counterName;
  private final String rowMetaJson;
  private final String keyField;
  private final String columnsJson;

  private transient List<BigtableSourceColumn> sourceColumns;
  private transient IRowMeta rowMeta;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;
  private transient Map<String, Integer> qualifierTargetIndexes;
  private transient Map<String, BigtableSourceColumn> qualifierSourceColumns;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger(HopToBigtableFn.class);

  public BigtableRowToHopRowFn(
      String counterName, String rowMetaJson, String keyField, String columnsJson) {
    this.counterName = counterName;
    this.rowMetaJson = rowMetaJson;
    this.keyField = keyField;
    this.columnsJson = columnsJson;
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
      BeamHop.init();
      rowMeta = JsonRowMeta.fromJson(rowMetaJson);

      int indexIncrement = 0;
      if (StringUtils.isNotEmpty(keyField)) {
        indexIncrement++;
      }

      // De-serialize the columns...
      //
      JSONParser parser = new JSONParser();
      JSONArray array = (JSONArray) parser.parse(columnsJson);
      sourceColumns = new ArrayList<>();
      qualifierTargetIndexes = new HashMap<>();
      qualifierSourceColumns = new HashMap<>();
      for (int i = 0; i < array.size(); i++) {
        JSONObject jc = (JSONObject) array.get(i);
        String qualifier = (String) jc.get("qualifier");
        String targetType = (String) jc.get("target_type");
        String targetField = (String) jc.get("target_field_name");
        BigtableSourceColumn sourceColumn =
            new BigtableSourceColumn(qualifier, targetType, targetField);
        sourceColumns.add(sourceColumn);
        qualifierTargetIndexes.put(qualifier, i + indexIncrement);
        qualifierSourceColumns.put(qualifier, sourceColumn);
      }

      initCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      LOG.info("Parse error on setup of Bigtable Row to HopRow function : " + e.getMessage());
      throw new RuntimeException("Error on setup of Bigtable Row to HopRow function function", e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    Row inputRow = processContext.element();
    Map<Descriptors.FieldDescriptor, Object> allFields = inputRow.getAllFields();

    readCounter.inc();

    try {
      // All Strings coming from Bigtable
      //
      int rowSize = rowMeta.size();
      Object[] row = new Object[rowSize];

      if (StringUtils.isNotEmpty(keyField)) {
        row[0] = inputRow.getKey().toStringUtf8();
      }

      for (Family family : inputRow.getFamiliesList()) {
        // String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          String qualifier = column.getQualifier().toStringUtf8();

          Integer targetIndex = qualifierTargetIndexes.get(qualifier);
          BigtableSourceColumn sourceColumn = qualifierSourceColumns.get(qualifier);

          if (targetIndex != null) {
            StringBuffer values = new StringBuffer();

            for (Cell cell : column.getCellsList()) {
              // long timestamp = cell.getTimestampMicros();
              String value = cell.getValue().toStringUtf8();
              if (values.length() > 0) {
                values.append(',');
              }
              values.append(value);
            }

            IValueMeta targetValueMeta = sourceColumn.getValueMeta();

            IValueMeta hopValueMeta =
                ValueMetaFactory.createValueMeta("source", IValueMeta.TYPE_STRING);
            row[targetIndex] = targetValueMeta.convertData(hopValueMeta, values.toString());
          }
        }
      }

      HopRow hopRow = new HopRow(row);
      processContext.output(hopRow);
      outputCounter.inc();
    } catch (Exception e) {
      errorCounter.inc();
      LOG.info("Conversion error Bigtable Row to HopRow: " + e.getMessage());
      throw new RuntimeException("Error converting Bigtable Row to HopRow: ", e);
    }
  }
}
