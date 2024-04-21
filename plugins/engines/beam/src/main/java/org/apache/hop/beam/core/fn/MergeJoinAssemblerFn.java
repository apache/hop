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

package org.apache.hop.beam.core.fn;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeJoinAssemblerFn extends DoFn<KV<HopRow, KV<HopRow, HopRow>>, HopRow> {

  public static int JOIN_TYPE_INNER = 0;
  public static int JOIN_TYPE_LEFT_OUTER = 1;
  public static int JOIN_TYPE_RIGHT_OUTER = 2;
  public static int JOIN_TYPE_FULL_OUTER = 3;

  private int joinType;
  private String leftRowMetaJson;
  private String rightRowMetaJson;
  private String leftKRowMetaJson;
  private String leftVRowMetaJson;
  private String rightKRowMetaJson;
  private String rightVRowMetaJson;
  private String counterName;

  private static final Logger LOG = LoggerFactory.getLogger(MergeJoinAssemblerFn.class);

  private transient IRowMeta leftRowMeta;
  private transient IRowMeta rightRowMeta;
  private transient IRowMeta leftKRowMeta;
  private transient IRowMeta leftVRowMeta;
  private transient IRowMeta rightKRowMeta;
  private transient IRowMeta rightVRowMeta;

  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  private transient Map<Integer, Integer> leftKeyIndexes;
  private transient Map<Integer, Integer> leftValueIndexes;
  private transient Map<Integer, Integer> rightKeyIndexes;
  private transient Map<Integer, Integer> rightValueIndexes;

  public MergeJoinAssemblerFn() {}

  public MergeJoinAssemblerFn(
      int joinType,
      String leftRowMetaJson,
      String rightRowMetaJson,
      String leftKRowMetaJson,
      String leftVRowMetaJson,
      String rightKRowMetaJson,
      String rightVRowMetaJson,
      String counterName) {
    this.joinType = joinType;
    this.leftRowMetaJson = leftRowMetaJson;
    this.rightRowMetaJson = rightRowMetaJson;
    this.leftKRowMetaJson = leftKRowMetaJson;
    this.leftVRowMetaJson = leftVRowMetaJson;
    this.rightKRowMetaJson = rightKRowMetaJson;
    this.rightVRowMetaJson = rightVRowMetaJson;
    this.counterName = counterName;
  }

  @Setup
  public void setUp() {
    try {
      writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, counterName);
      errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, counterName);

      // Initialize Hop Beam
      //
      BeamHop.init();
      leftRowMeta = JsonRowMeta.fromJson(leftRowMetaJson);
      rightRowMeta = JsonRowMeta.fromJson(rightRowMetaJson);
      leftKRowMeta = JsonRowMeta.fromJson(leftKRowMetaJson);
      leftVRowMeta = JsonRowMeta.fromJson(leftVRowMetaJson);
      rightKRowMeta = JsonRowMeta.fromJson(rightKRowMetaJson);
      rightVRowMeta = JsonRowMeta.fromJson(rightVRowMetaJson);

      leftKeyIndexes = new HashMap<>();
      leftValueIndexes = new HashMap<>();
      rightKeyIndexes = new HashMap<>();
      rightValueIndexes = new HashMap<>();

      // Cache source-to-target mappings for values
      //
      for (int i = 0; i < leftRowMeta.size(); i++) {
        IValueMeta valueMeta = leftRowMeta.getValueMeta(i);
        int index = leftKRowMeta.indexOfValue(valueMeta.getName());
        if (index >= 0) {
          leftKeyIndexes.put(i, index);
        }
        index = leftVRowMeta.indexOfValue(valueMeta.getName());
        if (index >= 0) {
          leftValueIndexes.put(i, index);
        }
      }
      for (int i = 0; i < rightRowMeta.size(); i++) {
        IValueMeta valueMeta = rightRowMeta.getValueMeta(i);
        int index = rightKRowMeta.indexOfValue(valueMeta.getName());
        if (index >= 0) {
          rightKeyIndexes.put(i, index);
        }
        index = rightVRowMeta.indexOfValue(valueMeta.getName());
        if (index >= 0) {
          rightValueIndexes.put(i, index);
        }
      }

      Metrics.counter(Pipeline.METRIC_NAME_INIT, counterName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      LOG.error("Error initializing assembling rows", e);
      throw new RuntimeException("Error initializing assembling output KV<row, KV<row, row>>", e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {

    try {
      KV<HopRow, KV<HopRow, HopRow>> element = processContext.element();
      KV<HopRow, HopRow> value = element.getValue();

      HopRow leftValue = value.getKey();
      HopRow rightValue = value.getValue();

      Object[] outputRow = new Object[leftRowMeta.size() + rightRowMeta.size()];

      for (int i = 0; i < leftRowMeta.size(); i++) {
        // Only add key for inner and left-outer join types
        // Otherwise, leave this field blank to match up with default Hop behavior of the transform.
        // If the left-hand side row is empty, we don't want to add keys or values
        //
        if (leftValue.isNotEmpty()) {
          Integer keyIndex = leftKeyIndexes.get(i);
          if (keyIndex != null) {
            outputRow[i] = leftValue.getRow()[keyIndex];
          }
          Integer valueIndex = leftValueIndexes.get(i);
          if (valueIndex != null) {
            outputRow[i] = leftValue.getRow()[valueIndex];
          }
        }
      }

      for (int i = 0; i < rightRowMeta.size(); i++) {
        // If the right-hand side row is empty, we don't want to add keys or values
        //
        if (rightValue.isNotEmpty()) {
          Integer keyIndex = rightKeyIndexes.get(i);
          if (keyIndex != null) {
            outputRow[leftRowMeta.size() + i] = rightValue.getRow()[keyIndex];
          }
          Integer valueIndex = rightValueIndexes.get(i);
          if (valueIndex != null) {
            outputRow[leftRowMeta.size() + i] = rightValue.getRow()[valueIndex];
          }
        }
      }

      processContext.output(new HopRow(outputRow));
      writtenCounter.inc();

    } catch (Exception e) {
      errorCounter.inc();
      LOG.error("Error assembling rows", e);
      throw new RuntimeException("Error assembling output KV<row, KV<row, row>>", e);
    }
  }
}
