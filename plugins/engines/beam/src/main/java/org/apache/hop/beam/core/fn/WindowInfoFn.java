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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowInfoFn extends DoFn<HopRow, HopRow> {

  private final String transformName;
  private final String maxWindowField;
  private final String startWindowField;
  private final String endWindowField;
  private final String rowMetaJson;

  private transient Counter readCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  private transient int fieldIndex;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger(WindowInfoFn.class);

  private transient IRowMeta inputRowMeta;
  private transient IValueMeta fieldValueMeta;

  public WindowInfoFn(
      String transformName,
      String maxWindowField,
      String startWindowField,
      String endWindowField,
      String rowMetaJson) {
    this.transformName = transformName;
    this.maxWindowField = maxWindowField;
    this.startWindowField = startWindowField;
    this.endWindowField = endWindowField;
    this.rowMetaJson = rowMetaJson;
  }

  @Setup
  public void setUp() {
    try {
      readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);
      writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);
      errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName);

      // Initialize Hop Beam
      //
      BeamHop.init();
      inputRowMeta = JsonRowMeta.fromJson(rowMetaJson);

      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      LOG.error("Error in setup of adding window information to rows : " + e.getMessage());
      throw new RuntimeException("Error in setup of adding window information to rows", e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext processContext, BoundedWindow window) {

    try {

      HopRow hopRow = processContext.element();
      readCounter.inc();

      Instant instant = window.maxTimestamp();

      Object[] outputRow = RowDataUtil.createResizedCopy(hopRow.getRow(), inputRowMeta.size() + 3);

      int fieldIndex = inputRowMeta.size();

      // Hop "Date" type field output: java.util.Date.
      // Use the last field in the output
      //
      if (StringUtils.isNotEmpty(startWindowField)) {
        if (window instanceof IntervalWindow intervalWindow) {
          Instant start = intervalWindow.start();
          if (start != null) {
            outputRow[fieldIndex] = start.toDate();
          }
        }
        fieldIndex++;
      }
      if (StringUtils.isNotEmpty(endWindowField)) {
        if (window instanceof IntervalWindow intervalWindowInstance) {
          IntervalWindow intervalWindow = intervalWindowInstance;
          Instant end = intervalWindow.end();
          if (end != null) {
            outputRow[fieldIndex] = end.toDate();
          }
        }
        fieldIndex++;
      }

      if (StringUtils.isNotEmpty(maxWindowField)) {
        Instant maxTimestamp = window.maxTimestamp();
        if (maxTimestamp != null) {
          outputRow[fieldIndex] = maxTimestamp.toDate();
        }
        fieldIndex++;
      }

      // Pass the new row to the process context
      //
      HopRow outputHopRow = new HopRow(outputRow);
      processContext.outputWithTimestamp(outputHopRow, instant);
      writtenCounter.inc();

    } catch (Exception e) {
      errorCounter.inc();
      LOG.error(
          "Error adding window information to rows : "
              + processContext.element()
              + ", "
              + e.getMessage());
      throw new RuntimeException("Error adding window information to rows", e);
    }
  }
}
