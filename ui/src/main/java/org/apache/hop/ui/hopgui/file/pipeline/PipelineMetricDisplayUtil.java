/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.hop.ui.hopgui.file.pipeline;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.IEngineMetric;

/**
 * Utility for displaying pipeline metrics in the UI with appropriate units. Does not change metric
 * storage or keys; only used for column headers and labels.
 */
public final class PipelineMetricDisplayUtil {

  private PipelineMetricDisplayUtil() {}

  /**
   * Returns the metric header for display in the metrics tab/grid. The underlying metric key (e.g.
   * for lookup) remains the raw header from {@link IEngineMetric#getHeader()}.
   *
   * @param metric the engine metric
   * @param withUnit when true, append unit in parentheses (e.g. "Input (rows)"); when false, header
   *     only (e.g. "Input")
   * @return display label
   */
  public static String getDisplayHeader(IEngineMetric metric, boolean withUnit) {
    if (metric == null) {
      return "";
    }
    if (!withUnit) {
      return metric.getHeader();
    }
    String unit = getUnitForMetric(metric);
    if (unit == null || unit.isEmpty()) {
      return metric.getHeader();
    }
    return metric.getHeader() + " (" + unit + ")";
  }

  /**
   * Returns the metric header with the correct unit for display (same as getDisplayHeader(metric,
   * true)).
   */
  public static String getDisplayHeaderWithUnit(IEngineMetric metric) {
    return getDisplayHeader(metric, true);
  }

  /**
   * Returns the unit label for a metric (e.g. "rows", "runs") for use in column headers. Returns
   * null if the metric has no unit.
   */
  public static String getUnitForMetric(IEngineMetric metric) {
    String code = metric.getCode();
    if (code == null) {
      return null;
    }
    switch (code) {
      case Pipeline.METRIC_NAME_INPUT,
          Pipeline.METRIC_NAME_READ,
          Pipeline.METRIC_NAME_WRITTEN,
          Pipeline.METRIC_NAME_OUTPUT,
          Pipeline.METRIC_NAME_UPDATED,
          Pipeline.METRIC_NAME_REJECTED,
          Pipeline.METRIC_NAME_ERROR,
          Pipeline.METRIC_NAME_BUFFER_IN,
          Pipeline.METRIC_NAME_BUFFER_OUT:
        return "rows";
      case Pipeline.METRIC_NAME_INIT:
        return "runs";
      case Pipeline.METRIC_NAME_FLUSH_BUFFER:
        return "flushes";
      default:
        return null;
    }
  }

  /**
   * Returns the short unit label for a metric for use in grid cell values (e.g. "r" for rows so the
   * header can keep "rows"). Returns null if the metric has no unit.
   */
  public static String getUnitForMetricCell(IEngineMetric metric) {
    String unit = getUnitForMetric(metric);
    if (unit == null) {
      return null;
    }
    if ("rows".equals(unit)) {
      return "r";
    }
    return unit;
  }
}
