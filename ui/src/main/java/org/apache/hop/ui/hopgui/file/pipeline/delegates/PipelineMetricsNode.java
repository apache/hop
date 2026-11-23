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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * One row in the pipeline metrics tree. A leaf is a single running copy. An aggregate is the
 * collapsed parent of several copies; its children are those copies.
 */
@Getter
final class PipelineMetricsNode {
  private final String transformName;
  private final int copyNr;
  private final int copyCount;
  private final boolean aggregate;
  private final Map<String, Long> metricSums;
  private final Long durationMs;
  private final Long speedValue;
  private final String speedText;
  private final String status;
  private final List<PipelineMetricsNode> children;

  PipelineMetricsNode(
      String transformName,
      int copyNr,
      int copyCount,
      boolean aggregate,
      Map<String, Long> metricSums,
      Long durationMs,
      Long speedValue,
      String speedText,
      String status,
      List<PipelineMetricsNode> children) {
    this.transformName = transformName == null ? "" : transformName;
    this.copyNr = copyNr;
    this.copyCount = copyCount;
    this.aggregate = aggregate;
    this.metricSums =
        metricSums == null || metricSums.isEmpty() ? Map.of() : Map.copyOf(metricSums);
    this.durationMs = durationMs;
    this.speedValue = speedValue;
    this.speedText = speedText == null ? "" : speedText;
    this.status = status == null ? "" : status;
    this.children = children == null || children.isEmpty() ? List.of() : List.copyOf(children);
  }
}
