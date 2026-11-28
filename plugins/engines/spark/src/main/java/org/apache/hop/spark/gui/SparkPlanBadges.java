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

package org.apache.hop.spark.gui;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.HopPipelineMetaToSparkConverter;
import org.apache.hop.spark.pipeline.SparkTransformPlan;
import org.apache.hop.spark.util.SparkConst;
import org.apache.hop.ui.hopgui.PaletteEngineFilter;

/**
 * The plan the canvas badges show: how every transform of a pipeline runs on the native Spark
 * engine, with the engine's default run configuration. Worked out again only when a setting, a
 * transform or a hop changes, not when a transform is moved.
 */
final class SparkPlanBadges {

  /** What a badge area holds, for its tooltip. */
  record Badge(String transformName, SparkTransformPlan plan) {}

  private record Entry(int fingerprint, Map<String, SparkTransformPlan> plan) {}

  private static final Map<PipelineMeta, Entry> PLANS =
      Collections.synchronizedMap(new WeakHashMap<>());

  private SparkPlanBadges() {}

  /** Whether the canvas designs for the native Spark engine ("Design for" on the toolbar). */
  static boolean designingForSpark() {
    return SparkConst.PLUGIN_ID.equals(PaletteEngineFilter.getPipelineDesignEngineId());
  }

  /** Works the plan out again when the pipeline changed since the last time. */
  static void refresh(PipelineMeta pipelineMeta, IVariables variables) {
    int fingerprint = fingerprint(pipelineMeta);
    Entry entry = PLANS.get(pipelineMeta);
    if (entry != null && entry.fingerprint() == fingerprint) {
      return;
    }
    Map<String, SparkTransformPlan> plan;
    try {
      plan =
          HopPipelineMetaToSparkConverter.plan(
              variables, pipelineMeta, new SparkPipelineRunConfiguration());
    } catch (Exception | LinkageError e) {
      LogChannel.GENERAL.logDebug("No native Spark plan for the canvas: " + e.getMessage());
      plan = Map.of();
    }
    PLANS.put(pipelineMeta, new Entry(fingerprint, plan));
  }

  static SparkTransformPlan plan(PipelineMeta pipelineMeta, String transformName) {
    Entry entry = PLANS.get(pipelineMeta);
    return entry == null ? null : entry.plan().get(transformName);
  }

  /** The transforms with their settings and the hops; not where they are on the canvas. */
  static int fingerprint(PipelineMeta pipelineMeta) {
    StringBuilder text = new StringBuilder();
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      text.append(transformMeta.getName())
          .append('|')
          .append(transformMeta.getTransformPluginId())
          .append('|')
          .append(transformMeta.getCopiesString())
          .append('|')
          .append(transformMeta.isDoingErrorHandling())
          .append('|')
          .append(transformMeta.isPartitioned());
      try {
        text.append('|').append(transformMeta.getTransform().getXml());
      } catch (HopException | RuntimeException e) {
        text.append("|?");
      }
      text.append('\n');
    }
    for (PipelineHopMeta hop : pipelineMeta.getPipelineHops()) {
      text.append(hop.getFromTransform() == null ? "" : hop.getFromTransform().getName())
          .append("->")
          .append(hop.getToTransform() == null ? "" : hop.getToTransform().getName())
          .append(hop.isEnabled())
          .append('\n');
    }
    return text.toString().hashCode();
  }
}
