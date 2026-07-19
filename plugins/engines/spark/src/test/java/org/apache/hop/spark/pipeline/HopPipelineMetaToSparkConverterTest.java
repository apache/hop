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

package org.apache.hop.spark.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.EngineCompatibility;
import org.apache.hop.core.plugins.EngineCompatibilityResolver;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.spark.engines.SparkPipelineEngine;
import org.apache.hop.spark.util.SparkConst;
import org.junit.jupiter.api.Test;

class HopPipelineMetaToSparkConverterTest {

  @Test
  void sortedGroupByStillBanned() {
    HopException group =
        assertThrows(
            HopException.class,
            () ->
                HopPipelineMetaToSparkConverter.validateTransformSparkUsage(
                    SparkConst.GROUP_BY_PLUGIN_ID, "g1"));
    assertTrue(group.getMessage().contains("Group By"));
  }

  @Test
  void nativeHandlersAreNotBanned() throws HopException {
    HopPipelineMetaToSparkConverter.validateTransformSparkUsage(
        SparkConst.MEMORY_GROUP_BY_PLUGIN_ID, "mg");
    HopPipelineMetaToSparkConverter.validateTransformSparkUsage(
        SparkConst.MERGE_JOIN_PLUGIN_ID, "mj");
    HopPipelineMetaToSparkConverter.validateTransformSparkUsage(
        SparkConst.UNIQUE_ROWS_PLUGIN_ID, "u");
    HopPipelineMetaToSparkConverter.validateTransformSparkUsage(
        SparkConst.SORT_ROWS_PLUGIN_ID, "s");
    HopPipelineMetaToSparkConverter.validateTransformSparkUsage(
        SparkConst.SPARK_FILE_INPUT_PLUGIN_ID, "in");
    HopPipelineMetaToSparkConverter.validateTransformSparkUsage(
        SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID, "out");
    HopPipelineMetaToSparkConverter.validateTransformSparkUsage("Calculator", "calc");
  }

  @Test
  void supportsSurfacesNativeAndBans() {
    SparkPipelineEngine engine = new SparkPipelineEngine();

    IPlugin banned = mock(IPlugin.class);
    when(banned.getIds()).thenReturn(new String[] {SparkConst.GROUP_BY_PLUGIN_ID});
    assertTrue(engine.supports(banned).isUnsupported());

    IPlugin memoryGroupBy = mock(IPlugin.class);
    when(memoryGroupBy.getIds()).thenReturn(new String[] {SparkConst.MEMORY_GROUP_BY_PLUGIN_ID});
    assertTrue(engine.supports(memoryGroupBy).isSupported());

    IPlugin sort = mock(IPlugin.class);
    when(sort.getIds()).thenReturn(new String[] {SparkConst.SORT_ROWS_PLUGIN_ID});
    assertTrue(engine.supports(sort).isSupported());

    assertEquals(EngineCompatibility.Verdict.UNKNOWN, engine.supports(null).getVerdict());
  }

  @Test
  void collectActiveTransformsSkipsDisabledHopsAndDisconnected() {
    PipelineMeta pm = new PipelineMeta();
    TransformMeta a = new TransformMeta("A", new DummyMeta());
    a.setTransformPluginId("Dummy");
    TransformMeta b = new TransformMeta("B", new DummyMeta());
    b.setTransformPluginId("Dummy");
    TransformMeta orphan = new TransformMeta("Orphan Sink", new DummyMeta());
    orphan.setTransformPluginId("Dummy");
    pm.addTransform(a);
    pm.addTransform(b);
    pm.addTransform(orphan);
    PipelineHopMeta hop = new PipelineHopMeta(a, b);
    hop.setEnabled(false);
    pm.addPipelineHop(hop);

    List<TransformMeta> active = HopPipelineMetaToSparkConverter.collectActiveTransforms(pm);
    assertTrue(active.isEmpty(), "disabled hop and disconnected transforms must not run");

    hop.setEnabled(true);
    pm.clearCaches();
    active = HopPipelineMetaToSparkConverter.collectActiveTransforms(pm);
    assertEquals(2, active.size());
    assertTrue(active.stream().anyMatch(t -> "A".equals(t.getName())));
    assertTrue(active.stream().anyMatch(t -> "B".equals(t.getName())));
    assertFalse(active.stream().anyMatch(t -> "Orphan Sink".equals(t.getName())));
  }

  @Test
  void explicitHandlerSetMatchesRegistrations() {
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.MEMORY_GROUP_BY_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.MERGE_JOIN_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.UNIQUE_ROWS_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.SORT_ROWS_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.SPARK_FILE_INPUT_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.SPARK_FILE_OUTPUT_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.SPARK_LAKE_TABLE_MERGE_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToSparkConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            SparkConst.SPARK_LAKE_TABLE_MAINTENANCE_PLUGIN_ID));
  }

  /**
   * Every plugin id in {@link HopPipelineMetaToSparkConverter#HARD_BANNED_PLUGIN_IDS} must declare
   * Native Spark on {@code @Transform.excludedEngines} (value of {@link SparkConst#PLUGIN_ID}).
   * Keep {@code bannedMetas} in lockstep when the ban list grows.
   */
  @Test
  void hardBannedTransformsExcludeNativeSparkOnAnnotation() {
    Map<String, Class<?>> bannedMetas = Map.of(SparkConst.GROUP_BY_PLUGIN_ID, GroupByMeta.class);

    assertEquals(
        HopPipelineMetaToSparkConverter.HARD_BANNED_PLUGIN_IDS.keySet(),
        bannedMetas.keySet(),
        "HARD_BANNED_PLUGIN_IDS and bannedMetas must stay in lockstep");

    for (Map.Entry<String, Class<?>> e : bannedMetas.entrySet()) {
      Transform ann = e.getValue().getAnnotation(Transform.class);
      assertNotNull(ann, e.getKey() + " must have @Transform");
      assertTrue(
          EngineCompatibilityResolver.matchesAny(ann.excludedEngines(), SparkConst.PLUGIN_ID),
          () ->
              e.getKey()
                  + " @Transform.excludedEngines must include "
                  + SparkConst.PLUGIN_ID
                  + " but was "
                  + Arrays.toString(ann.excludedEngines()));
    }
  }
}
