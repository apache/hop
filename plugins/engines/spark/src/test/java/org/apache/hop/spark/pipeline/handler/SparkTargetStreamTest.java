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

package org.apache.hop.spark.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.filterrows.FilterRowsMeta;
import org.apache.hop.spark.core.HopSparkUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit tests for target-stream discovery and Dataset map key helpers. */
class SparkTargetStreamTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void createTargetTupleIdMatchesBeamFormat() {
    assertEquals(
        "Filter - TARGET - True branch", HopSparkUtil.createTargetTupleId("Filter", "True branch"));
  }

  @Test
  void resolveTargetTransformNamesFromFilterRows() {
    PipelineMeta pm = new PipelineMeta();
    TransformMeta src = new TransformMeta("src", new DummyMeta());
    src.setTransformPluginId("Dummy");
    TransformMeta trueT = new TransformMeta("True branch", new DummyMeta());
    trueT.setTransformPluginId("Dummy");
    TransformMeta falseT = new TransformMeta("False branch", new DummyMeta());
    falseT.setTransformPluginId("Dummy");

    FilterRowsMeta filterMeta = new FilterRowsMeta();
    filterMeta.setTrueTransformName("True branch");
    filterMeta.setFalseTransformName("False branch");
    TransformMeta filter = new TransformMeta("Filter", filterMeta);
    filter.setTransformPluginId("FilterRows");

    pm.addTransform(src);
    pm.addTransform(trueT);
    pm.addTransform(falseT);
    pm.addTransform(filter);
    pm.addPipelineHop(new PipelineHopMeta(src, filter));
    pm.addPipelineHop(new PipelineHopMeta(filter, trueT));
    pm.addPipelineHop(new PipelineHopMeta(filter, falseT));
    filterMeta.searchInfoAndTargetTransforms(pm.getTransforms());

    List<String> targets = SparkGenericTransformHandler.resolveTargetTransformNames(filter);
    assertEquals(2, targets.size());
    assertTrue(targets.contains("True branch"));
    assertTrue(targets.contains("False branch"));

    List<TransformMeta> next = pm.findNextTransforms(filter);
    assertEquals(2, next.size());
  }

  @Test
  void resolveTargetNamesEmptyWhenNotConfigured() {
    FilterRowsMeta filterMeta = new FilterRowsMeta();
    TransformMeta filter = new TransformMeta("Filter", filterMeta);
    filter.setTransformPluginId("FilterRows");
    List<String> targets = SparkGenericTransformHandler.resolveTargetTransformNames(filter);
    assertTrue(targets.isEmpty());
  }

  @Test
  void sameFieldLayoutDetectsMismatch() {
    org.apache.hop.core.row.RowMeta a = new org.apache.hop.core.row.RowMeta();
    a.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("country_name"));
    org.apache.hop.core.row.RowMeta b = new org.apache.hop.core.row.RowMeta();
    b.addValueMeta(new org.apache.hop.core.row.value.ValueMetaInteger("ExecutionTime"));
    b.addValueMeta(new org.apache.hop.core.row.value.ValueMetaBoolean("ExecutionResult"));
    assertTrue(SparkGenericTransformHandler.sameFieldLayout(a, a));
    assertTrue(!SparkGenericTransformHandler.sameFieldLayout(a, b));
  }
}
