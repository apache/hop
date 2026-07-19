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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Partition-scoped Internal.Transform.* and beamContext for classic I/O on native Spark. */
class SparkParallelFileContextTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void partitionInternalVariablesUseNameAndPartitionId() {
    Variables variables = new Variables();
    HopMapPartitionsFn.applyPartitionInternalVariables(variables, "Text File Output", 3);

    assertEquals("Text File Output", variables.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_NAME));
    assertEquals("3", variables.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR));
    assertEquals("Text File Output-3", variables.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_ID));
    assertEquals("3", variables.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_BUNDLE_NR));
  }

  @Test
  void sparkParallelFileContextSetsBeamContextAndOverridesId() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("writer");
    pipelineMeta.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    TransformMeta dummyTm = new TransformMeta("writer", new DummyMeta());
    dummyTm.setTransformPluginId("Dummy");
    pipelineMeta.addTransform(dummyTm);

    Variables variables = new Variables();
    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(
            pipelineMeta, variables, new LoggingObject("spark-file-context-test"));
    pipeline.prepareExecution();

    // Simulate init having set UUID-based Internal.Transform.ID
    pipeline
        .getTransforms()
        .get(0)
        .transform
        .setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_ID, "uuid-xyz");

    HopMapPartitionsFn.applySparkParallelFileContext(pipeline, "writer", 2);

    var combi = pipeline.getTransforms().get(0);
    assertTrue(combi.data.isBeamContext());
    assertEquals(2, combi.data.getBeamBundleNr());
    assertEquals("writer-2", combi.transform.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_ID));
    assertEquals("2", combi.transform.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR));
    assertEquals("2", combi.transform.getVariable(Const.INTERNAL_VARIABLE_TRANSFORM_BUNDLE_NR));
  }
}
