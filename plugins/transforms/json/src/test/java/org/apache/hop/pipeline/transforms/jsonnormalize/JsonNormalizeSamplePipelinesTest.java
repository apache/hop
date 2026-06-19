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

package org.apache.hop.pipeline.transforms.jsonnormalize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.TransformRowsCollector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Ensures shipped sample pipelines load and reference JsonNormalizeInput. */
class JsonNormalizeSamplePipelinesTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void sampleOrdersPipelineLoads() throws Exception {
    Path p = Paths.get("src/main/samples/transforms/json-normalize-orders.hpl").toAbsolutePath();
    PipelineMeta meta =
        new PipelineMeta(p.toString(), new MemoryMetadataProvider(), new Variables());
    assertEquals(2, meta.getTransforms().size());
    TransformMeta tm = meta.findTransform("read normalized orders");
    assertTrue(tm.getTransform() instanceof JsonNormalizeInputMeta);
    JsonNormalizeInputMeta jnm = (JsonNormalizeInputMeta) tm.getTransform();
    assertEquals("$.orders[*]", jnm.getRecordPath());
    assertEquals(5, jnm.getInputFields().size());
  }

  @Test
  void sampleRootArrayPipelineLoads() throws Exception {
    Path p =
        Paths.get("src/main/samples/transforms/json-normalize-root-array.hpl").toAbsolutePath();
    PipelineMeta meta =
        new PipelineMeta(p.toString(), new MemoryMetadataProvider(), new Variables());
    TransformMeta tm = meta.findTransform("read normalized events");
    assertTrue(tm.getTransform() instanceof JsonNormalizeInputMeta);
    JsonNormalizeInputMeta jnm = (JsonNormalizeInputMeta) tm.getTransform();
    assertEquals("$[*]", jnm.getRecordPath());
  }

  @Test
  void sampleComplexTransactionsPipelineLoads() throws Exception {
    Path p =
        Paths.get("src/main/samples/transforms/json-normalize-complex-transactions.hpl")
            .toAbsolutePath();
    PipelineMeta meta =
        new PipelineMeta(p.toString(), new MemoryMetadataProvider(), new Variables());
    assertEquals(2, meta.getTransforms().size());
    TransformMeta tm = meta.findTransform("read complex transactions");
    assertTrue(tm.getTransform() instanceof JsonNormalizeInputMeta);
    JsonNormalizeInputMeta jnm = (JsonNormalizeInputMeta) tm.getTransform();
    assertEquals("$.transactions[*]", jnm.getRecordPath());
    assertEquals(25, jnm.getInputFields().size());
  }

  @Test
  void sampleHopConfigLayersPipelineLoads() throws Exception {
    Path p =
        Paths.get("src/main/samples/transforms/json-normalize-hop-config-layers.hpl")
            .toAbsolutePath();
    PipelineMeta meta =
        new PipelineMeta(p.toString(), new MemoryMetadataProvider(), new Variables());
    assertEquals(7, meta.getTransforms().size());
    TransformMeta wide = meta.findTransform("wide hop-config root");
    assertTrue(wide.getTransform() instanceof JsonNormalizeInputMeta);
    assertEquals("$", ((JsonNormalizeInputMeta) wide.getTransform()).getRecordPath());
    TransformMeta vars = meta.findTransform("variables as rows");
    JsonNormalizeInputMeta varMeta = (JsonNormalizeInputMeta) vars.getTransform();
    assertTrue(varMeta.isInFields());
    assertEquals("variables", varMeta.getFieldValue());
    assertEquals("$[*]", varMeta.getRecordPath());
  }

  @Test
  void sampleHopConfigLayersPipelineFeedsAllBranches() throws Exception {
    Path hpl =
        Paths.get("src/main/samples/transforms/json-normalize-hop-config-layers.hpl")
            .toAbsolutePath();
    Path hopConfigFolder =
        Paths.get("../../../integration-tests/json/resources/hop-config-layers-it")
            .toAbsolutePath()
            .normalize();
    Variables variables = new Variables();
    variables.setVariable("HOP_CONFIG_FOLDER", hopConfigFolder.toString());

    PipelineMeta meta = new PipelineMeta(hpl.toString(), new MemoryMetadataProvider(), variables);
    TransformMeta wide = meta.findTransform("wide hop-config root");
    assertFalse(
        wide.isDistributes(),
        "wide step must copy rows to all branches (distribute=N), not round-robin");

    Pipeline pipeline = new LocalPipelineEngine(meta, variables, null);
    pipeline.prepareExecution();
    TransformRowsCollector cVars = attachCollector(pipeline, "preview variables");
    TransformRowsCollector cLife = attachCollector(pipeline, "preview lifecycles");
    TransformRowsCollector cProj = attachCollector(pipeline, "preview project configs");
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getResult().getNrErrors(), "Pipeline errors: " + pipeline.getResult());
    assertEquals(2, cVars.getRowsRead().size(), "variables branch should emit 2 rows");
    assertEquals(1, cLife.getRowsRead().size(), "lifecycle branch should emit 1 row");
    assertEquals(1, cProj.getRowsRead().size(), "project configuration branch should emit 1 row");
  }

  private static TransformRowsCollector attachCollector(Pipeline pipeline, String transformName)
      throws HopException {
    IEngineComponent component = pipeline.findComponent(transformName, 0);
    TransformRowsCollector collector = new TransformRowsCollector();
    component.addRowListener(collector);
    return collector;
  }
}
