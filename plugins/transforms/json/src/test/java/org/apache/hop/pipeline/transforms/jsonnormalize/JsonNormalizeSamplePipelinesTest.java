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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
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
}
