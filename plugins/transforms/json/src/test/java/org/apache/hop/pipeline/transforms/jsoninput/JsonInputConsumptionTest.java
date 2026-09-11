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
package org.apache.hop.pipeline.transforms.jsoninput;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class JsonInputConsumptionTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void fileModeDoesNotConsumeMainInput() {
    JsonInputMeta meta = new JsonInputMeta();
    assertFalse(meta.isInFields());
    assertFalse(meta.consumesMainInput());
    assertTrue(meta.canStartWithoutInput());
  }

  @Test
  void sourceFromPreviousTransformConsumesMainInput() {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setInFields(true);
    assertTrue(meta.consumesMainInput());
    assertFalse(meta.canStartWithoutInput());
  }

  @Test
  void verifyFlagsHopIntoFileMode() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta from = new TransformMeta("from", new DummyMeta());
    JsonInputMeta jsonMeta = new JsonInputMeta();
    TransformMeta to = new TransformMeta("json", jsonMeta);
    pipelineMeta.addTransform(from);
    pipelineMeta.addTransform(to);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(from, to));

    assertTrue(pipelineMeta.isDisallowedMainInputHop(pipelineMeta.getPipelineHop(0)));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText() != null
                        && r.getText().contains("not reading rows")));
  }

  @Test
  void verifyAllowsHopWhenSourceIsFromField() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta from = new TransformMeta("from", new DummyMeta());
    JsonInputMeta jsonMeta = new JsonInputMeta();
    jsonMeta.setInFields(true);
    TransformMeta to = new TransformMeta("json", jsonMeta);
    pipelineMeta.addTransform(from);
    pipelineMeta.addTransform(to);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(from, to));

    assertFalse(pipelineMeta.isDisallowedMainInputHop(pipelineMeta.getPipelineHop(0)));
  }
}
