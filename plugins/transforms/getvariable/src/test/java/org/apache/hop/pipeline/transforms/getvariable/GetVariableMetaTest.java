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
package org.apache.hop.pipeline.transforms.getvariable;

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
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transform.TransformSourceSupport;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class GetVariableMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testSerialization() throws Exception {
    GetVariableMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/get-variables-transform.xml", GetVariableMeta.class);

    org.junit.jupiter.api.Assertions.assertEquals(4, meta.getFieldDefinitions().size());
  }

  @Test
  void canStartWithoutInputAndStillConsumesHops() {
    GetVariableMeta meta = new GetVariableMeta();
    assertTrue(meta.canStartWithoutInput());
    assertTrue(meta.consumesMainInput());
  }

  @Test
  void incomingHopIsAllowed() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta from = new TransformMeta("from", new DummyMeta());
    TransformMeta to = new TransformMeta("get-var", new GetVariableMeta());
    pipelineMeta.addTransform(from);
    pipelineMeta.addTransform(to);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(from, to));

    assertFalse(pipelineMeta.isDisallowedMainInputHop(pipelineMeta.getPipelineHop(0)));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());
    assertFalse(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText() != null
                        && r.getText().contains("not reading rows")));
    assertTrue(
        remarks.stream()
            .anyMatch(
                r -> TransformSourceSupport.CHECK_CODE_PIPELINE_SOURCE.equals(r.getErrorCode())));
  }
}
