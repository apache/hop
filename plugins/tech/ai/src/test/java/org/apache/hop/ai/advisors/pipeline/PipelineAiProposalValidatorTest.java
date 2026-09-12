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

package org.apache.hop.ai.advisors.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PipelineAiProposalValidatorTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void validatesAddTransformAndHop() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta input = new TransformMeta("TableInput", "Input", null);
    TransformMeta output = new TransformMeta("TableOutput", "Output", null);
    input.setLocation(100, 100);
    output.setLocation(300, 100);
    pipelineMeta.addTransform(input);
    pipelineMeta.addTransform(output);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, output));

    AiProposal addTransform =
        proposal(
            "ADD_TRANSFORM",
            Map.of(
                "transformPluginId", "Dummy",
                "name", "Check",
                "locationX", "200",
                "locationY", "100"));
    AiProposal addHop =
        proposal("ADD_PIPELINE_HOP", Map.of("fromTransform", "Input", "toTransform", "Check"));
    AiProposal duplicateName =
        proposal(
            "ADD_TRANSFORM",
            Map.of(
                "transformPluginId", "Dummy",
                "name", "Input",
                "locationX", "50",
                "locationY", "50"));

    List<AiProposalValidation> results =
        PipelineAiProposalValidator.validate(
            pipelineMeta, List.of(addTransform, addHop, duplicateName));

    assertFalse(results.get(0).isBlocked());
    assertFalse(results.get(1).isBlocked());
    assertTrue(results.get(2).isBlocked());
  }

  private static AiProposal proposal(String type, Map<String, String> parameters) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setDescription(type);
    proposal.setParameters(parameters);
    return proposal;
  }
}
