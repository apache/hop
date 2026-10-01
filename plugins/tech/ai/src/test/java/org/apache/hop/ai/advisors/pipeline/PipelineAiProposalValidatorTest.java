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
import org.apache.hop.ai.engine.AiProposalXmlSupportTest;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.gui.Point;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
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

  @Test
  void configureTransformRequiresExistingNameAndFields() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta input = new TransformMeta("TableInput", "Read current customers", null);
    pipelineMeta.addTransform(input);

    AiProposal configure =
        proposal(
            "CONFIGURE_TRANSFORM",
            Map.of(
                "transformName",
                "Read current customers",
                "sql",
                "SELECT customer_id FROM d_customer",
                "connection",
                "test_edw"));
    AiProposal missing =
        proposal("CONFIGURE_TRANSFORM", Map.of("transformName", "Read current customers"));
    AiProposal unknown =
        proposal("CONFIGURE_TRANSFORM", Map.of("transformName", "Missing", "sql", "SELECT 1"));

    java.util.List<AiProposalValidation> results =
        PipelineAiProposalValidator.validate(pipelineMeta, List.of(configure, missing, unknown));
    assertFalse(results.get(0).isBlocked());
    assertTrue(results.get(1).isBlocked());
    assertTrue(results.get(2).isBlocked());
  }

  @Test
  void validatesClipboardAndReplaceTransform() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    DummyMeta dummy = new DummyMeta();
    dummy.setDefault();
    TransformMeta existing = new TransformMeta("Dummy", "Check", dummy);
    existing.setTransformPluginId("Dummy");
    existing.setLocation(new Point(50, 60));
    pipelineMeta.addTransform(existing);
    String xml = AiProposalXmlSupportTest.dummyTransformXml("Other");

    AiProposal clipboard = proposal("CLIPBOARD_TRANSFORMS", Map.of("xml", xml));
    AiProposal replace =
        proposal("REPLACE_TRANSFORM", Map.of("transformName", "Check", "xml", xml));
    AiProposal missing =
        proposal("REPLACE_TRANSFORM", Map.of("transformName", "Missing", "xml", xml));
    AiProposal mismatch =
        proposal(
            "REPLACE_TRANSFORM",
            Map.of(
                "transformName",
                "Check",
                "xml",
                "<transform><name>Check</name><type>Injector</type></transform>"));

    List<AiProposalValidation> results =
        PipelineAiProposalValidator.validate(
            pipelineMeta, List.of(clipboard, replace, missing, mismatch));
    assertFalse(results.get(0).isBlocked(), results.get(0).getReason());
    assertFalse(results.get(1).isBlocked(), results.get(1).getReason());
    assertTrue(results.get(2).isBlocked(), results.get(2).getReason());
    assertTrue(results.get(3).isBlocked(), results.get(3).getReason());
    assertTrue(results.get(3).getReason().contains("does not match"), results.get(3).getReason());
  }

  private static AiProposal proposal(String type, Map<String, String> parameters) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setDescription(type);
    proposal.setParameters(parameters);
    return proposal;
  }
}
