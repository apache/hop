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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.engine.AiProposalXmlSupportTest;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.gui.Point;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PipelineAiProposalApplierTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void appliesAddTransformRenameAndHop() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta input = new TransformMeta("TableInput", "Input", null);
    input.setLocation(100, 100);
    pipelineMeta.addTransform(input);

    AiProposal add =
        proposal(
            "ADD_TRANSFORM",
            Map.of(
                "transformPluginId", "Dummy",
                "name", "Check",
                "locationX", "250",
                "locationY", "100"));
    AiProposal hop =
        proposal("ADD_PIPELINE_HOP", Map.of("fromTransform", "Input", "toTransform", "Check"));
    AiProposal rename =
        proposal("RENAME_TRANSFORM", Map.of("transformName", "Check", "newName", "Validated"));

    PipelineAiProposalApplier.apply(pipelineMeta, List.of(add, hop, rename));

    TransformMeta renamed = pipelineMeta.findTransform("Validated");
    assertNotNull(renamed);
    assertEquals("Dummy", renamed.getTransformPluginId());
    assertEquals(1, pipelineMeta.nrPipelineHops());
  }

  @Test
  void addHopWithoutEndpointThrows() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta input = new TransformMeta("TableInput", "Input", null);
    pipelineMeta.addTransform(input);
    AiProposal hop =
        proposal("ADD_PIPELINE_HOP", Map.of("fromTransform", "Input", "toTransform", "Missing"));
    assertThrows(
        Exception.class, () -> PipelineAiProposalApplier.apply(pipelineMeta, List.of(hop)));
  }

  @Test
  void replaceKeepsNameAndLocationAndSkipsClipboard() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    DummyMeta dummy = new DummyMeta();
    dummy.setDefault();
    TransformMeta existing = new TransformMeta("Dummy", "Check", dummy);
    existing.setLocation(new Point(50, 60));
    pipelineMeta.addTransform(existing);

    String xml = AiProposalXmlSupportTest.dummyTransformXml("Other");
    AiProposal replace =
        proposal("REPLACE_TRANSFORM", Map.of("transformName", "Check", "xml", xml));
    AiProposal clipboard = proposal("CLIPBOARD_TRANSFORMS", Map.of("xml", xml));

    PipelineAiProposalApplier.apply(pipelineMeta, List.of(clipboard, replace));

    assertEquals(1, pipelineMeta.getTransforms().size());
    TransformMeta updated = pipelineMeta.findTransform("Check");
    assertNotNull(updated);
    assertEquals(50, updated.getLocation().x);
    assertEquals(60, updated.getLocation().y);
    assertEquals("Dummy", updated.getTransformPluginId());
  }

  private static AiProposal proposal(String type, Map<String, String> parameters) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setDescription(type);
    proposal.setParameters(parameters);
    return proposal;
  }
}
