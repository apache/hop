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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.engine.AiProposalNormalizer;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The slips small models make in proposals are repaired before the user reviews them. */
class AiProposalNormalizerTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void aMissingPluginIdAWorkflowHopAndAMissingLocationAreRepaired() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta output = new TransformMeta("Dummy", "Output", new DummyMeta());
    output.setLocation(300, 100);
    pipelineMeta.addTransform(output);

    // What llama3.2 sent for "add a Dummy with a hop from Output".
    AiProposal add = new AiProposal();
    add.setType("ADD_TRANSFORM");
    add.setDescription("Add a new Dummy transform with a hop from Output");
    add.getParameters().put("name", "Dummy");
    AiProposal hop = new AiProposal();
    hop.setType("ADD_WORKFLOW_HOP");
    hop.getParameters().put("fromAction", "Output");
    hop.getParameters().put("toAction", "Dummy");

    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setArtifact(pipelineMeta);
    List<AiProposalValidation> validations =
        new PipelineAiAdvisor().validateProposals(request, List.of(add, hop));

    assertEquals("Dummy", add.parameter("transformPluginId"));
    assertTrue(add.getDescription().contains("found from the name"));
    assertEquals("460", add.parameter("locationX"), "placed right of Output");
    assertEquals("100", add.parameter("locationY"));
    assertEquals("ADD_PIPELINE_HOP", hop.getType());
    assertEquals("Output", hop.parameter("fromTransform"));
    assertEquals("Dummy", hop.parameter("toTransform"));
    for (AiProposalValidation validation : validations) {
      assertFalse(validation.isBlocked(), validation.getReason());
    }
  }

  @Test
  void aKnownPluginIdIsLeftAlone() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    AiProposal add = new AiProposal();
    add.setType("ADD_TRANSFORM");
    add.getParameters().put("transformPluginId", "Dummy");
    add.getParameters().put("name", "Check");
    add.getParameters().put("locationX", "50");
    add.getParameters().put("locationY", "60");
    org.apache.hop.ai.engine.AiProposalNormalizer.forPipeline(pipelineMeta, List.of(add));
    assertEquals("Dummy", add.parameter("transformPluginId"));
    assertEquals("50", add.parameter("locationX"));
    assertFalse(String.valueOf(add.getDescription()).contains("found from the name"));
  }

  @Test
  void hopsThatNameThePluginIdPointAtTheTransformAddedWithIt() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta output = new TransformMeta("Dummy", "Output", new DummyMeta());
    output.setLocation(300, 100);
    pipelineMeta.addTransform(output);

    AiProposal add = new AiProposal();
    add.setType("ADD_TRANSFORM");
    add.getParameters().put("transformPluginId", "Dummy");
    add.getParameters().put("name", "New Dummy");
    AiProposal hop = new AiProposal();
    hop.setType("ADD_PIPELINE_HOP");
    hop.getParameters().put("fromTransform", "output");
    hop.getParameters().put("toTransform", "Dummy");
    AiProposal reverse = new AiProposal();
    reverse.setType("ADD_PIPELINE_HOP");
    reverse.getParameters().put("fromTransform", "New Dummy");
    reverse.getParameters().put("toTransform", "Output");

    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setArtifact(pipelineMeta);
    List<AiProposalValidation> validations =
        new PipelineAiAdvisor().validateProposals(request, List.of(add, hop, reverse));

    assertEquals("Output", hop.parameter("fromTransform"), "only the case differed");
    assertEquals("New Dummy", hop.parameter("toTransform"), "the plugin id of the added one");
    assertFalse(validations.get(1).isBlocked(), validations.get(1).getReason());
    assertTrue(validations.get(2).isBlocked(), "the reverse hop would make a loop");
  }

  @Test
  void anInventedNamePointsAtTheOnlyTransformThatStartsLikeIt() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta output = new TransformMeta("Dummy", "Output", new DummyMeta());
    TransformMeta dummy = new TransformMeta("Dummy", "Dummy (do nothing)", new DummyMeta());
    pipelineMeta.addTransform(output);
    pipelineMeta.addTransform(dummy);
    AiProposal hop = new AiProposal();
    hop.setType("ADD_PIPELINE_HOP");
    hop.getParameters().put("fromTransform", "Output");
    hop.getParameters().put("toTransform", "dummy-new");

    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setArtifact(pipelineMeta);
    List<AiProposalValidation> validations =
        new PipelineAiAdvisor().validateProposals(request, List.of(hop));

    assertEquals("Dummy (do nothing)", hop.parameter("toTransform"));
    assertFalse(validations.get(0).isBlocked(), validations.get(0).getReason());
  }

  @Test
  void theTransformAProposalIsAboutIsFoundWhenItIsNamedAnotherWay() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta("Dummy", "My Dummy", new DummyMeta()));
    pipelineMeta.addTransform(new TransformMeta("Dummy", "concat", new DummyMeta()));

    // As phi3 wrote them: "name" instead of transformName, the name as a key, a log copy number.
    AiProposal byName = new AiProposal();
    byName.setType("DELETE_TRANSFORM");
    byName.getParameters().put("transformPluginId", "Dummy");
    byName.getParameters().put("name", "My Dummy");
    AiProposal asKey = new AiProposal();
    asKey.setType("DELETE_TRANSFORM");
    asKey.getParameters().put("My Dummy", "");
    AiProposal copyNumber = new AiProposal();
    copyNumber.setType("RENAME_TRANSFORM");
    copyNumber.getParameters().put("transformName", "concat.0");
    copyNumber.getParameters().put("newName", "Concatenate");
    AiProposal hop = new AiProposal();
    hop.setType("DELETE_PIPELINE_HOP");
    hop.getParameters().put("fromTransform", "concat.0");
    hop.getParameters().put("toTransform", "My Dummy");
    AiProposal unknown = new AiProposal();
    unknown.setType("DELETE_TRANSFORM");
    unknown.getParameters().put("name", "Not there");

    AiProposalNormalizer.forPipeline(
        pipelineMeta, List.of(byName, asKey, copyNumber, hop, unknown));

    assertEquals("My Dummy", byName.parameter("transformName"));
    assertEquals("My Dummy", asKey.parameter("transformName"));
    assertFalse(asKey.getParameters().containsKey("My Dummy"));
    assertEquals("concat", copyNumber.parameter("transformName"));
    assertEquals("concat", hop.parameter("fromTransform"));
    assertEquals("", Const.NVL(unknown.parameter("transformName"), ""), "only existing ones");
  }

  @Test
  void aHopWithItsEndsWrittenAsKeysIsRead() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta("Dummy", "concat", new DummyMeta()));
    pipelineMeta.addTransform(new TransformMeta("Dummy", "Dummy (do nothing)", new DummyMeta()));
    AiProposal hop = new AiProposal();
    hop.setType("DELETE_PIPELINE_HOP");
    hop.getParameters().put("concat.0", "");
    hop.getParameters().put("Dummy (do nothing)", "");

    AiProposalNormalizer.forPipeline(pipelineMeta, List.of(hop));

    assertEquals("concat", hop.parameter("fromTransform"));
    assertEquals("Dummy (do nothing)", hop.parameter("toTransform"));
    assertEquals(2, hop.getParameters().size());
  }

  @Test
  void settingsNamedLikeAHopEndStaySettings() {
    AiProposal configure = new AiProposal();
    configure.setType("CONFIGURE_ACTION");
    configure.getParameters().put("actionName", "dbt");
    // The dbt action has a setting called target.
    configure.getParameters().put("target", "prod");
    configure.getParameters().put("plugin", "x");

    AiProposalNormalizer.forWorkflow(
        new org.apache.hop.workflow.WorkflowMeta(), List.of(configure));

    assertEquals("prod", configure.parameter("target"));
    assertEquals("x", configure.parameter("plugin"));
    assertFalse(configure.getParameters().containsKey("toAction"));
  }
}
