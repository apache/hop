/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.advisor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class AiAdvisorPluginTypeTest {

  @Test
  void singleton() {
    assertSame(AiAdvisorPluginType.getInstance(), AiAdvisorPluginType.getInstance());
  }

  @Test
  void extractsAnnotation() {
    AiAdvisorPluginType type = AiAdvisorPluginType.getInstance();
    AiAdvisorPlugin annotation = FakeAdvisor.class.getAnnotation(AiAdvisorPlugin.class);
    assertNotNull(annotation);
    assertEquals("pipeline-advisor", type.extractID(annotation));
    assertEquals("Pipeline AI Help", type.extractName(annotation));
    assertEquals("For tests", type.extractDesc(annotation));
    assertEquals("ai-help.svg", type.extractImageFile(annotation));
    assertEquals("/docs/pipeline-ai.html", type.extractDocumentationUrl(annotation));
    assertEquals("hop-ai", type.extractClassLoaderGroup(annotation));
    assertArrayEquals(
        new String[] {AiAdvisorLocations.PIPELINE_GRAPH}, type.extractKeywords(annotation));
  }

  @Test
  void pluginTypeIds() {
    AiAdvisorPluginType type = AiAdvisorPluginType.getInstance();
    assertEquals("AI_ADVISORS", type.getId());
    assertEquals("AI advisors", type.getName());
  }

  @Test
  void parseResponseDefaultIsChatOnly() {
    FakeAdvisor advisor = new FakeAdvisor();
    AiAdvisorResponse response = advisor.parseResponse("  hello  ");
    assertEquals("hello", response.getMarkdownAdvice());
    assertEquals("  hello  ", response.getRawResponse());
    assertTrue(response.getProposals().isEmpty());
  }

  @Test
  void parseResponseDefaultExtractsHopProposals() {
    FakeAdvisor advisor = new FakeAdvisor();
    AiAdvisorResponse response =
        advisor.parseResponse(
            """
            Advice.

            ```hop_proposals
            {"proposals":[{"type":"CREATE_HUB","description":"Create H_CUSTOMER"}]}
            ```
            """);
    assertEquals("Advice.", response.getMarkdownAdvice());
    assertEquals(1, response.getProposals().size());
    assertEquals("CREATE_HUB", response.getProposals().get(0).getType());
    assertEquals(null, advisor.previewProposal(response.getProposals().get(0)));
  }

  @Test
  void validateAndApplyDefaultsAreNoOps() throws HopException {
    FakeAdvisor advisor = new FakeAdvisor();
    assertTrue(advisor.validateProposals(new AiAdvisorRequest(), List.of()).isEmpty());
    advisor.applyProposals(new AiAdvisorRequest(), List.of());
    advisor.afterApply(new AiAdvisorRequest(), List.of());
    assertTrue(advisor.isAvailable());
    assertTrue(advisor.listInclusionChoices("catalog", new AiAdvisorRequest()).isEmpty());
    assertTrue(advisor.getStandingContext().isEmpty());
  }

  @Test
  void selectedInclusionIdsRoundTrip() {
    AiAdvisorRequest request = new AiAdvisorRequest();
    assertTrue(request.selectedInclusionIds("catalog").isEmpty());
    request.getInclusionSelections().put("catalog", List.of("SRC_ORDERS"));
    assertEquals(List.of("SRC_ORDERS"), request.selectedInclusionIds("catalog"));
    assertTrue(request.selectedInclusionIds(null).isEmpty());
  }

  @Test
  void summarizeAppliedUsesTypeAndDescription() {
    FakeAdvisor advisor = new FakeAdvisor();
    AiProposal proposal = new AiProposal();
    proposal.setType("ADD_HUB");
    proposal.setDescription("Create H_CUSTOMER");
    assertEquals("ADD_HUB: Create H_CUSTOMER", advisor.summarizeApplied(proposal));
    AiProposal typeOnly = new AiProposal();
    typeOnly.setType("ADD_HUB");
    assertEquals("ADD_HUB", advisor.summarizeApplied(typeOnly));
    assertEquals("unknown change", advisor.summarizeApplied(null));
  }

  @AiAdvisorPlugin(
      id = "pipeline-advisor",
      name = "Pipeline AI Help",
      description = "For tests",
      image = "ai-help.svg",
      documentationUrl = "/docs/pipeline-ai.html",
      locations = {AiAdvisorLocations.PIPELINE_GRAPH},
      classLoaderGroup = "hop-ai")
  static class FakeAdvisor implements IAiAdvisor {
    @Override
    public String getId() {
      return "pipeline-advisor";
    }

    @Override
    public String getName() {
      return "Pipeline AI Help";
    }

    @Override
    public String[] getLocations() {
      return new String[] {AiAdvisorLocations.PIPELINE_GRAPH};
    }

    @Override
    public List<AiAdvisorScenario> listScenarios() {
      return List.of(new AiAdvisorScenario("general", "General", "Default"));
    }

    @Override
    public AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) {
      return new AiAdvisorPrompt("system", request.getUserPrompt());
    }
  }
}
