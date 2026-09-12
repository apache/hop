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

package org.apache.hop.ai.advisors.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.Test;

class WorkflowAiAdvisorTest {

  @Test
  void scenariosAndInclusionsArePresent() {
    WorkflowAiAdvisor advisor = new WorkflowAiAdvisor();
    assertEquals(WorkflowAiAdvisor.ID, advisor.getId());
    assertEquals(4, advisor.listScenarios().size());
    assertEquals(5, advisor.listInclusions().size());
    advisor
        .listInclusions()
        .forEach(
            inclusion -> {
              assertTrue(
                  inclusion.getDescription() != null && !inclusion.getDescription().isBlank(),
                  inclusion.getId());
              assertFalse(inclusion.isDefaultSelected(), inclusion.getId());
              assertTrue(
                  inclusion.getSummary() != null && !inclusion.getSummary().isBlank(),
                  inclusion.getId());
            });
    assertFalse(advisor.listBaselineSharing().isEmpty());
  }

  @Test
  void buildPromptLoadsPreambleAndStructure() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("orders");
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("How do I add a success hop?");
    request.setScenarioId("workflow-general");
    request.setArtifact(workflowMeta);
    request.setVariables(new Variables());

    AiAdvisorPrompt prompt = new WorkflowAiAdvisor().buildPrompt(request);
    assertTrue(prompt.getSystemPrompt().contains("Apache Hop workflow"));
    assertTrue(prompt.getUserPrompt().contains("How do I add a success hop?"));
    assertTrue(prompt.getUserPrompt().contains("\"name\":\"orders\""));
    assertTrue(prompt.getSystemPrompt().contains("hop_proposals"));
  }

  @Test
  void summarizeAppliedUsesPreview() {
    AiProposal proposal = new AiProposal();
    proposal.setType("ADD_ACTION");
    proposal.setParameters(Map.of("name", "Check", "actionPluginId", "DUMMY"));
    assertEquals("ADD_ACTION: Check (DUMMY)", new WorkflowAiAdvisor().summarizeApplied(proposal));
  }
}
