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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WorkflowAiProposalValidatorTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void validatesAddActionAndHop() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionDummy("Start"));
    ActionMeta dummy = new ActionMeta(new ActionDummy("Dummy"));
    workflowMeta.addAction(start);
    workflowMeta.addAction(dummy);
    workflowMeta.addWorkflowHop(new WorkflowHopMeta(start, dummy));

    AiProposal addAction =
        proposal(
            "ADD_ACTION",
            Map.of(
                "actionPluginId", "DUMMY",
                "name", "Check",
                "locationX", "200",
                "locationY", "100"));
    AiProposal addHop =
        proposal("ADD_WORKFLOW_HOP", Map.of("fromAction", "Start", "toAction", "Check"));
    AiProposal duplicateName =
        proposal(
            "ADD_ACTION",
            Map.of(
                "actionPluginId", "DUMMY",
                "name", "Start",
                "locationX", "50",
                "locationY", "50"));

    List<AiProposalValidation> results =
        WorkflowAiProposalValidator.validate(
            workflowMeta, List.of(addAction, addHop, duplicateName));

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
