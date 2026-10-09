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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.engine.AiProposalXmlSupportTest;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WorkflowAiProposalApplierTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void appliesAddActionRenameAndHop() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionDummy("Start"));
    start.setLocation(100, 100);
    workflowMeta.addAction(start);

    AiProposal add =
        proposal(
            "ADD_ACTION",
            Map.of(
                "actionPluginId", "DUMMY",
                "name", "Check",
                "locationX", "250",
                "locationY", "100"));
    AiProposal hop =
        proposal("ADD_WORKFLOW_HOP", Map.of("fromAction", "Start", "toAction", "Check"));
    AiProposal rename =
        proposal("RENAME_ACTION", Map.of("actionName", "Check", "newName", "Validated"));

    WorkflowAiProposalApplier.apply(workflowMeta, List.of(add, hop, rename));

    ActionMeta renamed = workflowMeta.findAction("Validated");
    assertNotNull(renamed);
    assertEquals(1, workflowMeta.nrWorkflowHops());
  }

  @Test
  void aFailingProposalLeavesTheWorkflowUntouched() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionDummy("Start"));
    start.setLocation(100, 100);
    workflowMeta.addAction(start);

    assertThrows(
        Exception.class,
        () -> WorkflowAiProposalApplier.apply(workflowMeta, List.of(addCheck(), badHop())));
    assertNull(workflowMeta.findAction("Check"), "the first proposal must not stay applied");
    assertEquals(1, workflowMeta.nrActions());
  }

  @Test
  void aProposalThatFailsAfterTheDryRunIsRolledBack() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionDummy("Start"));
    start.setLocation(100, 100);
    workflowMeta.addAction(start);
    Variables variables = new Variables();
    String before = workflowMeta.getXml(variables);

    // Without the dry run, as when the workflow could not be copied.
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                WorkflowAiProposalApplier.applyOrRestore(
                    workflowMeta, before, List.of(addCheck(), badHop()), null, null, variables));
    assertTrue(e.getMessage().contains("Proposal 2"), e.getMessage());
    assertNull(workflowMeta.findAction("Check"), "the first proposal must be rolled back");
    assertNotNull(workflowMeta.findAction("Start"));
    assertEquals(1, workflowMeta.nrActions());
  }

  private static AiProposal addCheck() {
    return proposal(
        "ADD_ACTION",
        Map.of(
            "actionPluginId", "DUMMY",
            "name", "Check",
            "locationX", "250",
            "locationY", "100"));
  }

  private static AiProposal badHop() {
    return proposal("ADD_WORKFLOW_HOP", Map.of("fromAction", "Check", "toAction", "Missing"));
  }

  @Test
  void addHopWithoutEndpointThrows() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionDummy("Start"));
    workflowMeta.addAction(start);
    AiProposal hop =
        proposal("ADD_WORKFLOW_HOP", Map.of("fromAction", "Start", "toAction", "Missing"));
    assertThrows(
        Exception.class, () -> WorkflowAiProposalApplier.apply(workflowMeta, List.of(hop)));
  }

  @Test
  void replaceKeepsNameAndLocationAndSkipsClipboard() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta existing = new ActionMeta(new ActionDummy("Check"));
    existing.setLocation(new Point(40, 70));
    workflowMeta.addAction(existing);

    String xml = AiProposalXmlSupportTest.dummyActionXml("Other");
    AiProposal replace = proposal("REPLACE_ACTION", Map.of("actionName", "Check", "xml", xml));
    AiProposal clipboard = proposal("CLIPBOARD_ACTIONS", Map.of("xml", xml));

    WorkflowAiProposalApplier.apply(workflowMeta, List.of(clipboard, replace));

    assertEquals(1, workflowMeta.getActions().size());
    ActionMeta updated = workflowMeta.findAction("Check");
    assertNotNull(updated);
    assertEquals(40, updated.getLocation().x);
    assertEquals(70, updated.getLocation().y);
  }

  private static AiProposal proposal(String type, Map<String, String> parameters) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setDescription(type);
    proposal.setParameters(parameters);
    return proposal;
  }
}
