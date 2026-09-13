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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.Test;

class WorkflowAiContextBuilderTest {

  @Test
  void serializeStructureIncludesActionsHopsAndFocus() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("demo-workflow");
    ActionMeta start = new ActionMeta(new ActionDummy("Start"));
    ActionMeta dummy = new ActionMeta(new ActionDummy("Dummy"));
    workflowMeta.addAction(start);
    workflowMeta.addAction(dummy);
    workflowMeta.addWorkflowHop(new WorkflowHopMeta(start, dummy));

    String json = WorkflowAiContextBuilder.serializeStructure(workflowMeta, "Start");
    assertTrue(json.contains("\"name\":\"Start\""));
    assertTrue(json.contains("\"name\":\"Dummy\""));
    assertTrue(json.contains("\"from\":\"Start\""));
    assertTrue(json.contains("\"to\":\"Dummy\""));
    assertTrue(json.contains("\"focusAction\":\"Start\""));
  }

  @Test
  void userPromptIncludesFocusActionXml() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta start = new ActionMeta(new ActionDummy("Start"));
    start.setLocation(new Point(10, 20));
    workflowMeta.addAction(start);
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("Configure this action");
    request.setArtifact(workflowMeta);
    request.setVariables(new Variables());
    request.setFocusNodeName("Start");

    String prompt = WorkflowAiContextBuilder.buildUserPrompt(workflowMeta, request);
    assertTrue(prompt.contains("Focus action:\nStart"));
    assertTrue(prompt.contains("Focus action XML:"));
    assertTrue(prompt.contains("<action>"));
  }
}
