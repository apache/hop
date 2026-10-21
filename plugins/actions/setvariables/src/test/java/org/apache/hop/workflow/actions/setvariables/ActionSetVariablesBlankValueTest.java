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

package org.apache.hop.workflow.actions.setvariables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.setvariables.ActionSetVariables.VariableDefinition;
import org.apache.hop.workflow.actions.setvariables.ActionSetVariables.VariableType;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * A blank value sets the variable to an empty string, the same way whether the action was just
 * edited in the dialog (which stores an empty string) or loaded from a saved workflow (an empty
 * {@code <variable_value/>} tag). Up to 2.17 a saved blank value loaded as null and unset the
 * variable instead, so the same workflow behaved differently after it was reopened.
 */
class ActionSetVariablesBlankValueTest {
  private static final String WORKFLOW_VAR = "SET_VARIABLES_BLANK_TEST_WORKFLOW";
  private static final String JVM_VAR = "hop.test.setvariables.blank.jvm";

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopLogStore.init();
    HopClientEnvironment.init();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(JVM_VAR);
  }

  @Test
  void blankValueFromDialogSetsAnEmptyVariable() throws Exception {
    ActionSetVariables action = new ActionSetVariables();
    action.setVariableDefinitions(definitions(""));

    assertVariablesAre("", run(action));
  }

  @Test
  void blankValueLoadedFromWorkflowSetsAnEmptyVariable() throws Exception {
    assertVariablesAre("", run(loadAction(actionXml("<variable_value/>"))));
  }

  @Test
  void blankValueSurvivesSaveAndReload() throws Exception {
    ActionSetVariables edited = new ActionSetVariables();
    edited.setName("set");
    edited.setVariableDefinitions(definitions(""));

    ActionSetVariables reloaded = loadAction("<action>" + edited.getXml() + "</action>");

    assertVariablesAre("", run(reloaded));
  }

  @Test
  void specifiedValueSetsTheVariable() throws Exception {
    assertVariablesAre("new", run(loadAction(actionXml("<variable_value>new</variable_value>"))));
  }

  private static List<VariableDefinition> definitions(String value) {
    return List.of(
        new VariableDefinition(WORKFLOW_VAR, value, VariableType.CURRENT_WORKFLOW),
        new VariableDefinition(JVM_VAR, value, VariableType.JVM));
  }

  private static String actionXml(String valueXml) {
    return "<action><name>set</name><type>SET_VARIABLES</type>"
        + "<replacevars>Y</replacevars><file_variable_type>CURRENT_WORKFLOW</file_variable_type>"
        + "<fields>"
        + "<field><variable_name>"
        + WORKFLOW_VAR
        + "</variable_name>"
        + valueXml
        + "<variable_type>CURRENT_WORKFLOW</variable_type></field>"
        + "<field><variable_name>"
        + JVM_VAR
        + "</variable_name>"
        + valueXml
        + "<variable_type>JVM</variable_type></field>"
        + "</fields></action>";
  }

  private static ActionSetVariables loadAction(String xml) throws Exception {
    Node actionNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "action");
    ActionSetVariables action = new ActionSetVariables();
    action.loadXml(actionNode, new MemoryMetadataProvider(), new Variables());
    return action;
  }

  private static IWorkflowEngine<WorkflowMeta> run(ActionSetVariables action) throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    workflow.setStopped(false);
    workflow.setVariable(WORKFLOW_VAR, "old");
    action.setVariable(WORKFLOW_VAR, "old");
    System.setProperty(JVM_VAR, "old");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    return workflow;
  }

  private static void assertVariablesAre(String expected, IWorkflowEngine<WorkflowMeta> workflow) {
    assertEquals(expected, workflow.getVariable(WORKFLOW_VAR), "workflow variable");
    assertEquals(expected, System.getProperty(JVM_VAR), "JVM property");
  }
}
