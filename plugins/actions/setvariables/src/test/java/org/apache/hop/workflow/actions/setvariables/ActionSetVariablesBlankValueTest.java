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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * A variable saved with a blank value must be unset, not defined as an empty string. Up to 2.17 a
 * saved empty tag {@code <variable_value/>} loaded as null, which clears the JVM property and
 * removes the workflow variable; since 2.18 it loads as an empty string.
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
  void blankValueUnsetsTheVariable() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = runWithValues("<variable_value/>");

    assertNull(workflow.getVariable(WORKFLOW_VAR), "workflow variable should be unset");
    assertNull(System.getProperty(JVM_VAR), "JVM property should be cleared");
  }

  @Test
  void specifiedValueSetsTheVariable() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = runWithValues("<variable_value>new</variable_value>");

    assertEquals("new", workflow.getVariable(WORKFLOW_VAR));
    assertEquals("new", System.getProperty(JVM_VAR));
  }

  private IWorkflowEngine<WorkflowMeta> runWithValues(String valueXml) throws Exception {
    String xml =
        "<action><name>set</name><type>SET_VARIABLES</type>"
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
    Node actionNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "action");
    ActionSetVariables action = new ActionSetVariables();
    action.loadXml(actionNode, new MemoryMetadataProvider(), new Variables());

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
}
