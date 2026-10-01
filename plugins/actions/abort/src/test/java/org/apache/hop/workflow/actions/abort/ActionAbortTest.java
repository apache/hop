/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.abort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class ActionAbortTest {

  private IWorkflowEngine<WorkflowMeta> parentWorkflow;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    parentWorkflow = mock(Workflow.class);
    doReturn(LogLevel.ROWLEVEL).when(parentWorkflow).getLogLevel();
  }

  @Test
  void testXmlRoundTrip() throws Exception {
    final String message = "This is a test message";
    ActionAbort action = new ActionAbort();
    action.setMessageAbort(message);
    action.setMessageLogLevel(LogLevel.BASIC);

    ActionAbort action2 = loadFromXml(action.getXml());

    assertEquals(action.getMessageAbort(), action2.getMessageAbort());
    assertEquals(LogLevel.BASIC, action2.getMessageLogLevel());
  }

  @Test
  void defaultLogLevelIsError() {
    assertEquals(LogLevel.ERROR, new ActionAbort().getMessageLogLevel());
  }

  /**
   * Workflows created before the log level became configurable used an "always_log_rows" flag to
   * pick between minimal and error logging. It has to keep selecting the same log level.
   */
  @Test
  void legacyAlwaysLogRowsMapsOntoTheMessageLogLevel() throws Exception {
    assertEquals(
        LogLevel.MINIMAL,
        loadFromXml("<message>abort</message><always_log_rows>Y</always_log_rows>")
            .getMessageLogLevel());
    assertEquals(
        LogLevel.ERROR,
        loadFromXml("<message>abort</message><always_log_rows>N</always_log_rows>")
            .getMessageLogLevel());
    assertEquals(LogLevel.ERROR, loadFromXml("<message>abort</message>").getMessageLogLevel());
  }

  /** Once converted, the old flag isn't written back to the workflow. */
  @Test
  void legacyAlwaysLogRowsIsNotSerializedAgain() throws Exception {
    ActionAbort action =
        loadFromXml("<message>abort</message><always_log_rows>Y</always_log_rows>");

    assertFalse(action.getXml().contains("always_log_rows"));
    assertTrue(action.getXml().contains("<loglevel>Minimal</loglevel>"));
  }

  /**
   * The abort has to fail the workflow on its own, without inheriting the outcome of the action
   * before it. See issue #5982: an action in between the failure and the abort used to flip the
   * workflow back to a success.
   */
  @Test
  void abortFailsTheWorkflowWhateverThePreviousActionReturned() {
    for (LogLevel logLevel : LogLevel.values()) {
      Result previousResult = new Result();
      previousResult.setResult(true);
      previousResult.setNrErrors(0);

      Result result = execute(logLevel, previousResult);

      assertFalse(result.isResult(), "Abort should fail the workflow with log level " + logLevel);
      assertEquals(
          1, result.getNrErrors(), "Abort should report an error for log level " + logLevel);
    }
  }

  @Test
  void abortFailsTheWorkflowAfterAFailedAction() {
    Result previousResult = new Result();
    previousResult.setResult(false);
    previousResult.setNrErrors(1);

    Result result = execute(LogLevel.ERROR, previousResult);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void abortStopsTheParentWorkflow() {
    execute(LogLevel.ERROR, new Result());

    verify(parentWorkflow).stopExecution();
  }

  private Result execute(LogLevel logLevel, Result previousResult) {
    ActionAbort action = new ActionAbort();
    action.setMessageAbort("Aborting the workflow");
    action.setMessageLogLevel(logLevel);
    action.setParentWorkflow(parentWorkflow);

    return action.execute(previousResult, 0);
  }

  /**
   * De-serializes through {@link XmlMetadataUtil}, the way a workflow loads the actions it holds.
   * That path is annotation driven and calls {@link ActionAbort#convertLegacyXml(Node)}, it does
   * not go through {@code loadXml()}.
   */
  private ActionAbort loadFromXml(String xml) throws Exception {
    String actionXml =
        XmlHandler.openTag(ActionMeta.XML_TAG) + xml + XmlHandler.closeTag(ActionMeta.XML_TAG);
    Node node = XmlHandler.loadXmlString(actionXml, ActionMeta.XML_TAG);
    return XmlMetadataUtil.deSerializeFromXml(node, ActionAbort.class, new ActionAbort(), null);
  }
}
