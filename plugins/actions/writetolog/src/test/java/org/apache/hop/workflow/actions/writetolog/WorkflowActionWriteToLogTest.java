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
package org.apache.hop.workflow.actions.writetolog;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.verification.VerificationMode;

class WorkflowActionWriteToLogTest {

  private IWorkflowEngine<WorkflowMeta> parentWorkflow;
  private ActionWriteToLog action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    parentWorkflow = mock(Workflow.class);
    doReturn(false).when(parentWorkflow).isStopped();

    action = new ActionWriteToLog();
    action = spy(action);
  }

  @Test
  void errorMessageIsNotLoggedWhenParentJobLogLevelIsNothing() {
    verifyErrorMessageForParentJobLogLevel(LogLevel.NOTHING, never());
  }

  @Test
  void errorMessageIsLoggedWhenParentJobLogLevelIsError() {
    verifyErrorMessageForParentJobLogLevel(LogLevel.ERROR, times(1));
  }

  @Test
  void errorMessageIsLoggedWhenParentJobLogLevelIsMinimal() {
    verifyErrorMessageForParentJobLogLevel(LogLevel.MINIMAL, times(1));
  }

  private void verifyErrorMessageForParentJobLogLevel(
      LogLevel parentJobLogLevel, VerificationMode mode) {
    action.setLogMessage("TEST");
    action.setActionLogLevel(LogLevel.ERROR);

    doReturn(parentJobLogLevel).when(parentWorkflow).getLogLevel();
    action.setParentWorkflow(parentWorkflow);

    ILogChannel logChannel = spy(action.createLogChannel());
    doReturn(logChannel).when(action).createLogChannel();

    action.evaluate(new Result());
    verify(logChannel, mode).logError("TEST" + Const.CR);
  }
}
