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

package org.apache.hop.workflow.actions.truncatetables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ActionTruncateTablesTest {

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void testExecuteWithNoConnection() {
    ActionTruncateTables action = new ActionTruncateTables();
    action.setConnection(null);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void testExecuteWithNonExistentConnection() {
    ActionTruncateTables action = new ActionTruncateTables();
    action.setConnection("non-existent-db");

    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(null);

    Workflow workflow = mock(Workflow.class);
    when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);

    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(workflowMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void testCheckRemarks() {
    ActionTruncateTables action = new ActionTruncateTables();
    action.setConnection(null);

    java.util.List<org.apache.hop.core.ICheckResult> remarks = new java.util.ArrayList<>();
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(any(), any())).thenReturn(null);

    org.apache.hop.core.variables.Variables variables =
        new org.apache.hop.core.variables.Variables();
    action.check(remarks, workflowMeta, variables, null);

    assertFalse(remarks.isEmpty());
    assertEquals(org.apache.hop.core.ICheckResult.TYPE_RESULT_WARNING, remarks.get(0).getType());
  }

  @Test
  void testCheckRemarksWithArgFromPrevious() {
    ActionTruncateTables action = new ActionTruncateTables();
    action.setArgFromPrevious(true);
    action.setConnection(null);

    java.util.List<org.apache.hop.core.ICheckResult> remarks = new java.util.ArrayList<>();
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);

    action.check(remarks, workflowMeta, new org.apache.hop.core.variables.Variables(), null);

    assertFalse(remarks.isEmpty());
    assertEquals(org.apache.hop.core.ICheckResult.TYPE_RESULT_WARNING, remarks.get(0).getType());
  }
}
