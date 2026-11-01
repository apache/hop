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
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

/** Unit test for {@link ActionTruncateTables} */
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
    ActionTruncateTables action = spy(new ActionTruncateTables());
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
    verify(action).logError(contains("non-existent-db"));
  }

  @Test
  void testExecuteWithBlankConnection() {
    ActionTruncateTables action = spy(new ActionTruncateTables());
    action.setConnection("");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    verify(action).logError(contains("No database connection is defined"));
  }

  @Test
  void findDatabaseRuntimeExceptionIsActionFailureNotWorkflowAbort() {
    ActionTruncateTables action = spy(new ActionTruncateTables());
    action.setConnection("corrupt-db");

    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any()))
        .thenThrow(new HopRuntimeException("Unable to load connection corrupt-db from metadata"));

    Workflow workflow = mock(Workflow.class);
    when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);

    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(workflowMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    verify(action).logError(contains("Unable to load connection corrupt-db from metadata"));
  }

  @Test
  void emptyTableNameInListIsCountedAsErrorAndLaterTablesStillRun() throws Exception {
    ActionTruncateTables action = spy(new ActionTruncateTables());
    action.setConnection("unit-test-db");
    List<TruncateTableItem> items = new ArrayList<>();
    items.add(new TruncateTableItem("", null));
    items.add(new TruncateTableItem("valid_table", null));
    action.setItems(items);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);

    Workflow workflow = mock(Workflow.class);
    when(workflow.isStopped()).thenReturn(false);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);

    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(workflowMeta);

    try (MockedConstruction<Database> databases =
        mockConstruction(
            Database.class,
            (db, context) ->
                when(db.checkTableExists(any(), eq("valid_table"))).thenReturn(true))) {
      Result result = action.execute(new Result(), 0);

      assertFalse(result.getResult());
      assertEquals(1, result.getNrErrors());
      assertEquals(1, result.getNrLinesDeleted());
      verify(action).logError(contains("empty"));
      verify(databases.constructed().get(0)).truncateTable("valid_table");
    }
  }

  @Test
  void emptyPreviousResultRowIsCountedAsErrorAndLaterRowsStillRun() throws Exception {
    ActionTruncateTables action = spy(new ActionTruncateTables());
    action.setConnection("unit-test-db");
    action.setArgFromPrevious(true);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);

    Workflow workflow = mock(Workflow.class);
    when(workflow.isStopped()).thenReturn(false);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);

    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(workflowMeta);

    Result previous = new Result();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("table"));
    rowMeta.addValueMeta(new ValueMetaString("schema"));
    previous.getRows().add(new RowMetaAndData(rowMeta, "", null));
    previous.getRows().add(new RowMetaAndData(rowMeta, "valid_table", null));

    try (MockedConstruction<Database> databases =
        mockConstruction(
            Database.class,
            (db, context) ->
                when(db.checkTableExists(any(), eq("valid_table"))).thenReturn(true))) {
      Result result = action.execute(previous, 0);

      assertFalse(result.getResult());
      assertEquals(1, result.getNrErrors());
      assertEquals(1, result.getNrLinesDeleted());
      verify(action).logError(contains("empty"));
      verify(databases.constructed().get(0)).truncateTable("valid_table");
    }
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
