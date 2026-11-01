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

package org.apache.hop.workflow.actions.tableexists;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

/** Unit test for {@link ActionTableExists} */
class ActionTableExistsTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void emptyConnectionCountsAsError() {
    ActionTableExists action = spy(new ActionTableExists("Table exists"));
    action.setConnection(null);
    action.setTableName("any_table");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    verify(action).logError(contains("No database connection is defined"));
  }

  @Test
  void unknownConnectionCountsAsError() {
    ActionTableExists action = spy(new ActionTableExists("Table exists"));
    action.setConnection("mysql-does-not-exist");
    action.setTableName("any_table");

    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(null);
    action.setParentWorkflowMeta(workflowMeta);

    Workflow workflow = mock(Workflow.class);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);
    action.setParentWorkflow(workflow);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    verify(action).logError(contains("mysql-does-not-exist"));
  }

  @Test
  void existingTableReturnsTrueWithoutErrors() throws Exception {
    ActionTableExists action = new ActionTableExists("Table exists");
    action.setConnection("unit-test-db");
    action.setTableName("present_table");

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);
    action.setParentWorkflowMeta(workflowMeta);

    Workflow workflow = mock(Workflow.class);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);
    action.setParentWorkflow(workflow);

    try (MockedConstruction<Database> databases =
        mockConstruction(
            Database.class,
            (db, context) ->
                when(db.checkTableExists(any(), eq("present_table"))).thenReturn(true))) {
      Result result = action.execute(new Result(), 0);

      assertTrue(result.getResult());
      assertEquals(0, result.getNrErrors());
      assertEquals(1, databases.constructed().size());
    }
  }

  @Test
  void missingTableReturnsFalseWithoutErrors() throws Exception {
    ActionTableExists action = new ActionTableExists("Table exists");
    action.setConnection("unit-test-db");
    action.setTableName("missing_table");

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);
    action.setParentWorkflowMeta(workflowMeta);

    Workflow workflow = mock(Workflow.class);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);
    action.setParentWorkflow(workflow);

    try (MockedConstruction<Database> databases =
        mockConstruction(
            Database.class,
            (db, context) ->
                when(db.checkTableExists(any(), eq("missing_table"))).thenReturn(false))) {
      Result result = action.execute(new Result(), 0);

      assertFalse(result.getResult());
      assertEquals(0, result.getNrErrors());
      assertEquals(1, databases.constructed().size());
    }
  }
}
