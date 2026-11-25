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

package org.apache.hop.workflow.actions.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.lineage.LineageRelationalIoEmitter;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

/** The lineage the SQL action reports when its SQL runs, or fails partway. */
class ActionSqlLineageTest {

  private static final String SCRIPT =
      "INSERT INTO sales.orders SELECT * FROM staging.orders; DELETE FROM staging.missing";

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
  }

  private static ActionSql action(DatabaseMeta databaseMeta, Workflow workflow) {
    ActionSql action = new ActionSql("Load orders");
    action.setConnection("warehouse");
    action.setSql(SCRIPT);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);
    action.setParentWorkflowMeta(workflowMeta);
    action.setParentWorkflow(workflow);
    return action;
  }

  /** The statements before the failing one may have written tables. */
  @Test
  void aScriptThatFailsPartwayIsReportedAsFailed() {
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    Workflow workflow = mock(Workflow.class);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);
    ActionSql action = action(databaseMeta, workflow);

    try (MockedStatic<LineageRelationalIoEmitter> emitter =
            mockStatic(LineageRelationalIoEmitter.class);
        MockedConstruction<Database> databases =
            mockConstruction(
                Database.class,
                (db, context) ->
                    doThrow(new HopDatabaseException("relation staging.missing does not exist"))
                        .when(db)
                        .execStatements(anyString()))) {
      Result result = action.execute(new Result(), 0);

      assertFalse(result.isResult());
      assertEquals(1, databases.constructed().size());
      emitter.verify(
          () ->
              LineageRelationalIoEmitter.emitWorkflowActionRelationalExec(
                  eq(workflow),
                  eq(action),
                  eq(databaseMeta),
                  eq(SCRIPT),
                  eq(false),
                  contains("staging.missing")));
    }
  }

  @Test
  void aScriptThatRanIsReportedAsSucceeded() {
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    Workflow workflow = mock(Workflow.class);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);
    ActionSql action = action(databaseMeta, workflow);

    try (MockedStatic<LineageRelationalIoEmitter> emitter =
            mockStatic(LineageRelationalIoEmitter.class);
        MockedConstruction<Database> databases = mockConstruction(Database.class)) {
      Result result = action.execute(new Result(), 0);

      assertEquals(0, result.getNrErrors());
      emitter.verify(
          () ->
              LineageRelationalIoEmitter.emitWorkflowActionRelationalExec(
                  eq(workflow), eq(action), eq(databaseMeta), eq(SCRIPT), anyBoolean(), any()),
          times(1));
      emitter.verify(
          () ->
              LineageRelationalIoEmitter.emitWorkflowActionRelationalExec(
                  any(), any(), any(), any(), eq(true), any()));
    }
  }
}
