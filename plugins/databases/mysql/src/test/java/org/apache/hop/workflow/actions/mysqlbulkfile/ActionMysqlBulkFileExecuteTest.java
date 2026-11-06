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

package org.apache.hop.workflow.actions.mysqlbulkfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

/** Issue #9166: High Priority export statement and a connection that does not exist. */
class ActionMysqlBulkFileExecuteTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  /** MySQL 8+ rejects HIGH_PRIORITY together with a locking clause (error 3569). */
  @Test
  void highPriorityHasNoLockingClause() throws Exception {
    String sql = runAndCaptureSql(true);

    assertTrue(sql.startsWith("SELECT HIGH_PRIORITY * INTO OUTFILE '"), sql);
    assertFalse(sql.contains("LOCK IN SHARE MODE"), sql);
  }

  @Test
  void withoutHighPriorityTheRowsAreLockedInShareMode() throws Exception {
    String sql = runAndCaptureSql(false);

    assertTrue(sql.startsWith("SELECT  * INTO OUTFILE '"), sql);
    assertTrue(sql.endsWith(" LOCK IN SHARE MODE"), sql);
  }

  /** Not mocking Database: its constructor threw the NullPointerException that stopped the run. */
  @Test
  void unknownConnectionIsAFalseResultWithOneError() {
    ActionMysqlBulkFile action = newAction();
    action.setConnection("does-not-exist");
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(eq("does-not-exist"), any())).thenReturn(null);
    action.setParentWorkflowMeta(workflowMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  private ActionMysqlBulkFile newAction() {
    ActionMysqlBulkFile action = new ActionMysqlBulkFile("bulkfile");
    action.setTableName("mytable");
    action.setFileName(tempDir.resolve("out.txt").toAbsolutePath().toString());
    return action;
  }

  private String runAndCaptureSql(boolean highPriority) throws Exception {
    ActionMysqlBulkFile action = newAction();
    action.setConnection("mysql");
    action.setHighPriority(highPriority);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(eq("mysql"), any())).thenReturn(mock(DatabaseMeta.class));
    action.setParentWorkflowMeta(workflowMeta);

    try (MockedConstruction<Database> databases =
        mockConstruction(
            Database.class,
            (db, context) -> {
              when(db.checkTableExists(any(), any())).thenReturn(true);
              when(db.prepareSql(anyString())).thenReturn(mock(PreparedStatement.class));
            })) {
      Result result = action.execute(new Result(), 0);

      assertTrue(result.getResult());
      ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
      verify(databases.constructed().get(0)).prepareSql(sql.capture());
      return sql.getValue();
    }
  }
}
