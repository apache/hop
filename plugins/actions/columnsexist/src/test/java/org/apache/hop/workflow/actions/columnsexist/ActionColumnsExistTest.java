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

package org.apache.hop.workflow.actions.columnsexist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.actions.columnsexist.ActionColumnsExist.ColumnExist;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit tests for column exist action. */
class ActionColumnsExistTest {

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerialization() throws Exception {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("unit-test-db");
    databaseMeta.setDatabaseType("NONE");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    ActionColumnsExist action =
        ActionSerializationTestUtil.testSerialization(
            "/columns-exist-action.xml", ActionColumnsExist.class, provider);

    Assertions.assertEquals("unit-test-db", action.getConnectionName());
    Assertions.assertEquals("SCHEMA", action.getSchemaName());
    Assertions.assertEquals("TABLE", action.getTableName());
    Assertions.assertEquals(2, action.getColumns().size());
    Assertions.assertEquals("F1", action.getColumns().get(0).getName());
    Assertions.assertEquals("F2", action.getColumns().get(1).getName());
  }

  @Test
  void testExecuteWithNoConnection() {
    ActionColumnsExist action = new ActionColumnsExist();
    action.setConnectionName(null);
    action.setTableName("test-table");
    List<ColumnExist> columns = new ArrayList<>();
    columns.add(new ColumnExist("c1"));
    action.setColumns(columns);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void testExecuteWithNonExistentConnection() {
    ActionColumnsExist action = new ActionColumnsExist();
    action.setConnectionName("non-existent-db");
    action.setTableName("test-table");
    List<ColumnExist> columns = new ArrayList<>();
    columns.add(new ColumnExist("c1"));
    action.setColumns(columns);

    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(null);

    action.setParentWorkflowMeta(workflowMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void testExecuteWithEmptyTableName() {
    ActionColumnsExist action = new ActionColumnsExist();
    action.setConnectionName("unit-test-db");
    action.setTableName(null);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);

    action.setParentWorkflowMeta(workflowMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void testExecuteWithEmptyColumns() {
    ActionColumnsExist action = new ActionColumnsExist();
    action.setConnectionName("unit-test-db");
    action.setTableName("test-table");
    action.setColumns(new ArrayList<>());

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);

    action.setParentWorkflowMeta(workflowMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void testExecuteSuccess() throws Exception {
    ActionColumnsExist action = spy(new ActionColumnsExist());
    action.setConnectionName("unit-test-db");
    action.setTableName("test-table");
    List<ColumnExist> columns = new ArrayList<>();
    columns.add(new ColumnExist("c1"));
    columns.add(new ColumnExist("c2"));
    action.setColumns(columns);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);
    action.setParentWorkflowMeta(workflowMeta);

    Database db = mock(Database.class);
    when(db.checkTableExists(any(), eq("test-table"))).thenReturn(true);
    when(db.checkColumnExists(any(), eq("test-table"), eq("c1"))).thenReturn(true);
    when(db.checkColumnExists(any(), eq("test-table"), eq("c2"))).thenReturn(true);

    doReturn(db).when(action).getDatabase(databaseMeta);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(2, result.getNrLinesWritten());
    assertEquals(0, result.getEntryNr());
  }

  @Test
  void testExecuteTableNotExists() throws Exception {
    ActionColumnsExist action = spy(new ActionColumnsExist());
    action.setConnectionName("unit-test-db");
    action.setTableName("test-table");
    List<ColumnExist> columns = new ArrayList<>();
    columns.add(new ColumnExist("c1"));
    action.setColumns(columns);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);
    action.setParentWorkflowMeta(workflowMeta);

    Database db = mock(Database.class);
    when(db.checkTableExists(any(), eq("test-table"))).thenReturn(false);

    doReturn(db).when(action).getDatabase(databaseMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void testExecuteColumnNotExists() throws Exception {
    ActionColumnsExist action = spy(new ActionColumnsExist());
    action.setConnectionName("unit-test-db");
    action.setTableName("test-table");
    List<ColumnExist> columns = new ArrayList<>();
    columns.add(new ColumnExist("c1"));
    columns.add(new ColumnExist("c2"));
    action.setColumns(columns);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(anyString(), any())).thenReturn(databaseMeta);
    action.setParentWorkflowMeta(workflowMeta);

    Database db = mock(Database.class);
    when(db.checkTableExists(any(), eq("test-table"))).thenReturn(true);
    when(db.checkColumnExists(any(), eq("test-table"), eq("c1"))).thenReturn(true);
    when(db.checkColumnExists(any(), eq("test-table"), eq("c2"))).thenReturn(false);

    doReturn(db).when(action).getDatabase(databaseMeta);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals(1, result.getNrLinesWritten());
    assertEquals(1, result.getEntryNr());
  }

  @Test
  void testCheckRemarks() {
    ActionColumnsExist action = new ActionColumnsExist();
    action.setTableName(null);
    action.setColumns(null);

    List<ICheckResult> remarks = new ArrayList<>();
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    IVariables variables = new Variables();

    action.check(remarks, workflowMeta, variables, null);

    assertFalse(remarks.isEmpty());
    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, remarks.get(1).getType());
  }
}
