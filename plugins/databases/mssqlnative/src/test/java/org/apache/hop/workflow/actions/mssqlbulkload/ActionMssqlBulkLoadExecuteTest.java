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

package org.apache.hop.workflow.actions.mssqlbulkload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
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

/** Issue #9167: the BULK INSERT statement and the result of the checks that stop the load. */
class ActionMssqlBulkLoadExecuteTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void specificCodepageSendsTheValue() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setCodePage("Specific");
    action.setSpecificCodePage("65001");

    String sql = runAndCaptureSql(action);

    assertTrue(sql.contains("CODEPAGE = '65001'"), sql);
    assertFalse(sql.contains("Specific"), sql);
  }

  @Test
  void specificCodepageSavedByATranslatedDialogSendsTheValue() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setCodePage("Spécifique");
    action.setSpecificCodePage("1252");

    String sql = runAndCaptureSql(action);

    assertTrue(sql.contains("CODEPAGE = '1252'"), sql);
  }

  @Test
  void specificCodepageFromAVariableSendsTheValue() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setCodePage("Specific");
    action.setSpecificCodePage("${CP}");
    action.setVariable("CP", "raw");

    String sql = runAndCaptureSql(action);

    assertTrue(sql.contains("CODEPAGE = 'raw'"), sql);
  }

  @Test
  void otherCodepageIsSentAsIs() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setCodePage("RAW");
    action.setSpecificCodePage("65001");

    String sql = runAndCaptureSql(action);

    assertTrue(sql.contains("CODEPAGE = 'RAW'"), sql);
    assertFalse(sql.contains("65001"), sql);
  }

  @Test
  void specificCodepageThatIsNotACodepageIsOneError() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setCodePage("Specific");
    action.setSpecificCodePage("${CP}");
    action.setVariable("CP", "65001', FIRE_TRIGGERS --");

    assertOneErrorWithoutLoad(action);
  }

  @Test
  void startAndEndLineAreBothSent() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setStartFile(2);
    action.setEndFile(3);

    String sql = runAndCaptureSql(action);

    assertTrue(sql.contains(",FIRSTROW=2,LASTROW=3,"), sql);
  }

  @Test
  void notAnMssqlConnectionIsOneError() throws Exception {
    assertOneErrorWithoutLoad(newAction("MYSQL"));
  }

  @Test
  void unknownConnectionIsOneError() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setConnection("does-not-exist");

    assertOneErrorWithoutLoad(action);
  }

  @Test
  void emptyFieldTerminatorIsOneError() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setFieldTerminator("");

    assertOneErrorWithoutLoad(action);
  }

  @Test
  void emptySpecificCodepageIsOneError() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    action.setCodePage("Specific");
    action.setSpecificCodePage("");

    assertOneErrorWithoutLoad(action);
  }

  @Test
  void existingErrorFileWithoutDateTimeIsOneError() throws Exception {
    ActionMssqlBulkLoad action = newAction("MSSQLNATIVE");
    Path errorFile = Files.writeString(tempDir.resolve("errors.txt"), "");
    action.setErrorFileName(errorFile.toAbsolutePath().toString());
    action.setAddDatetime(false);

    assertOneErrorWithoutLoad(action);
  }

  private ActionMssqlBulkLoad newAction(String pluginId) throws Exception {
    Path dataFile = Files.writeString(tempDir.resolve("customers.csv"), "1;a\n2;b\n3;c\n");
    ActionMssqlBulkLoad action = new ActionMssqlBulkLoad("bulkload");
    action.setFileName(dataFile.toAbsolutePath().toString());
    action.setTableName("customers");
    action.setFieldTerminator(";");
    action.setConnection("mssql");

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getPluginId()).thenReturn(pluginId);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.findDatabase(eq("mssql"), any())).thenReturn(databaseMeta);
    action.setParentWorkflowMeta(workflowMeta);
    return action;
  }

  private static MockedConstruction<Database> mockDatabases() {
    return mockConstruction(
        Database.class, (db, context) -> when(db.checkTableExists(any(), any())).thenReturn(true));
  }

  private static String runAndCaptureSql(ActionMssqlBulkLoad action) throws Exception {
    try (MockedConstruction<Database> databases = mockDatabases()) {
      Result result = action.execute(new Result(), 0);

      assertTrue(result.getResult());
      assertEquals(0, result.getNrErrors());
      ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
      verify(databases.constructed().get(0)).execStatement(sql.capture());
      return sql.getValue();
    }
  }

  private static void assertOneErrorWithoutLoad(ActionMssqlBulkLoad action) throws Exception {
    try (MockedConstruction<Database> databases = mockDatabases()) {
      Result result = action.execute(new Result(), 0);

      assertFalse(result.getResult());
      assertEquals(1, result.getNrErrors());
      for (Database db : databases.constructed()) {
        verify(db, never()).execStatement(anyString());
      }
    }
  }
}
