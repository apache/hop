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

package org.apache.hop.workflow.actions.waitforsql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Check validates connection plus tableName or customSql, never the ghost property WaitForSQL. */
class ActionWaitForSqlCheckTest {

  @BeforeAll
  static void initPlugins() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginType(ActionPluginType.class);
    registry.registerPluginClass(ActionStart.class.getName(), ActionPluginType.class, Action.class);
    registry.registerPluginClass(
        ActionWaitForSql.class.getName(), ActionPluginType.class, Action.class);
  }

  @Test
  void blankConnectionAndTableAreReported() {
    ActionWaitForSql action = new ActionWaitForSql("Wait for SQL");

    assertEquals(
        List.of("connection cannot be null or blank.", "tableName cannot be null or blank."),
        errors(action));
  }

  @Test
  void whitespaceConnectionAndTableAreReported() {
    ActionWaitForSql action = new ActionWaitForSql("Wait for SQL");
    action.setConnection("  ");
    action.setTableName("\t");
    action.setCustomSql("select 1");

    assertEquals(
        List.of("connection cannot be null or blank.", "tableName cannot be null or blank."),
        errors(action));
  }

  @Test
  void connectionAndTablePassWithoutReadingCustomSql() {
    ActionWaitForSql action = readyTableAction();
    action.setCustomSql("   ");

    assertEquals(List.of(), errors(action));
    assertTrue(texts(action).contains("connection is OK. Value: dummy-db"));
    assertTrue(texts(action).contains("tableName is OK. Value: dummy_table"));
    assertFalse(texts(action).stream().anyMatch(text -> text.contains("customSql")));
  }

  @Test
  void customSqlBlankIsReportedAndTableNameIsIgnored() {
    ActionWaitForSql action = new ActionWaitForSql("Wait for SQL");
    action.setConnection("dummy-db");
    action.setCustomSqlEnabled(true);
    action.setCustomSql("  ");

    assertEquals(List.of("customSql cannot be null or blank."), errors(action));
  }

  @Test
  void customSqlAndConnectionPassWithoutATable() {
    ActionWaitForSql action = new ActionWaitForSql("Wait for SQL");
    action.setConnection("dummy-db");
    action.setCustomSqlEnabled(true);
    action.setCustomSql("SELECT count(*) FROM dummy_table");

    assertEquals(List.of(), errors(action));
    assertTrue(texts(action).contains("customSql is OK. Value: SELECT count(*) FROM..."));
  }

  @Test
  void customSqlStillRequiresAConnection() {
    ActionWaitForSql action = new ActionWaitForSql("Wait for SQL");
    action.setCustomSqlEnabled(true);
    action.setCustomSql("SELECT count(*) FROM dummy_table");
    action.setTableName("dummy_table");

    assertEquals(List.of("connection cannot be null or blank."), errors(action));
  }

  @Test
  void loadedWorkflowDoesNotReportGhostProperty() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    Variables variables = new Variables();
    WorkflowMeta workflowMeta;
    try (InputStream in =
        ActionWaitForSqlCheckTest.class.getResourceAsStream(
            "/wf-wait-for-sql-ghost-property.hwf")) {
      workflowMeta = new WorkflowMeta(in, provider, variables);
    }

    ActionWaitForSql action =
        (ActionWaitForSql) workflowMeta.findAction("Wait for SQL").getAction();
    assertEquals("dummy-db", action.getConnection());
    assertEquals("dummy_table", action.getTableName());
    assertFalse(action.isCustomSqlEnabled());

    List<ICheckResult> remarks = new ArrayList<>();
    workflowMeta.checkActions(remarks, false, null, variables, provider);
    List<String> texts = remarks.stream().map(ICheckResult::getText).toList();

    assertEquals(List.of(), errorTexts(remarks));
    assertFalse(texts.stream().anyMatch(text -> text.contains("WaitForSQL")));
    assertTrue(texts.contains("connection is OK. Value: dummy-db"));
    assertTrue(texts.contains("tableName is OK. Value: dummy_table"));
  }

  private static ActionWaitForSql readyTableAction() {
    ActionWaitForSql action = new ActionWaitForSql("Wait for SQL");
    action.setConnection("dummy-db");
    action.setTableName("dummy_table");
    action.setCustomSqlEnabled(false);
    return action;
  }

  private static List<String> errors(ActionWaitForSql action) {
    return errorTexts(remarks(action));
  }

  private static List<String> texts(ActionWaitForSql action) {
    return remarks(action).stream().map(ICheckResult::getText).toList();
  }

  private static List<ICheckResult> remarks(ActionWaitForSql action) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, new WorkflowMeta(), new Variables(), null);
    return remarks;
  }

  private static List<String> errorTexts(List<ICheckResult> remarks) {
    return remarks.stream()
        .filter(remark -> remark.getType() == ICheckResult.TYPE_RESULT_ERROR)
        .map(ICheckResult::getText)
        .toList();
  }
}
