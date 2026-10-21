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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.actions.waitforsql.ActionWaitForSql.SuccessCondition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/** Unit tests for wait for sql action. */
class ActionWaitForSqlTest {

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("unit-test-db");
    databaseMeta.setDatabaseType("NONE");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    ActionWaitForSql action =
        ActionSerializationTestUtil.testSerialization(
            "/wait-for-sql-action.xml", ActionWaitForSql.class, provider);

    assertEquals("unit-test-db", action.getConnection());
    assertEquals("SCHEMATEST", action.getSchemaName());
    assertEquals("TABLETEST", action.getTableName());
    assertEquals("select FIELD from TABLE", action.getCustomSql());
    assertEquals(SuccessCondition.ROWS_COUNT_DIFFERENT, action.getSuccessCondition());
    assertEquals("1", action.getRowsCountValue());
    assertEquals("60", action.getCheckCycleTime());
    assertEquals("99", action.getMaximumTimeout());
    assertFalse(action.isAddRowsResult());
    assertTrue(action.isCustomSqlEnabled());
    assertTrue(action.isUseVars());
    assertFalse(action.isSuccessOnTimeout());
    assertTrue(action.isClearResultList());
  }

  @Test
  void constructorUsesDocumentedDefaults() {
    ActionWaitForSql action = new ActionWaitForSql("Wait for SQL");

    assertEquals(
        List.of(true, false, false, false, false),
        List.of(
            action.isClearResultList(),
            action.isAddRowsResult(),
            action.isUseVars(),
            action.isCustomSqlEnabled(),
            action.isSuccessOnTimeout()));
    assertNull(action.getConnection());
    assertNull(action.getSchemaName());
    assertNull(action.getTableName());
    assertNull(action.getCustomSql());
    assertEquals(
        List.of("0", "0", "60"),
        List.of(
            action.getRowsCountValue(), action.getMaximumTimeout(), action.getCheckCycleTime()));
    assertEquals(SuccessCondition.ROWS_COUNT_GREATER, action.getSuccessCondition());
    assertEquals("SELECT count(*) FROM ", ActionWaitForSql.getSelectCount());
  }

  @Test
  void accessorsRoundTrip() {
    ActionWaitForSql action = getActionWaitForSql();

    assertEquals(
        List.of(false, true, true, true, true),
        List.of(
            action.isClearResultList(),
            action.isAddRowsResult(),
            action.isUseVars(),
            action.isCustomSqlEnabled(),
            action.isSuccessOnTimeout()));
    assertEquals(
        List.of("dummy-db", "public", "dummy_table", "select 1", "3", "30", "5"),
        List.of(
            action.getConnection(),
            action.getSchemaName(),
            action.getTableName(),
            action.getCustomSql(),
            action.getRowsCountValue(),
            action.getMaximumTimeout(),
            action.getCheckCycleTime()));
    assertEquals(SuccessCondition.ROWS_COUNT_EQUAL, action.getSuccessCondition());
    assertEquals("rows_count_equal", action.getSuccessCondition().getCode());
    assertEquals(
        BaseMessages.getString(
            ActionWaitForSql.class, "ActionWaitForSQL.SuccessWhenRowCountEqual.Label"),
        action.getSuccessCondition().getDescription());
    assertEquals(
        SuccessCondition.ROWS_COUNT_EQUAL, SuccessCondition.lookupCode("rows_count_equal"));
  }

  private static @NotNull ActionWaitForSql getActionWaitForSql() {
    ActionWaitForSql action = new ActionWaitForSql("Action wait for SQL");
    action.setClearResultList(false);
    action.setAddRowsResult(true);
    action.setUseVars(true);
    action.setCustomSqlEnabled(true);
    action.setSuccessOnTimeout(true);
    action.setConnection("dummy-db");
    action.setSchemaName("public");
    action.setTableName("dummy_table");
    action.setCustomSql("select 1");
    action.setRowsCountValue("3");
    action.setMaximumTimeout("30");
    action.setCheckCycleTime("5");
    action.setSuccessCondition(SuccessCondition.ROWS_COUNT_EQUAL);
    return action;
  }
}
