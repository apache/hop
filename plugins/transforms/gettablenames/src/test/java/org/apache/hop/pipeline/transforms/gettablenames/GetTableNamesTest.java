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

package org.apache.hop.pipeline.transforms.gettablenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseObjectName;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GetTableNamesTest {
  private TransformMockHelper<GetTableNamesMeta, GetTableNamesData> helper;
  private GetTableNamesData data;
  private Database db;
  private GetTableNames transform;
  private final List<Object[]> rows = new ArrayList<>();

  @BeforeEach
  void setUp() throws Exception {
    helper =
        new TransformMockHelper<>(
            "Get table names", GetTableNamesMeta.class, GetTableNamesData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.iTransformMeta.isIncludeTable()).thenReturn(true);
    when(helper.iTransformMeta.isAddSchemaInOutput()).thenReturn(false);

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getQuotedSchemaTableCombination(any(), any(), anyString()))
        .thenAnswer(
            invocation -> {
              String schema = invocation.getArgument(1);
              String table = invocation.getArgument(2);
              return schema == null ? table : schema + "." + table;
            });

    db = spy(new Database(null, new Variables(), databaseMeta));
    doAnswer(invocation -> "a_system".equals(invocation.getArgument(0)))
        .when(db)
        .isSystemTable(anyString());
    doReturn(new String[0]).when(db).getPrimaryKeyColumnNames(any(), any(), anyString());
    doReturn(new RowMeta()).when(db).getTableFieldsMeta(any(), anyString());
    doAnswer(invocation -> "CREATE TABLE " + invocation.getArgument(0))
        .when(db)
        .getCreateTableStatement(
            anyString(), any(IRowMeta.class), isNull(), anyBoolean(), any(), anyBoolean());

    // Tables in two schemas, an empty schema name lists both
    doReturn(
            List.of(
                new DatabaseObjectName("hop", "public", "t_pub"),
                new DatabaseObjectName("hop", "sales", "a_system"),
                new DatabaseObjectName("hop", "sales", "t_sales")))
        .when(db)
        .getTableObjectNames(isNull(), isNull());

    data = new GetTableNamesData();
    data.db = db;
    data.realSchemaName = null;
    data.realIsSystemObjectFieldName = "isSystem";
    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta(new ValueMetaString("tablename"));

    transform =
        spy(
            new GetTableNames(
                helper.transformMeta,
                helper.iTransformMeta,
                data,
                0,
                helper.pipelineMeta,
                helper.pipeline));
    doAnswer(
            invocation -> {
              rows.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(), any());
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void looksUpEachTableInItsOwnSchema() throws Exception {
    data.realSqlCreationFieldName = "sql";

    transform.processIncludeTable(new Object[4]);

    verify(db).getTableFieldsMeta("public", "t_pub");
    verify(db).getTableFieldsMeta("sales", "t_sales");
    verify(db).getPrimaryKeyColumnNames("hop", "public", "t_pub");
    verify(db).getPrimaryKeyColumnNames("hop", "sales", "t_sales");

    assertEquals(3, rows.size());
    assertRow(rows.get(0), "a_system", true, "CREATE TABLE a_system");
    assertRow(rows.get(1), "t_pub", false, "CREATE TABLE t_pub");
    assertRow(rows.get(2), "t_sales", false, "CREATE TABLE t_sales");
  }

  @Test
  void keepsSchemaInOutputAndPairsItWithTheRightTable() throws Exception {
    when(helper.iTransformMeta.isAddSchemaInOutput()).thenReturn(true);
    data.realSqlCreationFieldName = "sql";

    transform.processIncludeTable(new Object[4]);

    assertEquals(3, rows.size());
    assertRow(rows.get(0), "public.t_pub", false, "CREATE TABLE public.t_pub");
    assertRow(rows.get(1), "sales.a_system", true, "CREATE TABLE sales.a_system");
    assertRow(rows.get(2), "sales.t_sales", false, "CREATE TABLE sales.t_sales");
  }

  @Test
  void skipsTheFieldLookupWithoutSqlCreationField() throws Exception {
    data.realSqlCreationFieldName = null;

    transform.processIncludeTable(new Object[4]);

    verify(db, never()).getTableFieldsMeta(any(), anyString());
    verify(db, never()).getPrimaryKeyColumnNames(any(), any(), anyString());
    assertEquals(3, rows.size());
  }

  @Test
  void pairsViewsWithTheirOwnName() throws Exception {
    when(helper.iTransformMeta.isIncludeTable()).thenReturn(false);
    when(helper.iTransformMeta.isIncludeView()).thenReturn(true);
    when(helper.iTransformMeta.isAddSchemaInOutput()).thenReturn(true);
    doReturn(
            List.of(
                new DatabaseObjectName("hop", "public", "v_pub"),
                new DatabaseObjectName("hop", "sales", "a_system")))
        .when(db)
        .getViewObjectNames(isNull());

    transform.processIncludeView(new Object[4]);

    verify(db).isSystemTable("v_pub");
    verify(db).isSystemTable("a_system");
    assertEquals(2, rows.size());
    assertEquals("public.v_pub", rows.get(0)[0]);
    assertEquals(false, rows.get(0)[1]);
    assertEquals("sales.a_system", rows.get(1)[0]);
    assertEquals(true, rows.get(1)[1]);
  }

  @Test
  void passesTheCatalogToThePrimaryKeyLookupWhenTablesHaveNoSchema() throws Exception {
    // MySQL and MariaDB list a database as a catalog, the same table can be in two of them
    doReturn(
            List.of(
                new DatabaseObjectName("db1", null, "users"),
                new DatabaseObjectName("db2", null, "users")))
        .when(db)
        .getTableObjectNames(isNull(), isNull());
    when(helper.iTransformMeta.isAddSchemaInOutput()).thenReturn(true);
    data.realSqlCreationFieldName = "sql";

    transform.processIncludeTable(new Object[4]);

    verify(db).getPrimaryKeyColumnNames("db1", null, "users");
    verify(db).getPrimaryKeyColumnNames("db2", null, "users");
    verify(db).getTableFieldsMeta("db1", "users");
    verify(db).getTableFieldsMeta("db2", "users");
    assertEquals(2, rows.size());
    assertEquals("db1.users", rows.get(0)[0]);
    assertEquals("db2.users", rows.get(1)[0]);
  }

  @Test
  void addsAPrimaryKeyClauseOnceAndOnlyWhenThereIsAKey() throws Exception {
    doReturn(
            List.of(
                new DatabaseObjectName("hop", "public", "t_none"),
                new DatabaseObjectName("hop", "public", "t_one"),
                new DatabaseObjectName("hop", "public", "t_one_text"),
                new DatabaseObjectName("hop", "public", "t_two")))
        .when(db)
        .getTableObjectNames(isNull(), isNull());
    doReturn(new String[0]).when(db).getPrimaryKeyColumnNames("hop", "public", "t_none");
    doReturn(new String[] {"id"}).when(db).getPrimaryKeyColumnNames("hop", "public", "t_one");
    doReturn(new String[] {"code"})
        .when(db)
        .getPrimaryKeyColumnNames("hop", "public", "t_one_text");
    doReturn(new String[] {"a", "b"}).when(db).getPrimaryKeyColumnNames("hop", "public", "t_two");
    // Like most dialects, the statement marks a numeric key ("id") itself but not a text key
    doAnswer(
            invocation -> {
              String pk = invocation.getArgument(4);
              return "CREATE TABLE "
                  + invocation.getArgument(0)
                  + "\n(\n  n VARCHAR(100)\n"
                  + ("id".equals(pk) ? ", PRIMARY KEY (id)\n" : "")
                  + ")\n;";
            })
        .when(db)
        .getCreateTableStatement(
            anyString(), any(IRowMeta.class), isNull(), anyBoolean(), any(), anyBoolean());
    data.realSqlCreationFieldName = "sql";

    transform.processIncludeTable(new Object[4]);

    verify(db)
        .getCreateTableStatement(
            eq("t_none"), any(IRowMeta.class), isNull(), anyBoolean(), isNull(), anyBoolean());
    verify(db)
        .getCreateTableStatement(
            eq("t_one"), any(IRowMeta.class), isNull(), anyBoolean(), eq("id"), anyBoolean());
    verify(db)
        .getCreateTableStatement(
            eq("t_one_text"),
            any(IRowMeta.class),
            isNull(),
            anyBoolean(),
            eq("code"),
            anyBoolean());
    verify(db)
        .getCreateTableStatement(
            eq("t_two"), any(IRowMeta.class), isNull(), anyBoolean(), isNull(), anyBoolean());
    assertEquals(4, rows.size());
    assertEquals("CREATE TABLE t_none\n(\n  n VARCHAR(100)\n)\n;", rows.get(0)[2]);
    assertEquals(
        "CREATE TABLE t_one\n(\n  n VARCHAR(100)\n, PRIMARY KEY (id)\n)\n;", rows.get(1)[2]);
    assertEquals(
        "CREATE TABLE t_one_text\n(\n  n VARCHAR(100)\n, PRIMARY KEY (code)"
            + Const.CR
            + ")"
            + Const.CR
            + ";",
        rows.get(2)[2]);
    assertEquals(
        "CREATE TABLE t_two\n(\n  n VARCHAR(100)\n, PRIMARY KEY (a, b)"
            + Const.CR
            + ")"
            + Const.CR
            + ";",
        rows.get(3)[2]);
  }

  private static void assertRow(Object[] row, String name, boolean isSystem, String sql) {
    assertEquals(name, row[0]);
    assertEquals(isSystem, row[1]);
    assertEquals(sql, row[2]);
  }
}
