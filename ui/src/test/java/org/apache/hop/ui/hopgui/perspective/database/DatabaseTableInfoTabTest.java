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

package org.apache.hop.ui.hopgui.perspective.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.junit.jupiter.api.Test;

class DatabaseTableInfoTabTest {

  @Test
  void withIndexStatementsAppendsCreateIndex() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    IVariables variables = mock(IVariables.class);
    when(meta.getQuotedSchemaTableCombination(any(), eq("public"), eq("t")))
        .thenReturn("\"public\".t");
    when(meta.quoteField("idx_name")).thenReturn("idx_name");
    when(meta.quoteField("name")).thenReturn("name");

    DatabaseIndexInfo index = new DatabaseIndexInfo();
    index.setName("idx_name");
    index.setUnique(true);
    index.getColumns().add("name");

    String ddl =
        DatabaseTableInfoTab.withIndexStatements(
            meta,
            variables,
            "public",
            "t",
            "CREATE TABLE \"public\".t (id INTEGER);",
            List.of(index));
    assertTrue(ddl.contains("CREATE UNIQUE INDEX idx_name ON \"public\".t (name);"));
  }

  @Test
  void withIndexStatementsSkipsCatalogDdlThatAlreadyHasKeys() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    IVariables variables = mock(IVariables.class);
    DatabaseIndexInfo index = new DatabaseIndexInfo();
    index.setName("PRIMARY");
    index.getColumns().add("id");
    String catalog = "CREATE TABLE t (\n  id INT,\n  PRIMARY KEY (id)\n);";
    assertEquals(
        catalog,
        DatabaseTableInfoTab.withIndexStatements(
            meta, variables, null, "t", catalog, List.of(index)));
  }

  @Test
  void catalogDdlIncludesIndexesDetectsShowCreate() {
    assertTrue(
        DatabaseTableInfoTab.catalogDdlIncludesIndexes(
            "CREATE TABLE `t` (\n  `id` int,\n  KEY `idx` (`id`)\n)"));
    assertFalse(DatabaseTableInfoTab.catalogDdlIncludesIndexes("CREATE TABLE t (id INTEGER);"));
  }

  @Test
  void columnInfosIncludesDefinition() {
    ColumnInfo[] infos = DatabaseTableInfoTab.columnInfos();
    assertEquals(6, infos.length);
    assertEquals("Name", infos[0].getName());
    assertEquals("Definition", infos[1].getName());
    assertEquals("Hop Type", infos[2].getName());
    assertEquals("Length", infos[3].getName());
    assertEquals("Precision", infos[4].getName());
    assertEquals("Comments", infos[5].getName());
  }

  @Test
  void loadColumnDefinitionsFromMetaData() throws Exception {
    Database db = mock(Database.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);

    when(db.getDatabaseMetaData()).thenReturn(metaData);
    when(metaData.getColumns(any(), any(), eq("orders"), any())).thenReturn(rs);
    when(rs.getMetaData()).thenReturn(rsmd);
    when(rsmd.getColumnCount()).thenReturn(10);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("COLUMN_NAME")).thenReturn("customer_name");
    when(rs.getInt("DATA_TYPE")).thenReturn(java.sql.Types.VARCHAR);
    when(rs.getString("TYPE_NAME")).thenReturn("varchar");
    when(rs.getInt("COLUMN_SIZE")).thenReturn(100);

    Map<String, String> defs =
        DatabaseTableInfoTab.loadColumnDefinitions(db, "public", "orders", null);
    assertEquals("varchar(100)", defs.get("customer_name"));
  }

  @Test
  void loadColumnDefinitionsFallsBackToFields() {
    IRowMeta rowMeta = new RowMeta();
    IValueMeta vm = new ValueMetaString("notes");
    vm.setLength(255);
    vm.setOriginalColumnTypeName("varchar");
    rowMeta.addValueMeta(vm);

    Map<String, String> defs =
        DatabaseTableInfoTab.loadColumnDefinitions(null, "public", "orders", rowMeta);
    assertEquals("varchar(255)", defs.get("notes"));
  }

  @Test
  void escapePatternEscapesSpecialCharacters() {
    assertEquals("order\\_items", DatabaseTableInfoTab.escapePattern("order_items", "\\"));
    assertEquals("sales\\%total", DatabaseTableInfoTab.escapePattern("sales%total", "\\"));
    assertEquals("path\\\\test", DatabaseTableInfoTab.escapePattern("path\\test", "\\"));
    assertEquals("plain_text", DatabaseTableInfoTab.escapePattern("plain_text", null));
    assertEquals("plain_text", DatabaseTableInfoTab.escapePattern("plain_text", ""));
  }

  @Test
  void loadColumnDefinitionsUsesCatalogWhenSchemasNotSupported() throws Exception {
    Database db = mock(Database.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);

    when(db.getDatabaseMetaData()).thenReturn(metaData);
    when(metaData.supportsCatalogsInTableDefinitions()).thenReturn(true);
    when(metaData.supportsSchemasInTableDefinitions()).thenReturn(false);
    when(metaData.getSearchStringEscape()).thenReturn("\\");
    when(metaData.getColumns(eq("otherdb"), eq(null), eq("orders"), any())).thenReturn(rs);
    when(rs.getMetaData()).thenReturn(rsmd);
    when(rsmd.getColumnCount()).thenReturn(10);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("COLUMN_NAME")).thenReturn("id");
    when(rs.getInt("DATA_TYPE")).thenReturn(java.sql.Types.INTEGER);
    when(rs.getString("TYPE_NAME")).thenReturn("int");

    Map<String, String> defs =
        DatabaseTableInfoTab.loadColumnDefinitions(db, "otherdb", "orders", null);
    assertEquals("int", defs.get("id"));
  }

  @Test
  void loadColumnDefinitionsEscapesWildcardCharactersInTableNameAndSchema() throws Exception {
    Database db = mock(Database.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);

    when(db.getDatabaseMetaData()).thenReturn(metaData);
    when(metaData.supportsCatalogsInTableDefinitions()).thenReturn(false);
    when(metaData.supportsSchemasInTableDefinitions()).thenReturn(true);
    when(metaData.getSearchStringEscape()).thenReturn("\\");
    when(metaData.getColumns(any(), eq("sales\\_schema"), eq("order\\_items\\%2024"), any()))
        .thenReturn(rs);
    when(rs.getMetaData()).thenReturn(rsmd);
    when(rsmd.getColumnCount()).thenReturn(10);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("TABLE_NAME")).thenReturn("order_items%2024");
    when(rs.getString("TABLE_SCHEM")).thenReturn("sales_schema");
    when(rs.getString("COLUMN_NAME")).thenReturn("item_id");
    when(rs.getInt("DATA_TYPE")).thenReturn(java.sql.Types.INTEGER);
    when(rs.getString("TYPE_NAME")).thenReturn("int");

    Map<String, String> defs =
        DatabaseTableInfoTab.loadColumnDefinitions(db, "sales_schema", "order_items%2024", null);
    assertEquals("int", defs.get("item_id"));
  }

  @Test
  void loadColumnDefinitionsFiltersMismatchedTablesAndSchemas() throws Exception {
    Database db = mock(Database.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);

    when(db.getDatabaseMetaData()).thenReturn(metaData);
    when(metaData.supportsCatalogsInTableDefinitions()).thenReturn(false);
    when(metaData.supportsSchemasInTableDefinitions()).thenReturn(true);
    when(metaData.getSearchStringEscape()).thenReturn("\\");
    when(metaData.getColumns(any(), eq("sales"), eq("orders"), any())).thenReturn(rs);
    when(rs.getMetaData()).thenReturn(rsmd);
    when(rsmd.getColumnCount()).thenReturn(10);
    AtomicInteger row = new AtomicInteger(0);
    when(rs.next()).thenAnswer(inv -> row.incrementAndGet() <= 3);
    when(rs.getString("TABLE_NAME"))
        .thenAnswer(
            inv -> {
              switch (row.get()) {
                case 1:
                  return "orders_extra";
                case 2:
                case 3:
                  return "orders";
                default:
                  return null;
              }
            });
    when(rs.getString("TABLE_SCHEM"))
        .thenAnswer(
            inv -> {
              switch (row.get()) {
                case 1:
                case 3:
                  return "sales";
                case 2:
                  return "other_schema";
                default:
                  return null;
              }
            });
    when(rs.getString("COLUMN_NAME"))
        .thenAnswer(
            inv -> {
              switch (row.get()) {
                case 1:
                  return "extra_col";
                case 2:
                  return "wrong_col";
                case 3:
                  return "valid_col";
                default:
                  return null;
              }
            });
    when(rs.getInt("DATA_TYPE")).thenReturn(java.sql.Types.VARCHAR);
    when(rs.getString("TYPE_NAME")).thenReturn("varchar");
    when(rs.getInt("COLUMN_SIZE")).thenReturn(50);

    Map<String, String> defs =
        DatabaseTableInfoTab.loadColumnDefinitions(db, "sales", "orders", null);
    assertEquals(1, defs.size());
    assertEquals("varchar(50)", defs.get("valid_col"));
    assertFalse(defs.containsKey("extra_col"));
    assertFalse(defs.containsKey("wrong_col"));
  }
}
