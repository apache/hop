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
package org.apache.hop.databases.sqlite;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SqliteDatabaseMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private SqliteDatabaseMeta nativeMeta;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setupBefore() {
    nativeMeta = new SqliteDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
  }

  @Test
  void testSettings() {
    assertArrayEquals(new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE}, nativeMeta.getAccessTypeList());
    assertEquals(-1, nativeMeta.getDefaultDatabasePort());
    assertTrue(nativeMeta.isSupportsAutoInc());
    assertEquals(1, nativeMeta.getNotFoundTK(true));
    assertEquals(0, nativeMeta.getNotFoundTK(false));
    assertEquals("org.sqlite.JDBC", nativeMeta.getDriverClass());
    assertEquals("jdbc:sqlite:WIBBLE", nativeMeta.getURL("IGNORED", "IGNORED", "WIBBLE"));
    assertFalse(nativeMeta.isFetchSizeSupported());
    assertFalse(nativeMeta.isSupportsBitmapIndex());
    assertFalse(nativeMeta.isSupportsSynonyms());
    assertFalse(nativeMeta.isSupportsErrorHandling());

    assertEquals("FOO.BAR", nativeMeta.getSchemaTableCombination("FOO", "BAR"));
  }

  @Test
  void testSqlStatements() {
    assertEquals("DELETE FROM FOO", nativeMeta.getTruncateTableStatement("FOO"));
    assertEquals(
        "ALTER TABLE FOO ADD BAR TEXT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO MODIFY BAR TEXT",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", false));
  }

  @Test
  void testGetFieldDefinition() {
    assertEquals(
        "FOO DATETIME",
        nativeMeta.getFieldDefinition(new ValueMetaDate("FOO"), "", "", false, true, false));
    assertEquals(
        "DATETIME",
        nativeMeta.getFieldDefinition(new ValueMetaTimestamp("FOO"), "", "", false, false, false));
    assertEquals(
        "CHAR(1)",
        nativeMeta.getFieldDefinition(new ValueMetaBoolean("FOO"), "", "", false, false, false));

    // PK/TK
    assertEquals(
        "INTEGER PRIMARY KEY AUTOINCREMENT",
        nativeMeta.getFieldDefinition(
            new ValueMetaNumber("FOO", 10, 0), "FOO", "", false, false, false));
    assertEquals(
        "INTEGER PRIMARY KEY AUTOINCREMENT",
        nativeMeta.getFieldDefinition(
            new ValueMetaBigNumber("FOO", 8, 0), "", "FOO", false, false, false));

    // Numeric Types
    assertEquals(
        "NUMERIC",
        nativeMeta.getFieldDefinition(
            new ValueMetaBigNumber("FOO", 8, -6), "", "", false, false, false));
    assertEquals(
        "NUMERIC",
        nativeMeta.getFieldDefinition(
            new ValueMetaBigNumber("FOO", -13, 0), "", "", false, false, false));
    assertEquals(
        "NUMERIC",
        nativeMeta.getFieldDefinition(
            new ValueMetaBigNumber("FOO", 19, 0), "", "", false, false, false));

    assertEquals(
        "INTEGER",
        nativeMeta.getFieldDefinition(
            new ValueMetaBigNumber("FOO", 11, 0), "", "", false, false, false));

    // Strings
    assertEquals(
        "TEXT",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", 50, 0), "", "", false, false, false));

    assertEquals(
        "BLOB",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", DatabaseMeta.CLOB_LENGTH + 1, 0),
            "",
            "",
            false,
            false,
            false));

    // Others
    assertEquals(
        "BLOB",
        nativeMeta.getFieldDefinition(
            new ValueMetaBinary("FOO", 15, 0), "", "", false, false, false));

    assertEquals(
        "UNKNOWN",
        nativeMeta.getFieldDefinition(
            new ValueMetaInternetAddress("FOO"), "", "", false, false, false));

    assertEquals(
        "UNKNOWN" + System.getProperty("line.separator"),
        nativeMeta.getFieldDefinition(
            new ValueMetaInternetAddress("FOO"), "", "", false, false, true));
  }

  /**
   * The SQLite JDBC driver reports columns whose declared type carries a size with a space before
   * the parenthesis (e.g. "TEXT (50)") as {@link java.sql.Types#NUMERIC}, so the generic mapper
   * wrongly turns them into BigNumber/Integer. The Hop type must instead follow SQLite's name-based
   * type affinity. See issue #6472.
   */
  private ResultSetMetaData resultSetMetaWithTypeName(String typeName) throws SQLException {
    ResultSetMetaData rm = mock(ResultSetMetaData.class);
    when(rm.getColumnTypeName(1)).thenReturn(typeName);
    return rm;
  }

  private IValueMeta effective(IValueMeta original, ResultSetMetaData rm) throws SQLException {
    IValueMeta customized = nativeMeta.customizeValueFromSqlType(original, rm, 1);
    return customized != null ? customized : original;
  }

  @Test
  void testTextAffinityStaysStringWhenDriverReportsNumeric() throws SQLException {
    // "TEXT (50)" -> driver says NUMERIC -> generic mapper produced BigNumber(50)
    IValueMeta generic = new ValueMetaBigNumber("TEXT_50", 50, 0);
    assertTrue(effective(generic, resultSetMetaWithTypeName("TEXT")).isString());
  }

  @Test
  void testIntegerAffinityStaysIntegerWhenDriverReportsNumeric() throws SQLException {
    // "INTEGER (20)" -> driver says NUMERIC -> generic mapper produced BigNumber(20)
    IValueMeta generic = new ValueMetaBigNumber("INTEGER20", 20, 0);
    assertTrue(effective(generic, resultSetMetaWithTypeName("INTEGER")).isInteger());
  }

  @Test
  void testRealAffinityStaysNumberWhenDriverReportsNumeric() throws SQLException {
    // "REAL (10)" -> driver says NUMERIC -> generic mapper produced Integer(10)
    IValueMeta generic = new ValueMetaInteger("REAL10", 10, 0);
    assertTrue(effective(generic, resultSetMetaWithTypeName("REAL")).isNumber());
  }

  @Test
  void testNumericAffinityKeepsGenericNumericMapping() throws SQLException {
    // "NUMERIC (20)" is genuine NUMERIC affinity: keep the generic mapping (BigNumber)
    IValueMeta generic = new ValueMetaBigNumber("NUMERIC20", 20, 0);
    assertTrue(effective(generic, resultSetMetaWithTypeName("NUMERIC")).isBigNumber());
  }

  @Test
  void testCorrectlyTypedStringIsNotAltered() throws SQLException {
    // "TEXT" (no size) already maps to String: no correction needed
    IValueMeta generic = new ValueMetaString("TEXT");
    assertTrue(effective(generic, resultSetMetaWithTypeName("TEXT")).isString());
  }

  /**
   * End-to-end reproduction of issue #6472 against the real SQLite JDBC driver, using the exact DDL
   * from the report (sizes written with a space before the parenthesis). Drives the full
   * ValueMetaBase.getValueFromSqlType -> customizeValueFromSqlType chain.
   */
  @Test
  void testGetValueFromSqlTypeFollowsSqliteAffinity() throws Exception {
    Class.forName("org.sqlite.JDBC");
    Map<String, Integer> types = new HashMap<>();
    try (Connection c = DriverManager.getConnection("jdbc:sqlite::memory:");
        Statement st = c.createStatement()) {
      st.execute(
          "CREATE TABLE str_test ("
              + "\"TEXT\" TEXT, TEXT_50 TEXT (50), "
              + "\"INTEGER\" INTEGER, INTEGER20 INTEGER (20), "
              + "\"NUMERIC\" NUMERIC, NUMERIC20 NUMERIC (20), "
              + "\"REAL\" REAL, REAL10 REAL (10), \"BLOB\" BLOB)");
      try (ResultSet rs = st.executeQuery("SELECT * FROM str_test")) {
        ResultSetMetaData rm = rs.getMetaData();
        DatabaseMeta dbMeta = new DatabaseMeta();
        dbMeta.setIDatabase(new SqliteDatabaseMeta());
        IValueMeta valueMetaBase = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_NONE);
        for (int i = 1; i <= rm.getColumnCount(); i++) {
          IValueMeta v =
              valueMetaBase.getValueFromSqlType(
                  new Variables(), dbMeta, rm.getColumnName(i), rm, i, false, false);
          types.put(rm.getColumnName(i), v.getType());
        }
      }
    }

    // Sized columns (the buggy cases) must follow SQLite name-based affinity.
    assertEquals(IValueMeta.TYPE_STRING, types.get("TEXT_50"));
    assertEquals(IValueMeta.TYPE_INTEGER, types.get("INTEGER20"));
    assertEquals(IValueMeta.TYPE_NUMBER, types.get("REAL10"));
    assertEquals(IValueMeta.TYPE_BIGNUMBER, types.get("NUMERIC20"));

    // Unsized columns were already correct and must stay so.
    assertEquals(IValueMeta.TYPE_STRING, types.get("TEXT"));
    assertEquals(IValueMeta.TYPE_INTEGER, types.get("INTEGER"));
    assertEquals(IValueMeta.TYPE_NUMBER, types.get("REAL"));
    assertEquals(IValueMeta.TYPE_STRING, types.get("BLOB"));
  }
}
