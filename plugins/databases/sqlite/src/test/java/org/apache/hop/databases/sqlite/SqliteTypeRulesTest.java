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

package org.apache.hop.databases.sqlite;

import static org.apache.hop.junit.database.TypeRuleFixture.column;
import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The binary handling that used to be an isSqliteVariant() branch in core, and the date handling of
 * issue #3910.
 */
class SqliteTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void binaryColumnsAreReadAsStringsBecauseTypingIsDynamic() throws Exception {
    for (int sqlType : new int[] {Types.BINARY, Types.BLOB, Types.VARBINARY, Types.LONGVARBINARY}) {
      IValueMeta valueMeta =
          DatabaseTypeMapper.getValueMeta(
              new Variables(),
              meta(new SqliteDatabaseMeta()),
              column(sqlType, "BLOB", 0, 0),
              false,
              false);
      assertTrue(valueMeta.isString(), "SQL type " + sqlType + " should be read as a string");
    }
  }

  /**
   * The driver answers NUMERIC when the row it typed an expression column from was null. Reading
   * that as a number is what turned STRFTIME('%Y-%m-%d', ...) into 2024. Issue #3910.
   */
  @Test
  void anExpressionTheDriverCouldNotTypeIsReadAsAString() throws Exception {
    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(
            new Variables(),
            meta(new SqliteDatabaseMeta()),
            column(Types.NUMERIC, "NUMERIC", 0, 0),
            false,
            false);

    assertTrue(valueMeta.isString());
  }

  /** A column of a table declared NUMERIC is a column the user asked to be numeric. */
  @Test
  void aTableColumnDeclaredNumericStaysNumeric() throws Exception {
    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(
            new Variables(),
            meta(new SqliteDatabaseMeta()),
            columnOfTable(Types.NUMERIC, "NUMERIC"),
            false,
            false);

    assertTrue(valueMeta.isNumeric(), "expected a number, got " + valueMeta.getTypeDesc());
  }

  /** A column the driver did type keeps that type, whatever its declared name says. */
  @Test
  void aColumnTheDriverDidTypeKeepsThatType() throws Exception {
    IValueMeta integer =
        DatabaseTypeMapper.getValueMeta(
            new Variables(),
            meta(new SqliteDatabaseMeta()),
            column(Types.INTEGER, "NUMERIC", 0, 0),
            false,
            false);
    assertEquals(IValueMeta.TYPE_INTEGER, integer.getType());

    IValueMeta decimal =
        DatabaseTypeMapper.getValueMeta(
            new Variables(),
            meta(new SqliteDatabaseMeta()),
            column(Types.DECIMAL, "DECIMAL", 10, 10),
            false,
            false);
    assertTrue(decimal.isNumeric());
  }

  /** Dates cross JDBC through {@link SqliteDateValues}, not through the driver's own parsing. */
  @Test
  void datesAndTimestampsAreBound() {
    SqliteDatabaseMeta dialect = new SqliteDatabaseMeta();

    assertNotNull(DatabaseTypeMapper.getBinding(dialect, new ValueMetaDate("d")));
    assertNotNull(DatabaseTypeMapper.getBinding(dialect, new ValueMetaTimestamp("ts")));
    assertNull(DatabaseTypeMapper.getBinding(dialect, new ValueMetaString("s")));
  }

  /** A column of a table, which is what a driver reports a table name for. */
  private static DatabaseColumn columnOfTable(int sqlType, String nativeTypeName)
      throws SQLException {
    ResultSetMetaData rm = mock(ResultSetMetaData.class);
    when(rm.getColumnName(1)).thenReturn("COL");
    when(rm.getColumnLabel(1)).thenReturn("COL");
    when(rm.getColumnType(1)).thenReturn(sqlType);
    when(rm.getColumnTypeName(1)).thenReturn(nativeTypeName);
    when(rm.getTableName(1)).thenReturn("SOME_TABLE");
    return DatabaseColumn.of(rm, 1);
  }
}
