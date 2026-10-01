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

import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A table column declared NUMERIC, DECIMAL or NUMBER reads as the same exact number before and
 * after its query runs, whatever the rows hold. Against the real driver, because the driver is what
 * types a column from its data. Issue #3633.
 */
class SqliteNumericValuesTest {

  private static final String[] DECLARED_TYPES = {
    "NUMERIC", "NUMERIC(20)", "NUMERIC (20)", "DECIMAL(10,2)", "decimal(10, 2)", "NUMBER"
  };

  /** One of each storage class, as the first row the driver sees. */
  private static final String[] FIRST_VALUES = {
    "NULL", "42", "4200000000", "2.5", "'abc'", "x'00'"
  };

  private Connection connection;
  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
    Class.forName("org.sqlite.JDBC");
  }

  @BeforeEach
  void setUp() throws Exception {
    connection = DriverManager.getConnection("jdbc:sqlite::memory:");
    databaseMeta = meta(new SqliteDatabaseMeta());
  }

  @AfterEach
  void tearDown() throws Exception {
    connection.close();
  }

  @Test
  void aNumericColumnIsTheSameBigNumberBeforeAndAfterItsQueryRuns() throws Exception {
    List<String> wrong = new ArrayList<>();
    for (String declared : DECLARED_TYPES) {
      createTable(declared);
      IValueMeta prepared;
      try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t")) {
        prepared = valueMeta(ps.getMetaData(), 1);
      }
      if (prepared.getType() != IValueMeta.TYPE_BIGNUMBER) {
        wrong.add(declared + " prepared: " + prepared.getTypeDesc());
      }
      for (String value : FIRST_VALUES) {
        insertOnly(value);
        try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t");
            ResultSet rs = ps.executeQuery()) {
          IValueMeta executed = valueMeta(rs.getMetaData(), 1);
          if (executed.getType() != IValueMeta.TYPE_BIGNUMBER) {
            wrong.add(declared + " holding " + value + ": " + executed.getTypeDesc());
          }
        }
      }
    }
    assertTrue(wrong.isEmpty(), String.join("\n", wrong));
  }

  @Test
  void aSizedColumnKeepsItsPrecisionAndScale() throws Exception {
    createTable("DECIMAL(10,2)");
    try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t")) {
      IValueMeta valueMeta = valueMeta(ps.getMetaData(), 1);
      assertEquals(10, valueMeta.getLength());
      assertEquals(2, valueMeta.getPrecision());
    }
  }

  @Test
  void valuesAreReadExactly() throws Exception {
    createTable("NUMERIC");
    assertEquals(BigDecimal.valueOf(Long.MAX_VALUE), readOnly(Long.toString(Long.MAX_VALUE)));
    // A double rounds this to 2^53 + 4.
    long pastTheMantissa = (1L << 53) + 3;
    assertEquals(BigDecimal.valueOf(pastTheMantissa), readOnly(Long.toString(pastTheMantissa)));
    assertEquals(BigDecimal.valueOf(-7), readOnly("-7"));
    assertEquals(new BigDecimal("2.5"), readOnly("2.5"));
    assertEquals(new BigDecimal("0.1"), readOnly("0.1"));
    // Too large for an integer, so SQLite keeps it a real. It must not come back with a negative
    // scale, which prints as 1E+20.
    BigDecimal large = (BigDecimal) readOnly("1e20");
    assertEquals("100000000000000000000", large.toString());
    assertNull(readOnly("NULL"));
  }

  @Test
  void textThatIsNotANumberIsAnErrorRatherThanZero() throws Exception {
    createTable("NUMERIC");
    HopDatabaseException error = assertThrows(HopDatabaseException.class, () -> readOnly("'abc'"));
    String message = messages(error);
    assertTrue(message.contains("'abc'"), message);
    assertTrue(message.contains("CAST(v AS TEXT)"), message);
  }

  /** The case from the issue: ids in NUMERIC columns, walked with a recursive query. */
  @Test
  void theIssueQueryReadsExactIdsOnBothPaths() throws Exception {
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE TABLE producttype (export_source TEXT, id NUMERIC, parentId NUMERIC)");
      st.execute(
          "INSERT INTO producttype VALUES ('ABCD', 100002348, NULL), ('ABCD', 100002344,"
              + " 100002348)");
    }
    String sql =
        "WITH RECURSIVE ChildNodes AS ("
            + " SELECT id, parentId FROM producttype WHERE export_source = ? AND id = ?"
            + " UNION ALL"
            + " SELECT t.id, t.parentId FROM producttype t JOIN ChildNodes c ON t.parentId = c.id)"
            + " SELECT * FROM ChildNodes";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      for (int i = 1; i <= 2; i++) {
        assertEquals(IValueMeta.TYPE_BIGNUMBER, valueMeta(ps.getMetaData(), i).getType());
      }
      ps.setString(1, "ABCD");
      ps.setLong(2, 100002348L);
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData rm = rs.getMetaData();
        IValueMeta id = valueMeta(rm, 1);
        IValueMeta parentId = valueMeta(rm, 2);
        assertEquals(IValueMeta.TYPE_BIGNUMBER, id.getType());
        assertEquals(IValueMeta.TYPE_BIGNUMBER, parentId.getType());

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(100002348L), read(rs, id, 0));
        assertNull(read(rs, parentId, 1));
        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(100002344L), read(rs, id, 0));
        assertEquals(BigDecimal.valueOf(100002348L), read(rs, parentId, 1));
      }
    }
  }

  /** An expression has no declared type, only its data, so it keeps what the driver says. */
  @Test
  void anExpressionIsLeftAsTheDriverTypedIt() throws Exception {
    createTable("NUMERIC");
    insertOnly("42");
    try (PreparedStatement ps =
            connection.prepareStatement(
                "SELECT v + 1 AS plus_one, CAST(v AS NUMERIC) AS c FROM t");
        ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData rm = rs.getMetaData();
      for (int i = 1; i <= rm.getColumnCount(); i++) {
        assertEquals(IValueMeta.TYPE_INTEGER, valueMeta(rm, i).getType(), rm.getColumnName(i));
      }
    }
  }

  /** Only the exact numeric names: other declared types map as they always did. */
  @Test
  void otherDeclaredTypesAreUnchanged() throws Exception {
    createTable("INTEGER");
    try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t")) {
      assertEquals(IValueMeta.TYPE_INTEGER, valueMeta(ps.getMetaData(), 1).getType());
    }
    createTable("REAL");
    try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t")) {
      assertEquals(IValueMeta.TYPE_NUMBER, valueMeta(ps.getMetaData(), 1).getType());
    }
    createTable("TEXT");
    try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t")) {
      assertEquals(IValueMeta.TYPE_STRING, valueMeta(ps.getMetaData(), 1).getType());
    }
  }

  private void createTable(String declared) throws Exception {
    try (Statement st = connection.createStatement()) {
      st.execute("DROP TABLE IF EXISTS t");
      st.execute("CREATE TABLE t (v " + declared + ")");
    }
  }

  private void insertOnly(String value) throws Exception {
    try (Statement st = connection.createStatement()) {
      st.execute("DELETE FROM t");
      st.execute("INSERT INTO t VALUES (" + value + ")");
    }
  }

  /** Stores one value and reads it back the way Hop reads a row. */
  private Object readOnly(String value) throws Exception {
    insertOnly(value);
    try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t");
        ResultSet rs = ps.executeQuery()) {
      IValueMeta valueMeta = valueMeta(rs.getMetaData(), 1);
      assertTrue(rs.next());
      return read(rs, valueMeta, 0);
    }
  }

  private Object read(ResultSet rs, IValueMeta valueMeta, int index) throws Exception {
    return databaseMeta.getIDatabase().getValueFromResultSet(rs, valueMeta, index);
  }

  private IValueMeta valueMeta(ResultSetMetaData rm, int index) throws Exception {
    return DatabaseTypeMapper.getValueMeta(
        new Variables(), databaseMeta, DatabaseColumn.of(rm, index), false, false);
  }

  private static String messages(Throwable error) {
    StringBuilder text = new StringBuilder();
    for (Throwable t = error; t != null; t = t.getCause()) {
      text.append(t.getMessage()).append('\n');
    }
    return text.toString();
  }
}
