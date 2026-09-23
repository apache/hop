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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A table column reads as the same Hop type before and after its query runs, whatever the rows
 * hold. Against the real driver, because the driver is what types a column from its data. Issue
 * #3633.
 */
class SqliteDeclaredTypesTest {

  /** Declared types across every affinity, spelled the ways SQLite accepts. */
  private static final String[] DECLARED_TYPES = {
    "NUMERIC",
    "NUMERIC(20)",
    "NUMERIC (20)",
    "DECIMAL(10,2)",
    "decimal(10, 2)",
    "NUMBER",
    "MONEY",
    "BOOLEAN",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "INT",
    "INTEGER",
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "BIGINT",
    "UNSIGNED BIG INT",
    "INT8",
    "REAL",
    "FLOAT",
    "DOUBLE",
    "DOUBLE PRECISION",
    "CHAR(5)",
    "CHARACTER(20)",
    "NCHAR(10)",
    "VARCHAR(20)",
    "NVARCHAR(100)",
    "TEXT",
    "TEXT (50)",
    "CLOB",
    "BINARY",
    "BLOB",
    "JSON",
    "UUID",
    "GEOMETRY",
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

  /**
   * For every name the driver knows, the table is its own answer for a column with no row to look
   * at. The only differences are on the driver's fallback, NUMERIC, which it also answers for names
   * it does not know.
   */
  @Test
  void theTableOnlyDiffersFromTheDriverOnItsFallback() throws Exception {
    for (String declared : DECLARED_TYPES) {
      createTable(declared);
      try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t")) {
        ResultSetMetaData rm = ps.getMetaData();
        int ours = SqliteDeclaredTypes.jdbcType(rm.getColumnTypeName(1));
        if (ours != rm.getColumnType(1)) {
          assertEquals(Types.NUMERIC, rm.getColumnType(1), "declared " + declared);
        }
      }
    }
  }

  /** Numeric affinity stores text as text, so a type Hop does not know is read as a string. */
  @Test
  void aColumnOfAnUnknownTypeKeepsItsText() throws Exception {
    createTable("COLOUR");
    try (Statement st = connection.createStatement()) {
      st.execute("INSERT INTO t VALUES ('red')");
    }
    try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t");
        ResultSet rs = ps.executeQuery()) {
      IValueMeta valueMeta = valueMeta(rs.getMetaData());
      assertTrue(valueMeta.isString(), valueMeta.getTypeDesc());
    }
  }

  @Test
  void theSpaceBeforeASizeIsIgnored() {
    assertEquals(Types.NUMERIC, SqliteDeclaredTypes.jdbcType("NUMERIC (20)"));
    assertEquals(Types.VARCHAR, SqliteDeclaredTypes.jdbcType("TEXT (50)"));
    assertEquals(Types.DECIMAL, SqliteDeclaredTypes.jdbcType("decimal(10, 2)"));
  }

  @Test
  void aTableColumnReadsAsTheSameTypeWhateverItsFirstRowHolds() throws Exception {
    List<String> mismatches = new ArrayList<>();
    for (String declared : DECLARED_TYPES) {
      createTable(declared);
      IValueMeta prepared;
      try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t")) {
        prepared = valueMeta(ps.getMetaData());
      }
      for (String value : FIRST_VALUES) {
        try (Statement st = connection.createStatement()) {
          st.execute("DELETE FROM t");
          st.execute("INSERT INTO t VALUES (" + value + ")");
        }
        try (PreparedStatement ps = connection.prepareStatement("SELECT v FROM t");
            ResultSet rs = ps.executeQuery()) {
          IValueMeta executed = valueMeta(rs.getMetaData());
          if (executed.getType() != prepared.getType()) {
            mismatches.add(
                declared
                    + " holding "
                    + value
                    + ": "
                    + prepared.getTypeDesc()
                    + " before the query ran, "
                    + executed.getTypeDesc()
                    + " after");
          }
        }
      }
    }
    assertTrue(mismatches.isEmpty(), String.join("\n", mismatches));
  }

  /** The case from the issue: a column declared NUMERIC holding integers, read through a CTE. */
  @Test
  void aNumericColumnHoldingIntegersIsANumberThroughARecursiveQuery() throws Exception {
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
      ps.setString(1, "ABCD");
      ps.setLong(2, 100002348L);
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData rm = rs.getMetaData();
        // What the driver says here is the bug: INTEGER for id, NUMERIC for the null parentId.
        assertEquals(Types.INTEGER, rm.getColumnType(1));
        assertEquals(Types.NUMERIC, rm.getColumnType(2));

        for (int i = 1; i <= rm.getColumnCount(); i++) {
          IValueMeta valueMeta =
              DatabaseTypeMapper.getValueMeta(
                  new Variables(), databaseMeta, DatabaseColumn.of(rm, i), false, false);
          assertEquals(IValueMeta.TYPE_NUMBER, valueMeta.getType(), rm.getColumnName(i));
        }
      }
    }
  }

  /**
   * An expression has no declared type, only its data, so it keeps what the driver says. CAST gives
   * it a type name but no table.
   */
  @Test
  void anExpressionIsLeftAsTheDriverTypedIt() throws Exception {
    createTable("NUMERIC");
    try (Statement st = connection.createStatement()) {
      st.execute("INSERT INTO t VALUES (42)");
    }
    try (PreparedStatement ps =
            connection.prepareStatement(
                "SELECT v + 1 AS plus_one, CAST(v AS NUMERIC) AS c FROM t");
        ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData rm = rs.getMetaData();
      for (int i = 1; i <= rm.getColumnCount(); i++) {
        IValueMeta valueMeta =
            DatabaseTypeMapper.getValueMeta(
                new Variables(), databaseMeta, DatabaseColumn.of(rm, i), false, false);
        assertEquals(IValueMeta.TYPE_INTEGER, valueMeta.getType(), rm.getColumnName(i));
      }
    }
  }

  private void createTable(String declared) throws Exception {
    try (Statement st = connection.createStatement()) {
      st.execute("DROP TABLE IF EXISTS t");
      st.execute("CREATE TABLE t (v " + declared + ")");
    }
  }

  private IValueMeta valueMeta(ResultSetMetaData rm) throws Exception {
    return DatabaseTypeMapper.getValueMeta(
        new Variables(), databaseMeta, DatabaseColumn.of(rm, 1), false, false);
  }
}
