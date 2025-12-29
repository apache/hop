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
package org.apache.hop.databases.singlestore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SingleStoreDatabaseMetaTest {
  SingleStoreDatabaseMeta nativeMeta;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setupBefore() {
    nativeMeta = new SingleStoreDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    nativeMeta.addDefaultOptions();
  }

  @Test
  void testSettings() {
    assertArrayEquals(new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE}, nativeMeta.getAccessTypeList());
    assertEquals(3306, nativeMeta.getDefaultDatabasePort());
    assertTrue(nativeMeta.isSupportsAutoInc());
    assertEquals(1, nativeMeta.getNotFoundTK(true));
    assertEquals(0, nativeMeta.getNotFoundTK(false));
    assertEquals("com.singlestore.jdbc.Driver", nativeMeta.getDriverClass());
    assertEquals("jdbc:singlestore://FOO:BAR/WIBBLE", nativeMeta.getURL("FOO", "BAR", "WIBBLE"));
    assertEquals("jdbc:singlestore://FOO:/WIBBLE", nativeMeta.getURL("FOO", "", "WIBBLE"));
    assertEquals("&", nativeMeta.getExtraOptionSeparator());
    assertEquals("?", nativeMeta.getExtraOptionIndicator());
    assertTrue(nativeMeta.isSupportsTransactions());
    assertFalse(nativeMeta.isSupportsBitmapIndex());
    assertTrue(nativeMeta.isSupportsViews());
    assertFalse(nativeMeta.isSupportsSynonyms());
    assertArrayEquals(
        new String[] {
          "ADD",
          "ALL",
          "ALTER",
          "ANALYZE",
          "AND",
          "AS",
          "ASC",
          "ASENSITIVE",
          "BEFORE",
          "BETWEEN",
          "BIGINT",
          "BINARY",
          "BLOB",
          "BOTH",
          "BY",
          "CALL",
          "CASCADE",
          "CASE",
          "CHANGE",
          "CHAR",
          "CHARACTER",
          "CHECK",
          "COLLATE",
          "COLUMN",
          "CONDITION",
          "CONNECTION",
          "CONSTRAINT",
          "CONTINUE",
          "CONVERT",
          "CREATE",
          "CROSS",
          "CURRENT_DATE",
          "CURRENT_TIME",
          "CURRENT_TIMESTAMP",
          "CURRENT_USER",
          "CURSOR",
          "DATABASE",
          "DATABASES",
          "DAY_HOUR",
          "DAY_MICROSECOND",
          "DAY_MINUTE",
          "DAY_SECOND",
          "DEC",
          "DECIMAL",
          "DECLARE",
          "DEFAULT",
          "DELAYED",
          "DELETE",
          "DESC",
          "DESCRIBE",
          "DETERMINISTIC",
          "DISTINCT",
          "DISTINCTROW",
          "DIV",
          "DOUBLE",
          "DROP",
          "DUAL",
          "EACH",
          "ELSE",
          "ELSEIF",
          "ENCLOSED",
          "ESCAPED",
          "EXISTS",
          "EXIT",
          "EXPLAIN",
          "FALSE",
          "FETCH",
          "FLOAT",
          "FOR",
          "FORCE",
          "FOREIGN",
          "FROM",
          "FULLTEXT",
          "GOTO",
          "GRANT",
          "GROUP",
          "HAVING",
          "HIGH_PRIORITY",
          "HOUR_MICROSECOND",
          "HOUR_MINUTE",
          "HOUR_SECOND",
          "IF",
          "IGNORE",
          "IN",
          "INDEX",
          "INFILE",
          "INNER",
          "INOUT",
          "INSENSITIVE",
          "INSERT",
          "INT",
          "INTEGER",
          "INTERVAL",
          "INTO",
          "IS",
          "ITERATE",
          "JOIN",
          "KEY",
          "KEYS",
          "KILL",
          "LEADING",
          "LEAVE",
          "LEFT",
          "LIKE",
          "LIMIT",
          "LINES",
          "LOAD",
          "LOCALTIME",
          "LOCALTIMESTAMP",
          "LOCATE",
          "LOCK",
          "LONG",
          "LONGBLOB",
          "LONGTEXT",
          "LOOP",
          "LOW_PRIORITY",
          "MATCH",
          "MEDIUMBLOB",
          "MEDIUMINT",
          "MEDIUMTEXT",
          "MIDDLEINT",
          "MINUTE_MICROSECOND",
          "MINUTE_SECOND",
          "MOD",
          "MODIFIES",
          "NATURAL",
          "NOT",
          "NO_WRITE_TO_BINLOG",
          "NULL",
          "NUMERIC",
          "ON",
          "OPTIMIZE",
          "OPTION",
          "OPTIONALLY",
          "OR",
          "ORDER",
          "OUT",
          "OUTER",
          "OUTFILE",
          "POSITION",
          "PRECISION",
          "PRIMARY",
          "PROCEDURE",
          "PURGE",
          "READ",
          "READS",
          "REAL",
          "REFERENCES",
          "REGEXP",
          "RENAME",
          "REPEAT",
          "REPLACE",
          "REQUIRE",
          "RESTRICT",
          "RETURN",
          "REVOKE",
          "RIGHT",
          "RLIKE",
          "SCHEMA",
          "SCHEMAS",
          "SECOND_MICROSECOND",
          "SELECT",
          "SENSITIVE",
          "SEPARATOR",
          "SET",
          "SHOW",
          "SMALLINT",
          "SONAME",
          "SPATIAL",
          "SPECIFIC",
          "SQL",
          "SQLEXCEPTION",
          "SQLSTATE",
          "SQLWARNING",
          "SQL_BIG_RESULT",
          "SQL_CALC_FOUND_ROWS",
          "SQL_SMALL_RESULT",
          "SSL",
          "STARTING",
          "STRAIGHT_JOIN",
          "TABLE",
          "TERMINATED",
          "THEN",
          "TINYBLOB",
          "TINYINT",
          "TINYTEXT",
          "TO",
          "TRAILING",
          "TRIGGER",
          "TRUE",
          "UNDO",
          "UNION",
          "UNIQUE",
          "UNLOCK",
          "UNSIGNED",
          "UPDATE",
          "USAGE",
          "USE",
          "USING",
          "UTC_DATE",
          "UTC_TIME",
          "UTC_TIMESTAMP",
          "VALUES",
          "VARBINARY",
          "VARCHAR",
          "VARCHARACTER",
          "VARYING",
          "WHEN",
          "WHERE",
          "WHILE",
          "WITH",
          "WRITE",
          "XOR",
          "YEAR_MONTH",
          "ZEROFILL"
        },
        nativeMeta.getReservedWords());

    assertEquals("`", nativeMeta.getStartQuote());
    assertEquals("`", nativeMeta.getEndQuote());
    assertEquals(
        "https://docs.singlestore.com/cloud/developer-resources/"
            + "connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/",
        nativeMeta.getExtraOptionsHelpText());
    assertTrue(nativeMeta.isSystemTable("sysTest"));
    assertTrue(nativeMeta.isSystemTable("dtproperties"));
    assertFalse(nativeMeta.isSystemTable("SysTest"));
    assertFalse(nativeMeta.isSystemTable("dTproperties"));
    assertFalse(nativeMeta.isSystemTable("Testsys"));
    assertTrue(nativeMeta.isMySqlVariant());
    assertTrue(nativeMeta.isReleaseSavepoint());
    assertTrue(nativeMeta.IsSupportsErrorHandlingOnBatchUpdates());
    assertFalse(nativeMeta.isRequiringTransactionsOnQueries());
  }

  @Test
  void testSqlStatements() {
    assertEquals(" limit 15", nativeMeta.getLimitClause(15));
    assertEquals("SELECT * FROM FOO limit 1", nativeMeta.getSqlQueryFields("FOO"));
    assertEquals("SELECT * FROM FOO limit 1", nativeMeta.getSqlTableExists("FOO"));
    assertEquals("SELECT FOO FROM BAR limit 1", nativeMeta.getSqlQueryColumnFields("FOO", "BAR"));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DATETIME(6)",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaDate("BAR"), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DATETIME(6)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaTimestamp("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BOOLEAN",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaBoolean("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR INT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 0, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR INT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, 3), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 3), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DECIMAL(21, 4)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 21, 4), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR TEXT",
        nativeMeta.getAddColumnStatement(
            "FOO",
            new ValueMetaString("BAR", nativeMeta.getMaxVARCHARLength() + 2, 0),
            "",
            false,
            "",
            false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR VARCHAR(15)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO",
            new ValueMetaNumber("BAR", 10, -7),
            "",
            false,
            "",
            false)); // Bug here - invalid SQL

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DECIMAL(22, 7)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 22, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", -10, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR  UNKNOWN",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInternetAddress("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR"), "BAR", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 26, 8), "BAR", true, "", false));

    assertEquals(
        "ALTER TABLE FOO DROP COLUMN BAR",
        nativeMeta.getDropColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO MODIFY BAR VARCHAR(15)",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO MODIFY BAR TINYTEXT",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR"), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR INT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 4, 0), "", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT NOT NULL PRIMARY KEY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR"), "BAR", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DECIMAL(22)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 22, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR CHAR(1)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 1, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR LONGTEXT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 16777250, 0), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR VARBINARY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBinary("BAR", 16777250, 0), "", false, "", false));

    assertEquals(
        "LOCK TABLES FOO WRITE, BAR WRITE;" + Const.CR,
        nativeMeta.getSqlLockTables(new String[] {"FOO", "BAR"}));

    assertEquals("UNLOCK TABLES", nativeMeta.getSqlUnlockTables(new String[] {}));

    assertEquals(
        "insert into FOO(FOOKEY, FOOVERSION) values (1, 1)",
        nativeMeta.getSqlInsertAutoIncUnknownDimensionRow("FOO", "FOOKEY", "FOOVERSION"));
  }

  @Test
  void testExtraOptions() {
    DatabaseMeta nativeMeta =
        new DatabaseMeta("", "SINGLESTORE", "JDBC", null, "stub:stub", null, null, null);
    Map<String, String> opts = nativeMeta.getExtraOptions();
    assertNotNull(opts);
    assertEquals("500", opts.get("SINGLESTORE.defaultFetchSize"));
  }

  @Test
  void testReleaseSavepoint() {
    assertTrue(nativeMeta.isReleaseSavepoint());
  }

  @Test
  void testSupportsSequence() {
    String dbType = nativeMeta.getClass().getSimpleName();
    assertFalse(nativeMeta.isSupportsSequences(), dbType);
    assertTrue(Utils.isEmpty(nativeMeta.getSqlListOfSequences()));
    assertEquals("", nativeMeta.getSqlSequenceExists("testSeq"));
    assertEquals("", nativeMeta.getSqlNextSequenceValue("testSeq"));
    assertEquals("", nativeMeta.getSqlCurrentSequenceValue("testSeq"));
  }

  private Connection mockConnection(DatabaseMetaData dbMetaData) throws SQLException {
    Connection conn = mock(Connection.class);
    when(conn.getMetaData()).thenReturn(dbMetaData);
    return conn;
  }

  @Test
  void testVarBinaryIsConvertedToStringType() throws Exception {
    ILoggingObject log = mock(ILoggingObject.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    DatabaseMetaData dbMetaData = mock(DatabaseMetaData.class);
    IVariables variables = mock(IVariables.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);

    when(rsMeta.getColumnCount()).thenReturn(1);
    when(rsMeta.getColumnLabel(1)).thenReturn("column");
    when(rsMeta.getColumnName(1)).thenReturn("column");
    when(rsMeta.getColumnType(1)).thenReturn(java.sql.Types.VARBINARY);
    when(rs.getMetaData()).thenReturn(rsMeta);
    when(ps.executeQuery()).thenReturn(rs);

    DatabaseMeta meta = new DatabaseMeta();
    meta.setIDatabase(new SingleStoreDatabaseMeta());

    try (Database db = new Database(log, variables, meta)) {
      db.setConnection(mockConnection(dbMetaData));
      db.getLookup(ps, false);

      IRowMeta rowMeta = db.getReturnRowMeta();
      assertEquals(1, db.getReturnRowMeta().size());

      IValueMeta valueMeta = rowMeta.getValueMeta(0);
      assertEquals(IValueMeta.TYPE_BINARY, valueMeta.getType());
    }
  }
}
