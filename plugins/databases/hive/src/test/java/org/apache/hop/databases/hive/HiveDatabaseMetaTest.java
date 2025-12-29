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
 *
 */
package org.apache.hop.databases.hive;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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

class HiveDatabaseMetaTest {
  HiveDatabaseMeta nativeMeta;

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
    nativeMeta = new HiveDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    nativeMeta.addDefaultOptions();
  }

  @Test
  void testSettings() {
    assertArrayEquals(new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE}, nativeMeta.getAccessTypeList());
    assertEquals(10000, nativeMeta.getDefaultDatabasePort());
    assertTrue(nativeMeta.isSupportsAutoInc());
    assertEquals(1, nativeMeta.getNotFoundTK(true));
    assertEquals(0, nativeMeta.getNotFoundTK(false));
    assertEquals("org.apache.hive.jdbc.HiveDriver", nativeMeta.getDriverClass());
    assertEquals("jdbc:hive2://FOO:BAR/WIBBLE", nativeMeta.getURL("FOO", "BAR", "WIBBLE"));
    assertEquals(
        "jdbc:hive2://FOO1:BAR1,FOO2:BAR2/WIBBLE",
        nativeMeta.getURL("FOO1,FOO2", "BAR1,BAR2", "WIBBLE"));
    assertEquals("jdbc:hive2://FOO/WIBBLE", nativeMeta.getURL("FOO", "", "WIBBLE"));
    assertEquals("&", nativeMeta.getExtraOptionSeparator());
    assertEquals("?", nativeMeta.getExtraOptionIndicator());
    assertFalse(nativeMeta.isSupportsTransactions());
    assertFalse(nativeMeta.isSupportsBitmapIndex());
    assertTrue(nativeMeta.isSupportsViews());
    assertFalse(nativeMeta.isSupportsSynonyms());
    assertArrayEquals(
        new String[] {
          "ALL",
          "ALTER",
          "AND",
          "ARRAY",
          "AS",
          "AUTHORIZATION",
          "BETWEEN",
          "BIGINT",
          "BINARY",
          "BOOLEAN",
          "BOTH",
          "BY",
          "CACHE",
          "CASE",
          "CAST",
          "CHAR",
          "COLUMN",
          "COMMIT",
          "CONF",
          "CONSTRAINT",
          "CREATE",
          "CROSS",
          "CUBE",
          "CURRENT",
          "CURRENT_DATE",
          "CURRENT_TIMESTAMP",
          "CURSOR",
          "DATABASE",
          "DATE",
          "DAYOFWEEK",
          "DECIMAL",
          "DELETE",
          "DESCRIBE",
          "DISTINCT",
          "DOUBLE",
          "DROP",
          "ELSE",
          "END",
          "EXCHANGE",
          "EXISTS",
          "EXTENDED",
          "EXTERNAL",
          "EXTRACT",
          "FALSE",
          "FETCH",
          "FLOAT",
          "FLOOR",
          "FOLLOWING",
          "FOR",
          "FOREIGN",
          "FROM",
          "FULL",
          "FUNCTION",
          "GRANT",
          "GROUP",
          "GROUPING",
          "HAVING",
          "IF",
          "IMPORT",
          "IN",
          "INNER",
          "INSERT",
          "INT",
          "INTEGER",
          "INTERSECT",
          "INTERVAL",
          "INTO",
          "IS",
          "JOIN",
          "LATERAL",
          "LEFT",
          "LESS",
          "LIKE",
          "LOCAL",
          "MACRO",
          "MAP",
          "MORE",
          "NONE",
          "NOT",
          "NULL",
          "OF",
          "ON",
          "ONLY",
          "OR",
          "ORDER",
          "OUT",
          "OUTER",
          "OVER",
          "PARTIALSCAN",
          "PARTITION",
          "PERCENT",
          "PRECEDING",
          "PRECISION",
          "PRESERVE",
          "PRIMARY",
          "PROCEDURE",
          "RANGE",
          "READS",
          "REDUCE",
          "REFERENCES",
          "REGEXP",
          "REVOKE",
          "RIGHT",
          "RLIKE",
          "ROLLBACK",
          "ROLLUP",
          "ROW",
          "ROWS",
          "SELECT",
          "SET",
          "SMALLINT",
          "START",
          "TABLE",
          "TABLESAMPLE",
          "THEN",
          "TIMESTAMP",
          "TO",
          "TRANSFORM",
          "TRIGGER",
          "TRUE",
          "TRUNCATE",
          "UNBOUNDED",
          "UNION",
          "UNIQUEJOIN",
          "UPDATE",
          "USER",
          "USING",
          "UTC_TMESTAMP",
          "VALUES",
          "VARCHAR",
          "VIEWS",
          "WHEN",
          "WHERE",
          "WINDOW",
          "WITH",
        },
        nativeMeta.getReservedWords());

    assertEquals("`", nativeMeta.getStartQuote());
    assertEquals("`", nativeMeta.getEndQuote());
    assertEquals(
        "https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/integrating-hive/content/hive_connection_string_url_syntax.html",
        nativeMeta.getExtraOptionsHelpText());
    assertFalse(nativeMeta.isReleaseSavepoint());
    assertTrue(nativeMeta.IsSupportsErrorHandlingOnBatchUpdates());
    assertFalse(nativeMeta.isRequiringTransactionsOnQueries());
  }

  @Test
  void testSqlStatements() {
    assertEquals(" LIMIT 15", nativeMeta.getLimitClause(15));
    assertEquals("SELECT * FROM FOO LIMIT 0", nativeMeta.getSqlQueryFields("FOO"));
    assertEquals("SELECT * FROM FOO LIMIT 0", nativeMeta.getSqlTableExists("FOO"));
    assertEquals("SELECT FOO FROM BAR LIMIT 0", nativeMeta.getSqlQueryColumnFields("FOO", "BAR"));

    assertEquals(
        "ALTER TABLE FOO ADD BAR TIMESTAMP",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaDate("BAR"), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR TIMESTAMP",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaTimestamp("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR BOOLEAN",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaBoolean("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DECIMAL(10,16)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR BIGINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 0, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, 3), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DECIMAL(10,3)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 3), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DECIMAL(21,4)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 21, 4), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR STRING",
        nativeMeta.getAddColumnStatement(
            "FOO",
            new ValueMetaString("BAR", nativeMeta.getMaxVARCHARLength() + 2, 0),
            "",
            false,
            "",
            false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR STRING",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO",
            new ValueMetaNumber("BAR", 10, -7),
            "",
            false,
            "",
            false)); // Bug here - invalid SQL

    assertEquals(
        "ALTER TABLE FOO ADD BAR DECIMAL(22,7)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 22, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", -10, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR  UNKNOWN",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInternetAddress("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR"), "BAR", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 26, 8), "BAR", true, "", false));

    String lineSep = System.getProperty("line.separator");
    assertEquals(
        "ALTER TABLE FOO DROP BAR" + lineSep,
        nativeMeta.getDropColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO MODIFY BAR STRING",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO MODIFY BAR STRING",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR"), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ADD BAR SMALLINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 4, 0), "", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR BIGINT NOT NULL PRIMARY KEY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR"), "BAR", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DECIMAL(10,16)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DECIMAL(22,16)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 22, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR STRING",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 1, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR STRING",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 16777250, 0), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR BINARY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBinary("BAR", 16777250, 0), "", false, "", false));

    assertEquals(
        "LOCK TABLES FOO WRITE, BAR WRITE;" + lineSep,
        nativeMeta.getSqlLockTables(new String[] {"FOO", "BAR"}));

    assertEquals("UNLOCK TABLES", nativeMeta.getSqlUnlockTables(new String[] {}));

    assertEquals(
        "insert into FOO(FOOKEY, FOOVERSION) values (1, 1)",
        nativeMeta.getSqlInsertAutoIncUnknownDimensionRow("FOO", "FOOKEY", "FOOVERSION"));
  }

  /**
   * @return
   * @throws Exception
   */
  private ResultSetMetaData getResultSetMetaData() throws Exception {
    ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

    /**
     * Fields setup around the following query:
     *
     * <p>select CUSTOMERNUMBER as NUMBER , CUSTOMERNAME as NAME , CONTACTLASTNAME as LAST_NAME ,
     * CONTACTFIRSTNAME as FIRST_NAME , 'Hive' as DB , 'NoAliasText' from CUSTOMERS ORDER BY
     * CUSTOMERNAME;
     */
    doReturn("NUMBER").when(resultSetMetaData).getColumnLabel(1);
    doReturn("NAME").when(resultSetMetaData).getColumnLabel(2);
    doReturn("LAST_NAME").when(resultSetMetaData).getColumnLabel(3);
    doReturn("FIRST_NAME").when(resultSetMetaData).getColumnLabel(4);
    doReturn("DB").when(resultSetMetaData).getColumnLabel(5);
    doReturn("NoAliasText").when(resultSetMetaData).getColumnLabel(6);

    doReturn("CUSTOMERNUMBER").when(resultSetMetaData).getColumnName(1);
    doReturn("CUSTOMERNAME").when(resultSetMetaData).getColumnName(2);
    doReturn("CONTACTLASTNAME").when(resultSetMetaData).getColumnName(3);
    doReturn("CONTACTFIRSTNAME").when(resultSetMetaData).getColumnName(4);
    doReturn("Hive").when(resultSetMetaData).getColumnName(5);
    doReturn("NoAliasText").when(resultSetMetaData).getColumnName(6);

    return resultSetMetaData;
  }

  /**
   * @return
   * @throws Exception
   */
  private ResultSetMetaData getResultSetMetaDataException() throws Exception {
    ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

    doThrow(new SQLException()).when(resultSetMetaData).getColumnLabel(1);
    doThrow(new SQLException()).when(resultSetMetaData).getColumnName(1);

    return resultSetMetaData;
  }

  @Test
  void testReleaseSavepoint() {
    assertFalse(nativeMeta.isReleaseSavepoint());
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
    meta.setIDatabase(new HiveDatabaseMeta());

    Database db = new Database(log, variables, meta);
    db.setConnection(mockConnection(dbMetaData));
    db.getLookup(ps, false);

    IRowMeta rowMeta = db.getReturnRowMeta();
    assertEquals(1, db.getReturnRowMeta().size());

    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    assertEquals(IValueMeta.TYPE_BINARY, valueMeta.getType());
  }
}
