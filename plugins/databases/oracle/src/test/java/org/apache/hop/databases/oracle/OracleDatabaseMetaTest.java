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

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.sql.ResultSet;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
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
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

class OracleDatabaseMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private final String sequenceName = "sequence_name";

  private OracleDatabaseMeta nativeMeta;
  private IVariables variables;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
    HopLogStore.init();
  }

  @BeforeEach
  void setupOnce() throws Exception {
    nativeMeta = new OracleDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    nativeMeta.addDefaultOptions();
    HopClientEnvironment.init();
    variables = Mockito.spy(new Variables());
  }

  @Test
  void testOverriddenSettings() throws Exception {
    // Tests the settings of the Oracle Database Meta
    // according to the features of the DB as we know them

    assertEquals(1521, nativeMeta.getDefaultDatabasePort());
    assertFalse(nativeMeta.isSupportsAutoInc());
    assertEquals("oracle.jdbc.driver.OracleDriver", nativeMeta.getDriverClass());
    assertEquals("jdbc:oracle:thin:@FOO:1024:BAR", nativeMeta.getURL("FOO", "1024", "BAR"));
    assertEquals("jdbc:oracle:thin:@FOO:11:BAR", nativeMeta.getURL("FOO", "11", ":BAR"));
    assertEquals("jdbc:oracle:thin:@BAR:65534/FOO", nativeMeta.getURL("BAR", "65534", "/FOO"));
    assertEquals("jdbc:oracle:thin:@FOO", nativeMeta.getURL("", "", "FOO"));
    assertEquals("jdbc:oracle:thin:@FOO", nativeMeta.getURL(null, "-1", "FOO"));
    assertEquals("jdbc:oracle:thin:@FOO", nativeMeta.getURL(null, null, "FOO"));
    assertEquals("jdbc:oracle:thin:@FOO:1234:BAR", nativeMeta.getURL("FOO", "1234", "BAR"));
    assertEquals(
        "jdbc:oracle:thin:@", nativeMeta.getURL("", "", "")); // Pretty sure this is a bug...
    assertFalse(nativeMeta.isSupportsOptionsInURL());
    assertTrue(nativeMeta.isSupportsSequences());
    assertTrue(nativeMeta.isSupportsSequenceNoMaxValueOption());
    assertTrue(nativeMeta.useSchemaNameForTableList());
    assertTrue(nativeMeta.isSupportsSynonyms());
    String[] reservedWords =
        new String[] {
          "ACCESS",
          "ADD",
          "ALL",
          "ALTER",
          "AND",
          "ANY",
          "ARRAYLEN",
          "AS",
          "ASC",
          "AUDIT",
          "BETWEEN",
          "BY",
          "CHAR",
          "CHECK",
          "CLUSTER",
          "COLUMN",
          "COMMENT",
          "COMPRESS",
          "CONNECT",
          "CREATE",
          "CURRENT",
          "DATE",
          "DECIMAL",
          "DEFAULT",
          "DELETE",
          "DESC",
          "DISTINCT",
          "DROP",
          "ELSE",
          "EXCLUSIVE",
          "EXISTS",
          "FILE",
          "FLOAT",
          "FOR",
          "FROM",
          "GRANT",
          "GROUP",
          "HAVING",
          "IDENTIFIED",
          "IMMEDIATE",
          "IN",
          "INCREMENT",
          "INDEX",
          "INITIAL",
          "INSERT",
          "INTEGER",
          "INTERSECT",
          "INTO",
          "IS",
          "LEVEL",
          "LIKE",
          "LOCK",
          "LONG",
          "MAXEXTENTS",
          "MINUS",
          "MODE",
          "MODIFY",
          "NOAUDIT",
          "NOCOMPRESS",
          "NOT",
          "NOTFOUND",
          "NOWAIT",
          "NULL",
          "NUMBER",
          "OF",
          "OFFLINE",
          "ON",
          "ONLINE",
          "OPTION",
          "OR",
          "ORDER",
          "PCTFREE",
          "PRIOR",
          "PRIVILEGES",
          "PUBLIC",
          "RAW",
          "RENAME",
          "RESOURCE",
          "REVOKE",
          "ROW",
          "ROWID",
          "ROWLABEL",
          "ROWNUM",
          "ROWS",
          "SELECT",
          "SESSION",
          "SET",
          "SHARE",
          "SIZE",
          "SMALLINT",
          "SQLBUF",
          "START",
          "SUCCESSFUL",
          "SYNONYM",
          "SYSDATE",
          "TABLE",
          "THEN",
          "TO",
          "TRIGGER",
          "UID",
          "UNION",
          "UNIQUE",
          "UPDATE",
          "USER",
          "VALIDATE",
          "VALUES",
          "VARCHAR",
          "VARCHAR2",
          "VIEW",
          "WHENEVER",
          "WHERE",
          "WITH"
        };
    assertArrayEquals(reservedWords, nativeMeta.getReservedWords());
    assertEquals(
        "http://download.oracle.com/docs/cd/B19306_01/java.102/b14355/urls.htm#i1006362",
        nativeMeta.getExtraOptionsHelpText());
    assertTrue(nativeMeta.isRequiresCreateTablePrimaryKeyAppend());
    assertTrue(nativeMeta.isSupportsPreparedStatementMetadataRetrieval());
    String quoteTest1 = "FOO 'BAR' \r TEST \n";
    String quoteTest2 = "FOO 'BAR' \\r TEST \\n";
    assertEquals(
        "'FOO ''BAR'' '||chr(10)||' TEST '||chr(13)||''", nativeMeta.quoteSqlString(quoteTest1));
    assertEquals("'FOO ''BAR'' \\r TEST \\n'", nativeMeta.quoteSqlString(quoteTest2));
    assertFalse(nativeMeta.isReleaseSavepoint());
    Variables v = new Variables();
    v.setVariable("FOOVARIABLE", "FOOVALUE");

    DatabaseMeta dm = new DatabaseMeta();
    dm.setIDatabase(nativeMeta);
    assertEquals("TABLESPACE FOOVALUE", nativeMeta.getTablespaceDDL(v, dm, "${FOOVARIABLE}"));
    assertEquals("", nativeMeta.getTablespaceDDL(v, dm, ""));
    assertFalse(nativeMeta.IsSupportsErrorHandlingOnBatchUpdates());
    assertEquals(2000, nativeMeta.getMaxVARCHARLength());
    assertTrue(nativeMeta.isSupportsTimestampDataType());
    assertEquals(32, nativeMeta.getMaxColumnsInIndex());
  }

  @Test
  void testOverriddenSqlStatements() {
    assertEquals(" WHERE ROWNUM <= 5", nativeMeta.getLimitClause(5));
    String reusedFieldsQuery = "SELECT * FROM FOO WHERE 1=0";
    assertEquals(reusedFieldsQuery, nativeMeta.getSqlQueryFields("FOO"));
    assertEquals(reusedFieldsQuery, nativeMeta.getSqlTableExists("FOO"));
    String reusedColumnsQuery = "SELECT FOO FROM BAR WHERE 1=0";
    assertEquals(reusedColumnsQuery, nativeMeta.getSqlQueryColumnFields("FOO", "BAR"));
    assertEquals(reusedColumnsQuery, nativeMeta.getSqlColumnExists("FOO", "BAR"));
    assertEquals(
        "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'FOO'",
        nativeMeta.getSqlSequenceExists("FOO"));
    assertEquals(
        "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'FOO'",
        nativeMeta.getSqlSequenceExists("foo"));

    assertEquals(
        "SELECT * FROM ALL_SEQUENCES WHERE SEQUENCE_NAME = 'BAR' AND SEQUENCE_OWNER = 'FOO'",
        nativeMeta.getSqlSequenceExists("FOO.BAR"));
    assertEquals(
        "SELECT * FROM ALL_SEQUENCES WHERE SEQUENCE_NAME = 'BAR' AND SEQUENCE_OWNER = 'FOO'",
        nativeMeta.getSqlSequenceExists("foo.bar"));

    assertEquals("SELECT FOO.currval FROM DUAL", nativeMeta.getSqlCurrentSequenceValue("FOO"));
    assertEquals("SELECT FOO.nextval FROM DUAL", nativeMeta.getSqlNextSequenceValue("FOO"));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO TIMESTAMP ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaTimestamp("FOO"), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO DATE ) ",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaDate("FOO"), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO VARCHAR2(15) ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("FOO", 15, 0), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO INTEGER ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("FOO", 15, 0), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO NUMBER(15, 10) ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("FOO", 15, 10), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO NUMBER(15, 10) ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("FOO", 15, 10), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO BLOB ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBinary("FOO", 2048, 0), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO CHAR(1) ) ",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaBoolean("FOO"), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO  UNKNOWN ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInternetAddress("FOO"), "", false, "", false));

    String lineSep = System.getProperty("line.separator");
    assertEquals(
        "ALTER TABLE FOO DROP ( BAR ) " + lineSep,
        nativeMeta.getDropColumnStatement("FOO", new ValueMetaString("BAR"), "", false, "", false));
    String modColStmtExpected =
        "ALTER TABLE FOO ADD ( BAR_KTL VARCHAR2(2000) ) ;"
            + lineSep
            + "UPDATE FOO SET BAR_KTL=BAR;"
            + lineSep
            + "ALTER TABLE FOO DROP ( BAR ) "
            + lineSep
            + ";"
            + lineSep
            + "ALTER TABLE FOO ADD ( BAR VARCHAR2(2000) ) ;"
            + lineSep
            + "UPDATE FOO SET BAR=BAR_KTL;"
            + lineSep
            + "ALTER TABLE FOO DROP ( BAR_KTL ) "
            + lineSep;
    assertEquals(
        modColStmtExpected,
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR"), "", false, "", false));
    modColStmtExpected =
        "ALTER TABLE \"FOO\" ADD ( BAR_KTL VARCHAR2(2000) ) ;"
            + lineSep
            + "UPDATE \"FOO\" SET BAR_KTL=BAR;"
            + lineSep
            + "ALTER TABLE \"FOO\" DROP ( BAR ) "
            + lineSep
            + ";"
            + lineSep
            + "ALTER TABLE \"FOO\" ADD ( BAR VARCHAR2(2000) ) ;"
            + lineSep
            + "UPDATE \"FOO\" SET BAR=BAR_KTL;"
            + lineSep
            + "ALTER TABLE \"FOO\" DROP ( BAR_KTL ) "
            + lineSep;
    assertEquals(
        modColStmtExpected,
        nativeMeta.getModifyColumnStatement(
            "\"FOO\"", new ValueMetaString("BAR"), "", false, "", false));

    modColStmtExpected =
        "ALTER TABLE FOO ADD ( A12345678901234567890123456789_KTL VARCHAR2(2000) ) ;"
            + lineSep
            + "UPDATE FOO SET A12345678901234567890123456789_KTL=A1234567890123456789012345678901234567890;"
            + lineSep
            + "ALTER TABLE FOO DROP ( A1234567890123456789012345678901234567890 ) "
            + lineSep
            + ";"
            + lineSep
            + "ALTER TABLE FOO ADD ( A1234567890123456789012345678901234567890 VARCHAR2(2000) ) ;"
            + lineSep
            + "UPDATE FOO SET A1234567890123456789012345678901234567890=A12345678901234567890123456789_KTL;"
            + lineSep
            + "ALTER TABLE FOO DROP ( A12345678901234567890123456789_KTL ) "
            + lineSep;
    assertEquals(
        modColStmtExpected,
        nativeMeta.getModifyColumnStatement(
            "FOO",
            new ValueMetaString("A1234567890123456789012345678901234567890"),
            "",
            false,
            "",
            false));

    String expectedProcSql =
        "SELECT OWNER||'.'||OBJECT_NAME||NVL2(PROCEDURE_NAME,'.'||PROCEDURE_NAME,NULL) FROM ALL_PROCEDURES ORDER BY 1";

    assertEquals(expectedProcSql, nativeMeta.getSqlListOfProcedures());

    String expectedLockOneItem = "LOCK TABLE FOO IN EXCLUSIVE MODE;" + lineSep;
    assertEquals(expectedLockOneItem, nativeMeta.getSqlLockTables(new String[] {"FOO"}));
    String expectedLockMultiItem =
        "LOCK TABLE FOO IN EXCLUSIVE MODE;"
            + lineSep
            + "LOCK TABLE BAR IN EXCLUSIVE MODE;"
            + lineSep;
    assertEquals(expectedLockMultiItem, nativeMeta.getSqlLockTables(new String[] {"FOO", "BAR"}));
    assertNull(nativeMeta.getSqlUnlockTables(null)); // Commit unlocks tables
    assertEquals("SELECT SEQUENCE_NAME FROM all_sequences", nativeMeta.getSqlListOfSequences());
    assertEquals(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE FOO'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
        nativeMeta.getDropTableIfExistsStatement("FOO"));
  }

  @Test
  void testGetFieldDefinition() {
    assertEquals(
        "FOO DATE",
        nativeMeta.getFieldDefinition(new ValueMetaDate("FOO"), "", "", false, true, false));
    assertEquals(
        "TIMESTAMP",
        nativeMeta.getFieldDefinition(new ValueMetaTimestamp("FOO"), "", "", false, false, false));

    assertEquals(
        "CHAR(1)",
        nativeMeta.getFieldDefinition(new ValueMetaBoolean("FOO"), "", "", false, false, false));

    assertEquals(
        "NUMBER(5, 3)",
        nativeMeta.getFieldDefinition(
            new ValueMetaNumber("FOO", 5, 3), "", "", false, false, false));
    assertEquals(
        "NUMBER(5)",
        nativeMeta.getFieldDefinition(
            new ValueMetaBigNumber("FOO", 5, 0), "", "", false, false, false));
    assertEquals(
        "INTEGER",
        nativeMeta.getFieldDefinition(
            new ValueMetaInteger("FOO", 17, 0), "", "", false, false, false));

    assertEquals(
        "CLOB",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", DatabaseMeta.CLOB_LENGTH, 0), "", "", false, false, false));
    assertEquals(
        "CHAR(1)",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", 1, 0), "", "", false, false, false));
    assertEquals(
        "VARCHAR2(15)",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", 15, 0), "", "", false, false, false));
    assertEquals(
        "VARCHAR2(2000)",
        nativeMeta.getFieldDefinition(new ValueMetaString("FOO"), "", "", false, false, false));
    assertEquals(
        "VARCHAR2(2000)",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", nativeMeta.getMaxVARCHARLength(), 0),
            "",
            "",
            false,
            false,
            false));
    assertEquals(
        "CLOB",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", nativeMeta.getMaxVARCHARLength() + 1, 0),
            "",
            "",
            false,
            false,
            false));

    assertEquals(
        "BLOB",
        nativeMeta.getFieldDefinition(
            new ValueMetaBinary("FOO", 45, 0), "", "", false, false, false));

    assertEquals(
        " UNKNOWN",
        nativeMeta.getFieldDefinition(
            new ValueMetaInternetAddress("FOO"), "", "", false, false, false));

    assertEquals(
        " UNKNOWN" + System.getProperty("line.separator"),
        nativeMeta.getFieldDefinition(
            new ValueMetaInternetAddress("FOO"), "", "", false, false, true));
  }

  private int rowCnt = 0;
  private String[] row1 = new String[] {"ROW1COL1", "ROW1COL2"};
  private String[] row2 = new String[] {"ROW2COL1", "ROW2COL2"};

  @Test
  void testCheckIndexExists() throws Exception {
    String expectedSql = "SELECT * FROM USER_IND_COLUMNS WHERE TABLE_NAME = 'FOO'";
    Database db = Mockito.mock(Database.class);
    IRowMeta rm = Mockito.mock(IRowMeta.class);
    ResultSet rs = Mockito.mock(ResultSet.class);
    DatabaseMeta dm = Mockito.mock(DatabaseMeta.class);
    Mockito.when(dm.getQuotedSchemaTableCombination(any(IVariables.class), eq(""), eq("FOO")))
        .thenReturn("FOO");
    Mockito.when(rs.next()).thenReturn(rowCnt < 2);
    Mockito.when(db.openQuery(expectedSql)).thenReturn(rs);
    Mockito.when(db.getReturnRowMeta()).thenReturn(rm);
    Mockito.when(rm.getString(row1, "COLUMN_NAME", "")).thenReturn("ROW1COL2");
    Mockito.when(rm.getString(row2, "COLUMN_NAME", "")).thenReturn("ROW2COL2");
    Mockito.when(db.getRow(rs))
        .thenAnswer(
            (Answer<Object[]>)
                invocation -> {
                  rowCnt++;
                  if (rowCnt == 1) {
                    return row1;
                  } else if (rowCnt == 2) {
                    return row2;
                  } else {
                    return null;
                  }
                });
    Mockito.when(db.getDatabaseMeta()).thenReturn(dm);
    assertTrue(nativeMeta.hasIndex(db, "", "FOO", new String[] {"ROW1COL2", "ROW2COL2"}));
    assertFalse(nativeMeta.hasIndex(db, "", "FOO", new String[] {"ROW2COL2", "NOTTHERE"}));
    assertFalse(nativeMeta.hasIndex(db, "", "FOO", new String[] {"NOTTHERE", "ROW1COL2"}));
  }

  @Test
  void testSupportsSavepoint() {
    assertFalse(nativeMeta.isReleaseSavepoint());
  }

  @Test
  void testSupportsSequence() {
    String dbType = nativeMeta.getClass().getSimpleName();
    assertTrue(nativeMeta.isSupportsSequences(), dbType);
    assertFalse(Utils.isEmpty(nativeMeta.getSqlListOfSequences()), dbType + ": List of Sequences");
    assertFalse(
        Utils.isEmpty(nativeMeta.getSqlSequenceExists("testSeq")), dbType + ": Sequence Exists");
    assertFalse(
        Utils.isEmpty(nativeMeta.getSqlCurrentSequenceValue("testSeq")),
        dbType + ": Current Value");
    assertFalse(
        Utils.isEmpty(nativeMeta.getSqlNextSequenceValue("testSeq")), dbType + ": Next Value");

    assertEquals(
        "SELECT sequence_name.nextval FROM DUAL", nativeMeta.getSqlNextSequenceValue(sequenceName));
    assertEquals(
        "SELECT sequence_name.currval FROM DUAL",
        nativeMeta.getSqlCurrentSequenceValue(sequenceName));
  }

  @Test
  void testSupportsTimestampDataTypeIsTrue() {
    nativeMeta.setSupportsTimestampDataType(true);
    assertEquals(
        "TIMESTAMP",
        nativeMeta.getFieldDefinition(new ValueMetaTimestamp("FOO"), "", "", false, false, false));
    assertEquals(
        "ALTER TABLE FOO ADD ( FOO TIMESTAMP ) ",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaTimestamp("FOO"), "", false, "", false));
  }
}
