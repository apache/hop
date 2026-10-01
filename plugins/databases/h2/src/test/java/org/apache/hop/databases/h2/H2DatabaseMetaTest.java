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
package org.apache.hop.databases.h2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.SqlScriptStatement;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class H2DatabaseMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  final String sequenceName = "sequence_name";

  H2DatabaseMeta nativeMeta;

  @BeforeEach
  void setupBefore() {
    nativeMeta = new H2DatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    nativeMeta.addDefaultOptions();
  }

  @Test
  void testSettings() {
    assertEquals(8082, nativeMeta.getDefaultDatabasePort());
    assertEquals("org.h2.Driver", nativeMeta.getDriverClass());

    assertEquals("jdbc:h2:WIBBLE", nativeMeta.getURL("", "", "WIBBLE"));
    assertEquals("jdbc:h2:tcp://FOO:BAR/WIBBLE", nativeMeta.getURL("FOO", "BAR", "WIBBLE"));

    assertEquals("jdbc:h2:WIBBLE", nativeMeta.getURL("", "-1", "WIBBLE"));
    assertEquals("jdbc:h2:mem:WIBBLE", nativeMeta.getURL("", "", "mem:WIBBLE"));

    assertEquals(0, nativeMeta.getNotFoundTK(true));
    assertEquals(0, nativeMeta.getNotFoundTK(false));

    assertArrayEquals(
        new String[] {
          "CURRENT_TIMESTAMP",
          "CURRENT_TIME",
          "CURRENT_DATE",
          "CROSS",
          "DISTINCT",
          "EXCEPT",
          "EXISTS",
          "FROM",
          "FOR",
          "FALSE",
          "FULL",
          "GROUP",
          "HAVING",
          "INNER",
          "INTERSECT",
          "IS",
          "JOIN",
          "LIKE",
          "MINUS",
          "NATURAL",
          "NOT",
          "NULL",
          "ON",
          "ORDER",
          "PRIMARY",
          "ROWNUM",
          "SELECT",
          "SYSDATE",
          "SYSTIME",
          "SYSTIMESTAMP",
          "TODAY",
          "TRUE",
          "UNION",
          "WHERE"
        },
        nativeMeta.getReservedWords());

    assertTrue(nativeMeta.isFetchSizeSupported());
    assertEquals("FOO.BAR", nativeMeta.getSchemaTableCombination("FOO", "BAR"));
    assertTrue(nativeMeta.isReleaseSavepoint());
    assertTrue(nativeMeta.isSupportsSequences());
    assertFalse(nativeMeta.isSupportsBitmapIndex());
    assertTrue(nativeMeta.isSupportsAutoInc());
    assertTrue(nativeMeta.isSupportsGetBlob());
    assertFalse(nativeMeta.isSupportsSetCharacterStream());
    assertFalse(nativeMeta.isSupportsPreparedStatementMetadataRetrieval());
  }

  @Test
  void testSqlStatements() {
    assertEquals("TRUNCATE TABLE FOO", nativeMeta.getTruncateTableStatement("FOO"));
    assertEquals("SELECT * FROM FOO", nativeMeta.getSqlQueryFields("FOO"));
    assertEquals("SELECT 1 FROM FOO", nativeMeta.getSqlTableExists("FOO"));

    assertEquals(
        "ALTER TABLE FOO ADD BAR TIMESTAMP",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaDate("BAR"), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR TIMESTAMP",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaTimestamp("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR CHAR(1)",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaBoolean("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
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
        "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 0, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
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
        "ALTER TABLE FOO ADD BAR TEXT",
        nativeMeta.getAddColumnStatement(
            "FOO",
            new ValueMetaString("BAR", nativeMeta.getMaxVARCHARLength() + 2, 0),
            "",
            false,
            "",
            false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR VARCHAR(15)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, -7), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR DECIMAL(22,7)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 22, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", -10, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR VARCHAR(45)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInternetAddress("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR IDENTITY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR"), "BAR", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR IDENTITY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 26, 8), "BAR", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR IDENTITY",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 26, 8), "", true, "BAR", false));

    assertEquals(
        "ALTER TABLE FOO DROP BAR" + System.lineSeparator(),
        nativeMeta.getDropColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ALTER BAR VARCHAR(15)",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ALTER BAR VARCHAR(2147483647)",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR"), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ADD BAR SMALLINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 4, 0), "", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR TINYINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 2, 0), "", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR IDENTITY",
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
        "ALTER TABLE FOO ADD BAR VARCHAR(1)",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 1, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD BAR TEXT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 16777250, 0), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD BAR BLOB",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBinary("BAR", 16777250, 0), "", false, "", false));

    assertEquals(
        "insert into FOO(FOOKEY, FOOVERSION) values (0, 1)",
        nativeMeta.getSqlInsertAutoIncUnknownDimensionRow("FOO", "FOOKEY", "FOOVERSION"));
  }

  @Test
  void testShowIsTreatedAsAResultsQuery() {
    List<SqlScriptStatement> sqlScriptStatements =
        new H2DatabaseMeta().getSqlScriptStatements("show annotations from service");
    assertTrue(sqlScriptStatements.getFirst().isQuery());
  }

  @Test
  void testSupportsSequence() {
    assertEquals(
        "SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SEQUENCES WHERE UPPER(SEQUENCE_NAME) = 'SEQUENCE_NAME'",
        nativeMeta.getSqlSequenceExists(sequenceName));
    assertEquals(
        "SELECT NEXT VALUE FOR " + sequenceName, nativeMeta.getSqlNextSequenceValue(sequenceName));
    assertEquals(
        "SELECT CURRENT VALUE FOR " + sequenceName,
        nativeMeta.getSqlCurrentSequenceValue(sequenceName));
  }

  /** An in-memory database of its own per test, which lives as long as the connection. */
  private Database database(String name) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new H2DatabaseMeta());
    databaseMeta.setName(name);
    databaseMeta.setDBName("mem:" + name);
    databaseMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    databaseMeta.setUsername("sa");
    return new Database(
        new SimpleLoggingObject(name, LoggingObjectType.GENERAL, null),
        new Variables(),
        databaseMeta);
  }

  @Test
  void sequencesAreCreatedListedAndRead() throws Exception {
    try (Database db = database("sequences")) {
      db.connect();
      db.execStatement(db.getCreateSequenceStatement(null, "SEQ_ONE", 1L, 1L, 999L, false));

      assertTrue(db.checkSequenceExists("SEQ_ONE"), "the sequence just created is found");
      assertFalse(db.checkSequenceExists("SEQ_MISSING"), "one never created is not invented");
      assertTrue(
          Arrays.asList(db.getSequences()).contains("SEQ_ONE"),
          "the picker lists it: " + Arrays.toString(db.getSequences()));

      assertEquals(Long.valueOf(1L), db.getNextSequenceValue("SEQ_ONE", "id"));
      assertEquals(Long.valueOf(2L), db.getNextSequenceValue("SEQ_ONE", "id"));
    }
  }

  /** A maximum of -1 stands for an unbounded sequence; H2 rejects the MAXVALUE -1 it replaces. */
  @Test
  void sequenceWithoutAMaximumIsCreated() throws Exception {
    try (Database db = database("unbounded")) {
      db.connect();
      String sql = db.getCreateSequenceStatement(null, "SEQ_UNBOUNDED", "1", "1", "-1", false);
      assertTrue(sql.contains("NOMAXVALUE"), "an unbounded sequence has no maximum: " + sql);

      db.execStatement(sql);
      assertEquals(Long.valueOf(1L), db.getNextSequenceValue("SEQ_UNBOUNDED", "id"));
    }
  }

  /** A schema tells two sequences of the same name apart. */
  @Test
  void sequencesAreLookedUpWithinTheirSchema() throws Exception {
    try (Database db = database("schemas")) {
      db.connect();
      db.execStatement("CREATE SCHEMA SIDE");
      db.execStatement(db.getCreateSequenceStatement("SIDE", "SEQ_TWO", 5L, 1L, 999L, false));

      assertTrue(db.checkSequenceExists("SIDE", "SEQ_TWO"), "found in the schema holding it");
      assertFalse(
          db.checkSequenceExists("PUBLIC", "SEQ_TWO"), "and not in the one that does not hold it");

      assertEquals(Long.valueOf(5L), db.getNextSequenceValue("SIDE", "SEQ_TWO", "id"));
    }
  }
}
