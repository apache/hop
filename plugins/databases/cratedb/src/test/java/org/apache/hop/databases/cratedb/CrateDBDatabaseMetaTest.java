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
package org.apache.hop.databases.cratedb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CrateDBDatabaseMetaTest {
  CrateDBDatabaseMeta nativeMeta;

  private static final String SEQUENCES_NOT_SUPPORTED = "CrateDB doesn't support sequences";

  @BeforeEach
  void setupBefore() {
    nativeMeta = new CrateDBDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    nativeMeta.addDefaultOptions();
  }

  @Test
  void testSettings() {
    assertEquals("&", nativeMeta.getExtraOptionSeparator());
    assertEquals("?", nativeMeta.getExtraOptionIndicator());
    assertArrayEquals(new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE}, nativeMeta.getAccessTypeList());
    assertEquals(5432, nativeMeta.getDefaultDatabasePort());
    assertEquals("io.crate.client.jdbc.CrateDriver", nativeMeta.getDriverClass());
    assertEquals("jdbc:crate://FOO:BAR/WIBBLE", nativeMeta.getURL("FOO", "BAR", "WIBBLE"));
    assertTrue(nativeMeta.isFetchSizeSupported());
    assertFalse(nativeMeta.isSupportsBitmapIndex());
    assertFalse(nativeMeta.isSupportsSynonyms());
    assertFalse(nativeMeta.isSupportsSequences());
    assertFalse(nativeMeta.isSupportsSequenceNoMaxValueOption());
    assertFalse(nativeMeta.isSupportsAutoInc());
    assertEquals(" limit 5", nativeMeta.getLimitClause(5));

    assertArrayEquals(
        new String[] {
          "ADD",
          "ALL",
          "ALTER",
          "AND",
          "ANY",
          "ARRAY",
          "AS",
          "ASC",
          "BETWEEN",
          "BY",
          "CALLED",
          "CASE",
          "CAST",
          "COLUMN",
          "CONSTRAINT",
          "COSTS",
          "CREATE",
          "CROSS",
          "CURRENT_DATE",
          "CURRENT_SCHEMA",
          "CURRENT_TIME",
          "CURRENT_TIMESTAMP",
          "CURRENT_USER",
          "DEFAULT",
          "DELETE",
          "DENY",
          "DESC",
          "DESCRIBE",
          "DIRECTORY",
          "DISTINCT",
          "DROP",
          "ELSE",
          "END",
          "ESCAPE",
          "EXCEPT",
          "EXISTS",
          "EXTRACT",
          "FALSE",
          "FIRST",
          "FOR",
          "FROM",
          "FULL",
          "FUNCTION",
          "GRANT",
          "GROUP",
          "HAVING",
          "IF",
          "IN",
          "INDEX",
          "INNER",
          "INPUT",
          "INSERT",
          "INTERSECT",
          "INTO",
          "IS",
          "JOIN",
          "LAST",
          "LEFT",
          "LIKE",
          "LIMIT",
          "MATCH",
          "NATURAL",
          "NOT",
          "NULL",
          "NULLS",
          "OBJECT",
          "OFFSET",
          "ON",
          "OR",
          "ORDER",
          "OUTER",
          "PERSISTENT",
          "RECURSIVE",
          "RESET",
          "RETURNS",
          "REVOKE",
          "RIGHT",
          "SELECT",
          "SESSION_USER",
          "SET",
          "SOME",
          "STRATIFY",
          "TABLE",
          "THEN",
          "TRANSIENT",
          "TRUE",
          "TRY_CAST",
          "UNBOUNDED",
          "UNION",
          "UPDATE",
          "USER",
          "USING",
          "WHEN",
          "WHERE",
          "WITH"
        },
        nativeMeta.getReservedWords());
    assertFalse(nativeMeta.isDefaultingToUppercase());

    assertEquals(
        "https://cratedb.com/docs/jdbc/en/latest/connect.html#connection-properties",
        nativeMeta.getExtraOptionsHelpText());
    assertFalse(nativeMeta.IsSupportsErrorHandlingOnBatchUpdates());
    assertTrue(nativeMeta.isRequiresCastToVariousForIsNull());
    assertFalse(nativeMeta.isSupportsGetBlob());
    assertTrue(nativeMeta.isUseSafePoints());
    assertTrue(nativeMeta.isSupportsBooleanDataType());
    assertTrue(nativeMeta.isSupportsTimestampDataType());
  }

  @Test
  void testSqlStatements() {
    assertEquals("SELECT * FROM FOO limit 1", nativeMeta.getSqlQueryFields("FOO"));
    assertEquals("SELECT * FROM FOO limit 1", nativeMeta.getSqlTableExists("FOO"));
    assertEquals("SELECT FOO FROM BAR limit 1", nativeMeta.getSqlColumnExists("FOO", "BAR"));
    assertEquals("SELECT FOO FROM BAR limit 1", nativeMeta.getSqlQueryColumnFields("FOO", "BAR"));
    assertThrows(UnsupportedOperationException.class, () -> nativeMeta.getSqlListOfSequences());
    assertThrows(
        UnsupportedOperationException.class, () -> nativeMeta.getSqlNextSequenceValue("FOO"));
    assertThrows(
        UnsupportedOperationException.class, () -> nativeMeta.getSqlCurrentSequenceValue("FOO"));
    assertThrows(UnsupportedOperationException.class, () -> nativeMeta.getSqlSequenceExists("FOO"));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR TIMESTAMP",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaDate("BAR"), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR TIMESTAMP",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaTimestamp("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BOOLEAN",
        nativeMeta.getAddColumnStatement("FOO", new ValueMetaBoolean("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 10, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 0, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, 3), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 10, 3), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
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
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 10, -7), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaBigNumber("BAR", 22, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", -10, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR DOUBLE PRECISION",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 5, 7), "", false, "", false));
    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR  UNKNOWN",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInternetAddress("BAR"), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGSERIAL",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR"), "BAR", true, "", false));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR BIGSERIAL",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaNumber("BAR", 26, 8), "BAR", true, "", false));

    String lineSep = System.lineSeparator();
    assertEquals(
        "ALTER TABLE FOO DROP COLUMN BAR",
        nativeMeta.getDropColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR_KTL VARCHAR(15);"
            + lineSep
            + "UPDATE FOO SET BAR_KTL=BAR;"
            + lineSep
            + "ALTER TABLE FOO DROP COLUMN BAR;"
            + lineSep
            + "ALTER TABLE FOO RENAME BAR_KTL TO BAR;"
            + lineSep,
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR", 15, 0), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR_KTL TEXT;"
            + lineSep
            + "UPDATE FOO SET BAR_KTL=BAR;"
            + lineSep
            + "ALTER TABLE FOO DROP COLUMN BAR;"
            + lineSep
            + "ALTER TABLE FOO RENAME BAR_KTL TO BAR;"
            + lineSep,
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaString("BAR"), "", false, "", true));

    assertEquals(
        "ALTER TABLE FOO ADD COLUMN BAR SMALLINT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaInteger("BAR", 4, 0), "", true, "", false));

    assertThrows(
        UnsupportedOperationException.class,
        () -> nativeMeta.getSqlLockTables(new String[] {"FOO", "BAR"}));

    assertThrows(
        UnsupportedOperationException.class,
        () -> nativeMeta.getSqlUnlockTables(new String[] {"FOO"}));
  }

  @Test
  void doesNotSupportSequences() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> nativeMeta.getSqlListOfSequences(),
        SEQUENCES_NOT_SUPPORTED);
    assertThrows(
        UnsupportedOperationException.class,
        () -> nativeMeta.getSqlNextSequenceValue("FOO"),
        SEQUENCES_NOT_SUPPORTED);
    assertThrows(
        UnsupportedOperationException.class,
        () -> nativeMeta.getSqlCurrentSequenceValue("FOO"),
        SEQUENCES_NOT_SUPPORTED);
    assertThrows(
        UnsupportedOperationException.class,
        () -> nativeMeta.getSqlSequenceExists("FOO"),
        SEQUENCES_NOT_SUPPORTED);
  }
}
