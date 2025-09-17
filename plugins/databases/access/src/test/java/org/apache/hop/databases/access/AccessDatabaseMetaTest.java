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

package org.apache.hop.databases.access;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

class AccessDatabaseMetaTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  AccessDatabaseMeta nativeMeta;

  @Before
  public void setupOnce() throws Exception {
    nativeMeta = new AccessDatabaseMeta();
    nativeMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    HopClientEnvironment.init();
  }

  @Test
  void testSettings() {
    int[] aTypes = new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
    assertArrayEquals(aTypes, nativeMeta.getAccessTypeList());
    assertEquals("net.ucanaccess.jdbc.UcanaccessDriver", nativeMeta.getDriverClass());
    assertEquals(8388607, nativeMeta.getMaxTextFieldLength());
    assertEquals(
        "jdbc:ucanaccess://e://Dir//Contacts.accdb;showSchema=true",
        nativeMeta.getURL(
            null,
            null,
            "e://Dir//Contacts.accdb")); // note - MS Access driver ignores the server and port
    String[] expectedReservedWords =
        new String[] {
          "AND",
          "ANY",
          "AS",
          "ALL",
          "AT",
          "AVG",
          "BETWEEN",
          "BOTH",
          "BY",
          "CALL",
          "CASE",
          "CAST",
          "COALESCE",
          "CONSTRAINT",
          "CORRESPONDING",
          "CONVERT",
          "COUNT",
          "CREATE",
          "CROSS",
          "DEFAULT",
          "DISTINCT",
          "DO",
          "DROP",
          "ELSE",
          "EVERY",
          "EXISTS",
          "EXCEPT",
          "FOR",
          "FROM",
          "FULL",
          "GRANT",
          "GROUP",
          "HAVING",
          "IN",
          "INNER",
          "INTERSECT",
          "INTO",
          "IS",
          "JOIN",
          "LEFT",
          "LEADING",
          "LIKE",
          "MAX",
          "MIN",
          "NATURAL",
          "NOT",
          "NULLIF",
          "ON",
          "ORDER",
          "OR",
          "OUTER",
          "PRIMARY",
          "REFERENCES",
          "RIGHT",
          "SELECT",
          "SET",
          "SOME",
          "STDDEV_POP",
          "STDDEV_SAMP",
          "SUM",
          "TABLE",
          "THEN",
          "TO",
          "TRAILING",
          "TRIGGER",
          "UNION",
          "UNIQUE",
          "USING",
          "USER",
          "VALUES",
          "VAR_POP",
          "VAR_SAMP",
          "WHEN",
          "WHERE",
          "WITH",
          "END"
        };

    assertArrayEquals(expectedReservedWords, nativeMeta.getReservedWords());
    assertFalse(nativeMeta.isSupportsFloatRoundingOnUpdate());
    assertEquals(255, nativeMeta.getMaxVARCHARLength());
    assertFalse(nativeMeta.isSupportsSequences());
  }

  @Test
  void testSqlStatements() {
    assertEquals("DELETE FROM FOO", nativeMeta.getTruncateTableStatement("FOO"));
    assertEquals(
        "ALTER TABLE FOO ADD BAR TEXT",
        nativeMeta.getAddColumnStatement(
            "FOO", new ValueMetaString("BAR", 100, 0), "", false, "", false));

    assertEquals(
        "ALTER TABLE FOO ALTER COLUMN BAR SET DATETIME",
        nativeMeta.getModifyColumnStatement(
            "FOO", new ValueMetaTimestamp("BAR"), "", false, "", false));
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
        "YESNO",
        nativeMeta.getFieldDefinition(new ValueMetaBoolean("FOO"), "", "", false, false, false));

    assertEquals(
        "DOUBLE",
        nativeMeta.getFieldDefinition(new ValueMetaNumber("FOO"), "", "", false, false, false));
    assertEquals(
        "LONG",
        nativeMeta.getFieldDefinition(
            new ValueMetaInteger("FOO", 5, 0), "", "", false, false, false));
    assertEquals(
        "DOUBLE",
        nativeMeta.getFieldDefinition(
            new ValueMetaNumber("FOO", 5, 3), "", "", false, false, false));

    assertEquals(
        "NUMERIC",
        nativeMeta.getFieldDefinition(
            new ValueMetaBigNumber("FOO", 0, 3), "", "", false, false, false)); // This is a bug

    assertEquals(
        "MEMO",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", DatabaseMeta.CLOB_LENGTH + 1, 0),
            "",
            "",
            false,
            false,
            false));

    assertEquals(
        "TEXT",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", nativeMeta.getMaxVARCHARLength() - 1, 0),
            "",
            "",
            false,
            false,
            false));

    assertEquals(
        "MEMO",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", nativeMeta.getMaxVARCHARLength() + 1, 0),
            "",
            "",
            false,
            false,
            false));

    assertEquals(
        "MEMO",
        nativeMeta.getFieldDefinition(
            new ValueMetaString("FOO", DatabaseMeta.CLOB_LENGTH - 1, 0),
            "",
            "",
            false,
            false,
            false));

    assertEquals(
        " UNKNOWN",
        nativeMeta.getFieldDefinition(
            new ValueMetaInternetAddress("FOO"), "", "", false, false, false));

    assertEquals(
        " UNKNOWN" + System.getProperty("line.separator"),
        nativeMeta.getFieldDefinition(
            new ValueMetaInternetAddress("FOO"), "", "", false, false, true));
  }
}
