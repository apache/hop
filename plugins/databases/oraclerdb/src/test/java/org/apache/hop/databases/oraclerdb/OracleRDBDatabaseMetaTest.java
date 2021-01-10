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
package org.apache.hop.databases.oraclerdb;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.*;

public class OracleRDBDatabaseMetaTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();
  private OracleRDBDatabaseMeta nativeMeta;

  @Before
  public void setupOnce() throws Exception {
    nativeMeta = new OracleRDBDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    HopClientEnvironment.init();
  }


  @Test
  public void testOverriddenSettings() throws Exception {
    // Tests the settings of the Oracle Database Meta
    // according to the features of the DB as we know them

    assertEquals( -1, nativeMeta.getDefaultDatabasePort() );
    assertFalse( nativeMeta.supportsAutoInc() );
    assertEquals( "oracle.rdb.jdbc.rdbThin.Driver", nativeMeta.getDriverClass() );
    assertEquals( "jdbc:rdbThin://FOO:1024/BAR", nativeMeta.getURL( "FOO", "1024", "BAR" ) );
    assertEquals( "jdbc:rdbThin://FOO:11/:BAR", nativeMeta.getURL( "FOO", "11", ":BAR" ) );
    assertEquals( "jdbc:rdbThin://BAR:65534//FOO", nativeMeta.getURL( "BAR", "65534", "/FOO" ) );
    assertEquals( "jdbc:rdbThin://:/FOO", nativeMeta.getURL( "", "", "FOO" ) ); // Pretty sure this is a bug...
    assertEquals( "jdbc:rdbThin://null:-1/FOO", nativeMeta.getURL( null, "-1", "FOO" ) ); // Pretty sure this is a bug...
    assertEquals( "jdbc:rdbThin://null:null/FOO", nativeMeta.getURL( null, null, "FOO" ) ); // Pretty sure this is a bug...
    assertEquals( "jdbc:rdbThin://FOO:1234/BAR", nativeMeta.getURL( "FOO", "1234", "BAR" ) );
    assertEquals( "jdbc:rdbThin://:/", nativeMeta.getURL( "", "", "" ) ); // Pretty sure this is a bug...
    assertFalse( nativeMeta.supportsOptionsInURL() );
    assertTrue( nativeMeta.supportsSequences() );
    assertTrue( nativeMeta.useSchemaNameForTableList() );
    assertTrue( nativeMeta.supportsSynonyms() );
    String[] reservedWords =
      new String[] { "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "ARRAYLEN", "AS", "ASC", "AUDIT", "BETWEEN", "BY",
        "CHAR", "CHECK", "CLUSTER", "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE",
        "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT",
        "FOR", "FROM", "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL",
        "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS",
        "MODE", "MODIFY", "NOAUDIT", "NOCOMPRESS", "NOT", "NOTFOUND", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE",
        "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW", "RENAME",
        "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWLABEL", "ROWNUM", "ROWS", "SELECT", "SESSION", "SET", "SHARE",
        "SIZE", "SMALLINT", "SQLBUF", "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER",
        "UID", "UNION", "UNIQUE", "UPDATE", "USER", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER",
        "WHERE", "WITH" };
    assertArrayEquals( reservedWords, nativeMeta.getReservedWords() );
    assertEquals( 9999999, nativeMeta.getMaxVARCHARLength() );
    assertEquals( "SELECT SEQUENCE_NAME FROM USER_SEQUENCES", nativeMeta.getSqlListOfSequences() );
    assertEquals( "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'FOO'", nativeMeta.getSqlSequenceExists( "FOO" ) );
    assertEquals( "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'FOO'", nativeMeta.getSqlSequenceExists( "foo" ) );
    assertEquals( "SELECT FOO.currval FROM DUAL", nativeMeta.getSqlCurrentSequenceValue( "FOO" ) );
    assertEquals( "SELECT FOO.nextval FROM dual", nativeMeta.getSqlNextSequenceValue( "FOO" ) );
    String reusedFieldsQuery = "SELECT * FROM FOO WHERE 1=0";
    ;
    assertEquals( reusedFieldsQuery, nativeMeta.getSqlQueryFields( "FOO" ) );
    assertEquals( reusedFieldsQuery, nativeMeta.getSqlTableExists( "FOO" ) );
    String reusedColumnsQuery = "SELECT FOO FROM BAR WHERE 1=0";
    assertEquals( reusedColumnsQuery, nativeMeta.getSqlQueryColumnFields( "FOO", "BAR" ) );
    assertEquals( reusedColumnsQuery, nativeMeta.getSqlColumnExists( "FOO", "BAR" ) );

  }


}
