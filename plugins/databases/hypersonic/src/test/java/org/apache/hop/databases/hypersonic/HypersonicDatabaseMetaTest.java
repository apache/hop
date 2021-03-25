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

package org.apache.hop.databases.hypersonic;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.*;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * User: Dzmitry Stsiapanau Date: 1/14/14 Time: 5:08 PM
 */
public class HypersonicDatabaseMetaTest {
  private HypersonicDatabaseMeta hypersonicDatabaseMeta;
  private HypersonicDatabaseMeta hypersonicDatabaseMetaQouting;
  private HypersonicDatabaseMeta hypersonicDatabaseMetaUppercase;
  private String tableName = "teST";
  private String sequenceName = "seQuence";
  private String schemaName = "SCHema";

  public HypersonicDatabaseMetaTest() {
    hypersonicDatabaseMeta = new HypersonicDatabaseMeta();
    hypersonicDatabaseMetaQouting = new HypersonicDatabaseMeta();
    hypersonicDatabaseMetaQouting.setQuoteAllFields( true );
    hypersonicDatabaseMetaUppercase = new HypersonicDatabaseMeta();
    hypersonicDatabaseMetaUppercase.setForcingIdentifiersToUpperCase( true );
  }

  @Test
  public void testSettings() throws Exception {
    HypersonicDatabaseMeta nativeMeta = hypersonicDatabaseMeta;
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );

    assertEquals( 9001, nativeMeta.getDefaultDatabasePort() );
    assertEquals( "org.hsqldb.jdbcDriver", nativeMeta.getDriverClass() );

    assertEquals( "jdbc:hsqldb:WIBBLE", nativeMeta.getURL( "", "", "WIBBLE" ) );
    assertEquals( "jdbc:hsqldb:hsql://FOO:BAR/WIBBLE", nativeMeta.getURL( "FOO", "BAR", "WIBBLE" ) );
    assertFalse( nativeMeta.supportsBitmapIndex() );
    assertEquals( "http://hsqldb.sourceforge.net/doc/guide/ch04.html#N109DA", nativeMeta.getExtraOptionsHelpText() );
    assertArrayEquals( new String[] { "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS", "ASENSITIVE",
      "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH",
      "BY", "CALL", "CALLED", "CASCADED", "CASE", "CAST", "CHAR", "CHARACTER", "CHECK", "CLOB", "CLOSE", "COLLATE",
      "COLUMN", "COMMIT", "CONDIITON", "CONNECT", "CONSTRAINT", "CONTINUE", "CORRESPONDING", "CREATE", "CROSS", "CUBE",
      "CURRENT", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME",
      "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE", "DATE", "DAY",
      "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DEREF", "DESCRIBE", "DETERMINISTIC",
      "DISCONNECT", "DISTINCT", "DO", "DOUBLE", "DROP", "DYNAMIC", "EACH", "ELEMENT", "ELSE", "ELSEIF", "END",
      "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL", "FALSE", "FETCH", "FILTER", "FLOAT", "FOR",
      "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION", "GET", "GLOBAL", "GRANT", "GROUP", "GROUPING", "HANDLER",
      "HAVING", "HEADER", "HOLD", "HOUR", "IDENTITY", "IF", "IMMEDIATE", "IN", "INDICATOR", "INNER", "INOUT", "INPUT",
      "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "LANGUAGE",
      "LARGE", "LATERAL", "LEADING", "LEAVE", "LEFT", "LIKE", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOOP", "MATCH",
      "MEMBER", "METHOD", "MINUTE", "MODIFIES", "MODULE", "MONTH", "MULTISET", "NATIONAL", "NAUTRAL", "NCHAR", "NCLOB",
      "NEW", "NEXT", "NO", "NONE", "NOT", "NULL", "NUMERIC", "OF", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT",
      "OUTER", "OUTPUT", "OVER", "OVERLAPS", "PARAMETER", "PARTITION", "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE",
      "RANGE", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "RELEASE", "REPEAT", "RESIGNAL",
      "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SAVEPOINT", "SCOPE",
      "SCROLL", "SECOND", "SEARCH", "SELECT", "SENSITIVE", "SESSION_USER", "SET", "SIGNAL", "SIMILAR", "SMALLINT",
      "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "START", "STATIC",
      "SUBMULTISET", "SYMMETRIC", "SYSTEM", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP",
      "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSLATION", "TREAT", "TRIGGER", "TRUE", "UNDO", "UNION",
      "UNIQUE", "UNKNOWN", "UNNEST", "UNTIL", "UPDATE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING",
      "WHEN", "WHENEVER", "WHERE", "WHILE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "YEAR", "ALWAYS", "ACTION", "ADMIN",
      "AFTER", "ALIAS", "ASC", "AUTOCOMMIT", "AVG", "BACKUP", "BEFORE", "CACHED", "CASCADE", "CASEWHEN", "CHECKPOINT",
      "CLASS", "COALESCE", "COLLATION", "COMPACT", "COMPRESSED", "CONCAT", "CONVERT", "COUNT", "DATABASE", "DEFRAG",
      "DESC", "EVERY", "EXPLAIN", "EXTRACT", "GENERATED", "IFNULL", "IGNORECASE", "IMMEDIATELY", "INCREMENT", "INDEX",
      "KEY", "LIMIT", "LOGSIZE", "MAX", "MAXROWS", "MEMORY", "MERGE", "MIN", "MINUS", "NOW", "NOWAIT", "NULLIF", "NVL",
      "OFFSET", "PASSWORD", "SCHEMA", "PLAN", "PRESERVE", "POSITION", "PROPERTY", "PUBLIC", "QUEUE", "READONLY",
      "REFERENTIAL_INTEGRITY", "RENAME", "RESTART", "RESTRICT", "ROLE", "SCRIPT", "SCRIPTFORMAT", "SEQUENCE",
      "SHUTDOWN", "SOURCE", "STDDEV_POP", "STDDEV_SAMP", "SUBSTRING", "SUM", "SYSDATE", "TEMP", "TEMPORARY", "TEXT",
      "TODAY", "TOP", "TRIM", "VAR_POP", "VAR_SAMP", "VIEW", "WORK", "WRITE_DELAY" }, nativeMeta.getReservedWords() );

    assertTrue( nativeMeta.supportsSequences() );

  }

  @Test
  public void testSqlStatements() throws Exception {
    HypersonicDatabaseMeta nativeMeta = new HypersonicDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );

    assertEquals( "TRUNCATE TABLE FOO", nativeMeta.getTruncateTableStatement( "FOO" ) );
    assertEquals( "SELECT * FROM FOO", nativeMeta.getSqlQueryFields( "FOO" ) );
    assertEquals( "SELECT 1 FROM FOO", nativeMeta.getSqlTableExists( "FOO" ) );

    assertEquals( "ALTER TABLE FOO ADD BAR TIMESTAMP",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaDate( "BAR" ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR TIMESTAMP",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaTimestamp( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR CHAR(1)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBoolean( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 0, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR INTEGER",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 5, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(10, 3)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, 3 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(10, 3)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 3 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(21, 4)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 21, 4 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR LONGVARCHAR",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", nativeMeta.getMaxVARCHARLength() + 2, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR VARCHAR(15)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, -7 ), "", false, "", false ) ); // Bug here - invalid SQL

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(22, 7)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 22, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE PRECISION",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", -10, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(5, 7)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 5, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR  UNKNOWN",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInternetAddress( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 0, INCREMENT BY 1) PRIMARY KEY",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR" ), "BAR", true, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 0, INCREMENT BY 1) PRIMARY KEY",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 26, 8 ), "BAR", true, "", false ) );

    String lineSep = System.getProperty( "line.separator" );
    assertEquals( "ALTER TABLE FOO DROP BAR" + lineSep,
      nativeMeta.getDropColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO ALTER COLUMN BAR VARCHAR(15)",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO ALTER COLUMN BAR VARCHAR()",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR" ), "", false, "", true ) ); // I think this is a bug ..

    assertEquals( "ALTER TABLE FOO ADD BAR SMALLINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR", 4, 0 ), "", true, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 0, INCREMENT BY 1) PRIMARY KEY",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR" ), "BAR", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR NUMERIC(22, 0)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 22, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR VARCHAR(1)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 1, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR LONGVARCHAR",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 16777250, 0 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR  UNKNOWN",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBinary( "BAR", 16777250, 0 ), "", false, "", false ) );

    assertEquals( "insert into FOO(FOOKEY, FOOVERSION) values (0, 1)", nativeMeta.getSqlInsertAutoIncUnknownDimensionRow( "FOO", "FOOKEY", "FOOVERSION" ) );
  }

  @Test
  public void testGetSqlSequenceExists() throws Exception {
    String sql = hypersonicDatabaseMeta.getSqlSequenceExists( sequenceName );
    String expectedSql = "SELECT * FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES WHERE SEQUENCE_NAME = 'seQuence'";
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaQouting.getSqlSequenceExists( sequenceName );
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaUppercase.getSqlSequenceExists( sequenceName );
    assertEquals( expectedSql, sql );
  }

  @Test
  public void testGetSqlCurrentSequenceValue() throws Exception {
    String sql = hypersonicDatabaseMeta.getSqlCurrentSequenceValue( sequenceName );
    String expectedSql =
      "SELECT seQuence.currval FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES WHERE SEQUENCE_NAME = 'seQuence'";
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaQouting.getSqlCurrentSequenceValue( sequenceName );
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaUppercase.getSqlCurrentSequenceValue( sequenceName );
    assertEquals( expectedSql, sql );
  }

  @Test
  public void testGetSqlNextSequenceValue() throws Exception {
    String sql = hypersonicDatabaseMeta.getSqlNextSequenceValue( sequenceName );
    String expectedSql =
      "SELECT NEXT VALUE FOR seQuence FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES WHERE SEQUENCE_NAME = 'seQuence'";
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaQouting.getSqlNextSequenceValue( sequenceName );
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaUppercase.getSqlNextSequenceValue( sequenceName );
    assertEquals( expectedSql, sql );
  }

  @Test
  public void testGetSqlQueryFields() throws Exception {
    String sql = hypersonicDatabaseMeta.getSqlQueryFields( tableName );
    String expectedSql = "SELECT * FROM teST";
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaQouting.getSqlQueryFields( tableName );
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaUppercase.getSqlQueryFields( tableName );
    assertEquals( expectedSql, sql );
  }

  @Test
  public void testGetSchemaTableCombination() throws Exception {
    String sql = hypersonicDatabaseMeta.getSchemaTableCombination( schemaName, tableName );
    String expectedSql = "SCHema.teST";
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaQouting.getSchemaTableCombination( schemaName, tableName );
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaUppercase.getSchemaTableCombination( schemaName, tableName );
    assertEquals( expectedSql, sql );
  }

  @Test
  public void testGetTruncateTableStatement() throws Exception {
    String sql = hypersonicDatabaseMeta.getTruncateTableStatement( tableName );
    String expectedSql = "TRUNCATE TABLE teST";
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaQouting.getTruncateTableStatement( tableName );
    assertEquals( expectedSql, sql );
    sql = hypersonicDatabaseMetaUppercase.getTruncateTableStatement( tableName );
    assertEquals( expectedSql, sql );
  }

  @Test
  public void testGetFieldDefinition() throws Exception {
    IValueMeta vm = new ValueMetaString();
    String sql = hypersonicDatabaseMeta.getFieldDefinition( vm, null, null, false, false, false );
    String expectedSql = "VARCHAR()";
    assertEquals( "Check Apache Hop string without length", expectedSql, sql );
    vm.setLength( DatabaseMeta.CLOB_LENGTH - 1 );
    sql = hypersonicDatabaseMeta.getFieldDefinition( vm, null, null, false, false, false );
    expectedSql = "VARCHAR(" + ( DatabaseMeta.CLOB_LENGTH - 1 ) + ")";
    assertEquals( "Check Apache Hop string with length", expectedSql, sql );
    vm.setLength( DatabaseMeta.CLOB_LENGTH );
    sql = hypersonicDatabaseMeta.getFieldDefinition( vm, null, null, false, false, false );
    expectedSql = "LONGVARCHAR";
    assertEquals( "Check Apache Hop string with clob/text length", expectedSql, sql );
  }
}
