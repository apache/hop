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
package org.apache.hop.databases.mysql;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
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
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MySqlDatabaseMetaTest {
  MySqlDatabaseMeta nativeMeta;

  @ClassRule
  public static RestoreHopEnvironment env = new RestoreHopEnvironment();
	
  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
		PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
		PluginRegistry.addPluginType(DatabasePluginType.getInstance());
		PluginRegistry.init();	
  }
  
  @Before
  public void setupBefore() {
    nativeMeta = new MySqlDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
  }

  @Test
  public void testSettings() throws Exception {
    assertArrayEquals( new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE },
      nativeMeta.getAccessTypeList() );
    assertEquals( 3306, nativeMeta.getDefaultDatabasePort() );
    assertTrue( nativeMeta.supportsAutoInc() );
    assertEquals( 1, nativeMeta.getNotFoundTK( true ) );
    assertEquals( 0, nativeMeta.getNotFoundTK( false ) );
    assertEquals( "org.gjt.mm.mysql.Driver", nativeMeta.getDriverClass() );
    assertEquals( "jdbc:mysql://FOO:BAR/WIBBLE", nativeMeta.getURL( "FOO", "BAR", "WIBBLE" ) );
    assertEquals( "jdbc:mysql://FOO/WIBBLE", nativeMeta.getURL( "FOO", "", "WIBBLE" ) );
    assertEquals( "&", nativeMeta.getExtraOptionSeparator() );
    assertEquals( "?", nativeMeta.getExtraOptionIndicator() );
    assertFalse( nativeMeta.supportsTransactions() );
    assertFalse( nativeMeta.supportsBitmapIndex() );
    assertTrue( nativeMeta.supportsViews() );
    assertFalse( nativeMeta.supportsSynonyms() );
    assertArrayEquals( new String[] { "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN",
      "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK",
      "COLLATE", "COLUMN", "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS",
      "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES",
      "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED",
      "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH",
      "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FOR", "FORCE",
      "FOREIGN", "FROM", "FULLTEXT", "GOTO", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND",
      "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT",
      "INT", "INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING", "LEAVE", "LEFT",
      "LIKE", "LIMIT", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCATE", "LOCK", "LONG", "LONGBLOB", "LONGTEXT",
      "LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND",
      "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE",
      "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "POSITION", "PRECISION", "PRIMARY", "PROCEDURE",
      "PURGE", "READ", "READS", "REAL", "REFERENCES", "REGEXP", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT",
      "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE",
      "SEPARATOR", "SET", "SHOW", "SMALLINT", "SONAME", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE",
      "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN",
      "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO",
      "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME",
      "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH",
      "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL" }, nativeMeta.getReservedWords() );

    assertEquals( "`", nativeMeta.getStartQuote() );
    assertEquals( "`", nativeMeta.getEndQuote() );
    assertEquals( "http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-configuration-properties.html", nativeMeta.getExtraOptionsHelpText() );
    assertTrue( nativeMeta.isSystemTable( "sysTest" ) );
    assertTrue( nativeMeta.isSystemTable( "dtproperties" ) );
    assertFalse( nativeMeta.isSystemTable( "SysTest" ) );
    assertFalse( nativeMeta.isSystemTable( "dTproperties" ) );
    assertFalse( nativeMeta.isSystemTable( "Testsys" ) );
    assertTrue( nativeMeta.isMySqlVariant() );
    assertFalse( nativeMeta.releaseSavepoint() );
    assertTrue( nativeMeta.supportsErrorHandlingOnBatchUpdates() );
    assertFalse( nativeMeta.isRequiringTransactionsOnQueries() );
  }

  @Test
  public void testSqlStatements() {
    assertEquals( " LIMIT 15", nativeMeta.getLimitClause( 15 ) );
    assertEquals( "SELECT * FROM FOO LIMIT 0", nativeMeta.getSqlQueryFields( "FOO" ) );
    assertEquals( "SELECT * FROM FOO LIMIT 0", nativeMeta.getSqlTableExists( "FOO" ) );
    assertEquals( "SELECT FOO FROM BAR LIMIT 0", nativeMeta.getSqlQueryColumnFields( "FOO", "BAR" ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DATETIME",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaDate( "BAR" ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR DATETIME",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaTimestamp( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR CHAR(1)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBoolean( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR INT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 0, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR INT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 5, 0 ), "", false, "", false ) );


    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, 3 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 3 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DECIMAL(21, 4)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 21, 4 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR MEDIUMTEXT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", nativeMeta.getMaxVARCHARLength() + 2, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR VARCHAR(15)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 10, -7 ), "", false, "", false ) ); // Bug here - invalid SQL

    assertEquals( "ALTER TABLE FOO ADD BAR DECIMAL(22, 7)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 22, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", -10, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR DOUBLE",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 5, 7 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR  UNKNOWN",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInternetAddress( "BAR" ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR" ), "BAR", true, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaNumber( "BAR", 26, 8 ), "BAR", true, "", false ) );

    String lineSep = System.getProperty( "line.separator" );
    assertEquals( "ALTER TABLE FOO DROP BAR" + lineSep,
      nativeMeta.getDropColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO MODIFY BAR VARCHAR(15)",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR", 15, 0 ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO MODIFY BAR TINYTEXT",
      nativeMeta.getModifyColumnStatement( "FOO", new ValueMetaString( "BAR" ), "", false, "", true ) );

    assertEquals( "ALTER TABLE FOO ADD BAR INT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR", 4, 0 ), "", true, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT NOT NULL PRIMARY KEY",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaInteger( "BAR" ), "BAR", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR BIGINT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 10, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR DECIMAL(22)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBigNumber( "BAR", 22, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR CHAR(1)",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 1, 0 ), "", false, "", false ) );

    assertEquals( "ALTER TABLE FOO ADD BAR LONGTEXT",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaString( "BAR", 16777250, 0 ), "", false, "", false ) );
    assertEquals( "ALTER TABLE FOO ADD BAR LONGBLOB",
      nativeMeta.getAddColumnStatement( "FOO", new ValueMetaBinary( "BAR", 16777250, 0 ), "", false, "", false ) );

    assertEquals( "LOCK TABLES FOO WRITE, BAR WRITE;" + lineSep,
      nativeMeta.getSqlLockTables( new String[] { "FOO", "BAR" } ) );

    assertEquals( "UNLOCK TABLES", nativeMeta.getSqlUnlockTables( new String[] {} ) );

    assertEquals( "insert into FOO(FOOKEY, FOOVERSION) values (1, 1)", nativeMeta.getSqlInsertAutoIncUnknownDimensionRow( "FOO", "FOOKEY", "FOOVERSION" ) );
  }

  /**
   * @return
   * @throws Exception
   */
  private ResultSetMetaData getResultSetMetaData() throws Exception {
    ResultSetMetaData resultSetMetaData = mock( ResultSetMetaData.class );

    /**
     * Fields setup around the following query:
     *
     * select
     *   CUSTOMERNUMBER as NUMBER
     * , CUSTOMERNAME as NAME
     * , CONTACTLASTNAME as LAST_NAME
     * , CONTACTFIRSTNAME as FIRST_NAME
     * , 'MySQL' as DB
     * , 'NoAliasText'
     * from CUSTOMERS
     * ORDER BY CUSTOMERNAME;
     */

    doReturn( "NUMBER" ).when( resultSetMetaData ).getColumnLabel( 1 );
    doReturn( "NAME" ).when( resultSetMetaData ).getColumnLabel( 2 );
    doReturn( "LAST_NAME" ).when( resultSetMetaData ).getColumnLabel( 3 );
    doReturn( "FIRST_NAME" ).when( resultSetMetaData ).getColumnLabel( 4 );
    doReturn( "DB" ).when( resultSetMetaData ).getColumnLabel( 5 );
    doReturn( "NoAliasText" ).when( resultSetMetaData ).getColumnLabel( 6 );

    doReturn( "CUSTOMERNUMBER" ).when( resultSetMetaData ).getColumnName( 1 );
    doReturn( "CUSTOMERNAME" ).when( resultSetMetaData ).getColumnName( 2 );
    doReturn( "CONTACTLASTNAME" ).when( resultSetMetaData ).getColumnName( 3 );
    doReturn( "CONTACTFIRSTNAME" ).when( resultSetMetaData ).getColumnName( 4 );
    doReturn( "MySQL" ).when( resultSetMetaData ).getColumnName( 5 );
    doReturn( "NoAliasText" ).when( resultSetMetaData ).getColumnName( 6 );

    return resultSetMetaData;
  }

  /**
   * @return
   * @throws Exception
   */
  private ResultSetMetaData getResultSetMetaDataException() throws Exception {
    ResultSetMetaData resultSetMetaData = mock( ResultSetMetaData.class );

    doThrow( new SQLException() ).when( resultSetMetaData ).getColumnLabel( 1 );
    doThrow( new SQLException() ).when( resultSetMetaData ).getColumnName( 1 );

    return resultSetMetaData;
  }

  @Test
  @Ignore // TODO: Fix test extra options
  public void testExtraOptions() {    
	  Map<String, String> opts = nativeMeta.getExtraOptions();
	  assertNotNull( opts );
	  assertEquals( "500", opts.get( "MYSQL.defaultFetchSize" ) );
  }
  
  @Test
  public void testGetLegacyColumnNameDriverGreaterThanThreeFieldNumber() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(5);

    assertEquals( "NUMBER", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 1 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverGreaterThanThreeFieldName() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(5);

    assertEquals( "NAME", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 2 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverGreaterThanThreeFieldLastName() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(5);

    assertEquals( "LAST_NAME", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 3 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverGreaterThanThreeFieldFirstName() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(5);

    assertEquals( "FIRST_NAME", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 4 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverGreaterThanThreeFieldDB() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(5);

    assertEquals( "DB", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 5 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverGreaterThanThreeFieldNoAliasText() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(5);

    assertEquals( "NoAliasText", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 6 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverLessOrEqualToThreeFieldCustomerNumber() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(3);

    assertEquals( "CUSTOMERNUMBER", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 1 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverLessOrEqualToThreeFieldCustomerName() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(3);

    assertEquals( "CUSTOMERNAME", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 2 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverLessOrEqualToThreeFieldContactLastName() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(3);

    assertEquals( "CONTACTLASTNAME", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 3 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverLessOrEqualToThreeFieldContactFirstName() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(3);

    assertEquals( "CONTACTFIRSTNAME", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 4 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverLessOrEqualToThreeFieldMySql() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(3);

    assertEquals( "MySQL", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 5 ) );
  }

  @Test
  public void testGetLegacyColumnNameDriverLessOrEqualToThreeFieldNoAliasText() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(3);

    assertEquals( "NoAliasText", new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaData(), 6 ) );
  }

  @Test( expected = HopDatabaseException.class )
  public void testGetLegacyColumnNameNullDBMetaDataException() throws Exception {
    new MySqlDatabaseMeta().getLegacyColumnName( null, getResultSetMetaData(), 1 );
  }

  @Test( expected = HopDatabaseException.class )
  public void testGetLegacyColumnNameNullRSMetaDataException() throws Exception {
    new MySqlDatabaseMeta().getLegacyColumnName( mock( DatabaseMetaData.class ), null, 1 );
  }

  @Test( expected = HopDatabaseException.class )
  public void testGetLegacyColumnNameDriverGreaterThanThreeException() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(5);

    new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaDataException(), 1 );
  }

  @Test( expected = HopDatabaseException.class )
  public void testGetLegacyColumnNameDriverLessOrEqualToThreeException() throws Exception {
    DatabaseMetaData databaseMetaData = mock( DatabaseMetaData.class );
    when( databaseMetaData.getDriverMajorVersion() ).thenReturn(3);

    new MySqlDatabaseMeta().getLegacyColumnName( databaseMetaData, getResultSetMetaDataException(), 1 );
  }

  @Test
  public void testReleaseSavepoint() {
    assertFalse( nativeMeta.releaseSavepoint() );
  }

  @Test
  public void testSupportsSequence() {
    String dbType = nativeMeta.getClass().getSimpleName();
    assertFalse( dbType, nativeMeta.supportsSequences() );
    assertTrue( Utils.isEmpty( nativeMeta.getSqlListOfSequences() ) );
    assertEquals( "", nativeMeta.getSqlSequenceExists( "testSeq" ) );
    assertEquals( "", nativeMeta.getSqlNextSequenceValue( "testSeq" ) );
    assertEquals( "", nativeMeta.getSqlCurrentSequenceValue( "testSeq" ) );
  }
  
  private Connection mockConnection( DatabaseMetaData dbMetaData ) throws SQLException {
	Connection conn = mock( Connection.class );
	when( conn.getMetaData() ).thenReturn( dbMetaData );
	return conn;
  }
  
  @Test
  public void testVarBinaryIsConvertedToStringType() throws Exception {	
	ILoggingObject log = mock( ILoggingObject.class );
	PreparedStatement ps = mock( PreparedStatement.class );  
	DatabaseMetaData dbMetaData = mock( DatabaseMetaData.class );
  IVariables variables = mock (IVariables.class);
	ResultSet rs = mock( ResultSet.class );
    ResultSetMetaData rsMeta = mock( ResultSetMetaData.class );
    
    when( rsMeta.getColumnCount() ).thenReturn( 1 );
    when( rsMeta.getColumnLabel( 1 ) ).thenReturn( "column" );
    when( rsMeta.getColumnName( 1 ) ).thenReturn( "column" );
    when( rsMeta.getColumnType( 1 ) ).thenReturn( java.sql.Types.VARBINARY );
    when( rs.getMetaData() ).thenReturn( rsMeta );
    when( ps.executeQuery() ).thenReturn( rs );

    DatabaseMeta meta = new DatabaseMeta();
    meta.setIDatabase( new MySqlDatabaseMeta() );

    Database db = new Database( log, variables, meta );
    db.setConnection( mockConnection( dbMetaData ) );
    db.getLookup( ps, false );

    IRowMeta rowMeta = db.getReturnRowMeta();
    assertEquals( 1, db.getReturnRowMeta().size() );

    IValueMeta valueMeta = rowMeta.getValueMeta( 0 );
    assertEquals( IValueMeta.TYPE_BINARY, valueMeta.getType() );
  }

}
