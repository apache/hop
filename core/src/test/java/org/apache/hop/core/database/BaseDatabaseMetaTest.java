/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.core.database;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BaseDatabaseMetaTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();
  BaseDatabaseMeta nativeMeta, odbcMeta;

  @Before
  public void setupOnce() throws Exception {
    nativeMeta = new ConcreteBaseDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    odbcMeta = new ConcreteBaseDatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );
    HopClientEnvironment.init();
  }

  @Test
  public void testDefaultSettings() throws Exception {
    // Note - this method should only use native or odbc.
    // (each test run in its own thread).
    assertEquals( -1, nativeMeta.getDefaultDatabasePort() );
    assertTrue( nativeMeta.supportsSetCharacterStream() );
    assertTrue( nativeMeta.supportsAutoInc() );
    assertEquals( "", nativeMeta.getLimitClause( 5 ) );
    assertEquals( 0, nativeMeta.getNotFoundTK( true ) );
    assertEquals( "", nativeMeta.getSQLNextSequenceValue( "FOO" ) );
    assertEquals( "", nativeMeta.getSQLCurrentSequenceValue( "FOO" ) );
    assertEquals( "", nativeMeta.getSQLSequenceExists( "FOO" ) );
    assertTrue( nativeMeta.isFetchSizeSupported() );
    assertFalse( nativeMeta.needsPlaceHolder() );
    assertTrue( nativeMeta.supportsSchemas() );
    assertTrue( nativeMeta.supportsCatalogs() );
    assertTrue( nativeMeta.supportsEmptyTransactions() );
    assertEquals( "SUM", nativeMeta.getFunctionSum() );
    assertEquals( "AVG", nativeMeta.getFunctionAverage() );
    assertEquals( "MIN", nativeMeta.getFunctionMinimum() );
    assertEquals( "MAX", nativeMeta.getFunctionMaximum() );
    assertEquals( "COUNT", nativeMeta.getFunctionCount() );
    assertEquals( "\"", nativeMeta.getStartQuote() );
    assertEquals( "\"", nativeMeta.getEndQuote() );
    assertEquals( "FOO.BAR", nativeMeta.getSchemaTableCombination( "FOO", "BAR" ) );
    assertEquals( DatabaseMeta.CLOB_LENGTH, nativeMeta.getMaxTextFieldLength() );
    assertEquals( DatabaseMeta.CLOB_LENGTH, nativeMeta.getMaxVARCHARLength() );
    assertTrue( nativeMeta.supportsTransactions() );
    assertFalse( nativeMeta.supportsSequences() );
    assertTrue( nativeMeta.supportsBitmapIndex() );
    assertTrue( nativeMeta.supportsSetLong() );
    assertArrayEquals( new String[] {}, nativeMeta.getReservedWords() );
    assertTrue( nativeMeta.quoteReservedWords() );
    assertArrayEquals( new String[] { "TABLE" }, nativeMeta.getTableTypes() );
    assertArrayEquals( new String[] { "VIEW" }, nativeMeta.getViewTypes() );
    assertArrayEquals( new String[] { "SYNONYM" }, nativeMeta.getSynonymTypes() );
    assertFalse( nativeMeta.useSchemaNameForTableList() );
    assertTrue( nativeMeta.supportsViews() );
    assertFalse( nativeMeta.supportsSynonyms() );
    assertNull( nativeMeta.getSQLListOfProcedures() );
    assertNull( nativeMeta.getSQLListOfSequences() );
    assertTrue( nativeMeta.supportsFloatRoundingOnUpdate() );
    assertNull( nativeMeta.getSQLLockTables( new String[] { "FOO" } ) );
    assertNull( nativeMeta.getSQLUnlockTables( new String[] { "FOO" } ) );
    assertTrue( nativeMeta.supportsTimeStampToDateConversion() );
    assertTrue( nativeMeta.supportsBatchUpdates() );
    assertFalse( nativeMeta.supportsBooleanDataType() );
    assertFalse( nativeMeta.supportsTimestampDataType() );
    assertTrue( nativeMeta.preserveReservedCase() );
    assertTrue( nativeMeta.isDefaultingToUppercase() );
    Map<String, String> emptyMap = new HashMap<>();
    assertEquals( emptyMap, nativeMeta.getExtraOptions() );
    assertEquals( ";", nativeMeta.getExtraOptionSeparator() );
    assertEquals( "=", nativeMeta.getExtraOptionValueSeparator() );
    assertEquals( ";", nativeMeta.getExtraOptionIndicator() );
    assertTrue( nativeMeta.supportsOptionsInURL() );
    assertNull( nativeMeta.getExtraOptionsHelpText() );
    assertTrue( nativeMeta.supportsGetBlob() );
    assertNull( nativeMeta.getConnectSQL() );
    assertTrue( nativeMeta.supportsSetMaxRows() );
    assertTrue( nativeMeta.isStreamingResults() );
    assertFalse( nativeMeta.isQuoteAllFields() );
    assertFalse( nativeMeta.isForcingIdentifiersToLowerCase() );
    assertFalse( nativeMeta.isForcingIdentifiersToUpperCase() );
    assertFalse( nativeMeta.isUsingDoubleDecimalAsSchemaTableSeparator() );
    assertTrue( nativeMeta.isRequiringTransactionsOnQueries() );
    assertEquals( "org.apache.hop.core.database.DatabaseFactory", nativeMeta.getDatabaseFactoryName() );
    assertNull( nativeMeta.getPreferredSchemaName() );
    assertFalse( nativeMeta.supportsSequenceNoMaxValueOption() );
    assertFalse( nativeMeta.requiresCreateTablePrimaryKeyAppend() );
    assertFalse( nativeMeta.requiresCastToVariousForIsNull() );
    assertFalse( nativeMeta.isDisplaySizeTwiceThePrecision() );
    assertTrue( nativeMeta.supportsPreparedStatementMetadataRetrieval() );
    assertFalse( nativeMeta.supportsResultSetMetadataRetrievalOnly() );
    assertFalse( nativeMeta.isSystemTable( "FOO" ) );
    assertTrue( nativeMeta.supportsNewLinesInSQL() );
    assertNull( nativeMeta.getSQLListOfSchemas() );
    assertEquals( 0, nativeMeta.getMaxColumnsInIndex() );
    assertTrue( nativeMeta.supportsErrorHandlingOnBatchUpdates() );
    assertTrue( nativeMeta.isExplorable() );
    assertTrue( nativeMeta.onlySpaces( "   \t   \n  \r   " ) );
    assertFalse( nativeMeta.isMySQLVariant() );
    assertTrue( nativeMeta.canTest() );
    assertTrue( nativeMeta.requiresName() );
    assertTrue( nativeMeta.releaseSavepoint() );
    Variables v = new Variables();
    v.setVariable( "FOOVARIABLE", "FOOVALUE" );
    DatabaseMeta dm = new DatabaseMeta();
    dm.setIDatabase( nativeMeta );
    assertEquals( "", nativeMeta.getDataTablespaceDDL( v, dm ) );
    assertEquals( "", nativeMeta.getIndexTablespaceDDL( v, dm ) );
    assertFalse( nativeMeta.useSafePoints() );
    assertTrue( nativeMeta.supportsErrorHandling() );
    assertEquals( "'DATA'", nativeMeta.getSQLValue( new ValueMetaString( "FOO" ), "DATA", null ) );
    assertEquals( "'15'", nativeMeta.getSQLValue( new ValueMetaString( "FOO" ), "15", null ) );
    assertEquals( "_", nativeMeta.getFieldnameProtector() );
    assertEquals( "_1ABC_123", nativeMeta.getSafeFieldname( "1ABC 123" ) );
    BaseDatabaseMeta tmpSC = new ConcreteBaseDatabaseMeta() {
      @Override
      public String[] getReservedWords() {
        return new String[] { "SELECT" };
      }
    };
    assertEquals( "SELECT_", tmpSC.getSafeFieldname( "SELECT" ) );
    assertEquals( "NOMAXVALUE", nativeMeta.getSequenceNoMaxValueOption() );
    assertTrue( nativeMeta.supportsAutoGeneratedKeys() );
    assertNull( nativeMeta.customizeValueFromSQLType( new ValueMetaString( "FOO" ), null, 0 ) );
    assertTrue( nativeMeta.fullExceptionLog( new RuntimeException( "xxxx" ) ) );
  }

  @SuppressWarnings( "deprecation" )
  @Test
  public void testDeprecatedItems() throws Exception {
    assertEquals( "'2016-08-11'", nativeMeta.getSQLValue( new ValueMetaDate( "FOO" ), new Date( 116, 7, 11 ), "YYYY-MM-dd" ) );
    assertEquals( "\"FOO\".\"BAR\"", nativeMeta.getBackwardsCompatibleSchemaTableCombination( "FOO", "BAR" ) );
    assertEquals( "\"null\".\"BAR\"", nativeMeta.getBackwardsCompatibleSchemaTableCombination( null, "BAR" ) ); // not sure this is right ...
    assertEquals( "FOO\".\"BAR\"", nativeMeta.getBackwardsCompatibleSchemaTableCombination( "FOO\"", "BAR" ) );
    assertEquals( "FOO\".BAR\"", nativeMeta.getBackwardsCompatibleSchemaTableCombination( "FOO\"", "BAR\"" ) );
    assertEquals( "\"FOO\"", nativeMeta.getBackwardsCompatibleTable( "FOO" ) );
    assertEquals( "\"null\"", nativeMeta.getBackwardsCompatibleTable( null ) ); // not sure this should happen but it does
    assertEquals( "FOO\"", nativeMeta.getBackwardsCompatibleTable( "FOO\"" ) );
    assertEquals( "\"FOO", nativeMeta.getBackwardsCompatibleTable( "\"FOO" ) );

  }

  @Test
  public void testDefaultSQLStatements() {
    // Note - this method should use only native or odbc metas.
    String lineSep = System.getProperty( "line.separator" );
    String expected = "ALTER TABLE FOO DROP BAR" + lineSep;
    assertEquals( expected, odbcMeta.getDropColumnStatement( "FOO", new ValueMetaString( "BAR" ), "", false, "", false ) );
    assertEquals( "TRUNCATE TABLE FOO", odbcMeta.getTruncateTableStatement( "FOO" ) );
    assertEquals( "SELECT * FROM FOO", odbcMeta.getSQLQueryFields( "FOO" ) );
    assertEquals( "SELECT 1 FROM FOO", odbcMeta.getSQLTableExists( "FOO" ) );
    assertEquals( "SELECT FOO FROM BAR", odbcMeta.getSQLColumnExists( "FOO", "BAR" ) );
    assertEquals( "insert into \"FOO\".\"BAR\"(KEYFIELD, VERSIONFIELD) values (0, 1)",
      nativeMeta.getSQLInsertAutoIncUnknownDimensionRow( "\"FOO\".\"BAR\"", "KEYFIELD", "VERSIONFIELD" ) );
    assertEquals( "select count(*) FROM FOO", nativeMeta.getSelectCountStatement( "FOO" ) );
    assertEquals( "COL9", nativeMeta.generateColumnAlias( 9, "FOO" ) );
    assertEquals( "[SELECT 1, INSERT INTO FOO VALUES(BAR), DELETE FROM BAR]", nativeMeta.parseStatements( "SELECT 1;INSERT INTO FOO VALUES(BAR);DELETE FROM BAR" ).toString() );
    assertEquals( "CREATE TABLE ", nativeMeta.getCreateTableStatement() );
    assertEquals( "DROP TABLE IF EXISTS FOO", nativeMeta.getDropTableIfExistsStatement( "FOO" ) );
  }

  @Test
  public void testGettersSetters() {
    nativeMeta.setUsername( "FOO" );
    assertEquals( "FOO", nativeMeta.getUsername() );
    nativeMeta.setPassword( "BAR" );
    assertEquals( "BAR", nativeMeta.getPassword() );
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    assertEquals( "FOO", nativeMeta.getUsername() );
    assertEquals( "BAR", nativeMeta.getPassword() );
    assertFalse( nativeMeta.isChanged() );
    nativeMeta.setChanged( true );
    assertTrue( nativeMeta.isChanged() );
    nativeMeta.setDatabaseName( "FOO" );
    assertEquals( "FOO", nativeMeta.getDatabaseName() );
    nativeMeta.setHostname( "FOO" );
    assertEquals( "FOO", nativeMeta.getHostname() );
    nativeMeta.setServername( "FOO" );
    assertEquals( "FOO", nativeMeta.getServername() );
    nativeMeta.setDataTablespace( "FOO" );
    assertEquals( "FOO", nativeMeta.getDataTablespace() );
    nativeMeta.setIndexTablespace( "FOO" );
    assertEquals( "FOO", nativeMeta.getIndexTablespace() );
    Properties attrs = nativeMeta.getAttributes();
    Properties testAttrs = new Properties();
    testAttrs.setProperty( "FOO", "BAR" );
    nativeMeta.setAttributes( testAttrs );
    assertEquals( testAttrs, nativeMeta.getAttributes() );
    nativeMeta.setAttributes( attrs ); // reset attributes back to what they were...
    nativeMeta.setSupportsBooleanDataType( true );
    assertTrue( nativeMeta.supportsBooleanDataType() );
    nativeMeta.setSupportsTimestampDataType( true );
    assertTrue( nativeMeta.supportsTimestampDataType() );
    nativeMeta.setPreserveReservedCase( false );
    assertFalse( nativeMeta.preserveReservedCase() );
    nativeMeta.addExtraOption( "JNDI", "FOO", "BAR" );
    Map<String, String> expectedOptionsMap = new HashMap<>();
    expectedOptionsMap.put( "JNDI.FOO", "BAR" );
    assertEquals( expectedOptionsMap, nativeMeta.getExtraOptions() );
    nativeMeta.setConnectSQL( "SELECT COUNT(*) FROM FOO" );
    assertEquals( "SELECT COUNT(*) FROM FOO", nativeMeta.getConnectSQL() );
    PartitionDatabaseMeta[] clusterInfo = new PartitionDatabaseMeta[ 1 ];
    PartitionDatabaseMeta aClusterDef = new PartitionDatabaseMeta( "FOO", "BAR", "WIBBLE", "NATTIE" );
    aClusterDef.setUsername( "FOOUSER" );
    aClusterDef.setPassword( "BARPASSWORD" );
    clusterInfo[ 0 ] = aClusterDef;
    // MB: Can't use arrayEquals because the PartitionDatabaseMeta doesn't have a toString. :(
    // assertArrayEquals( clusterInfo, gotPartitions );

    Properties poolProperties = new Properties();
    poolProperties.put( "FOO", "BAR" );
    poolProperties.put( "BAR", "FOO" );
    poolProperties.put( "ZZZZZZZZZZZZZZ", "Z.Z.Z.Z.Z.Z.Z.Z.a.a.a.a.a.a.a.a.a" );
    poolProperties.put( "TOM", "JANE" );
    poolProperties.put( "AAAAAAAAAAAAA", "BBBBB.BBB.BBBBBBB.BBBBBBBB.BBBBBBBBBBBBBB" );
    nativeMeta.setStreamingResults( false );
    assertFalse( nativeMeta.isStreamingResults() );
    nativeMeta.setQuoteAllFields( true );
    nativeMeta.setForcingIdentifiersToLowerCase( true );
    nativeMeta.setForcingIdentifiersToUpperCase( true );
    assertTrue( nativeMeta.isQuoteAllFields() );
    assertTrue( nativeMeta.isForcingIdentifiersToLowerCase() );
    assertTrue( nativeMeta.isForcingIdentifiersToUpperCase() );
    nativeMeta.setUsingDoubleDecimalAsSchemaTableSeparator( true );
    assertTrue( nativeMeta.isUsingDoubleDecimalAsSchemaTableSeparator() );
    nativeMeta.setPreferredSchemaName( "FOO" );
    assertEquals( "FOO", nativeMeta.getPreferredSchemaName() );
  }

  private int rowCnt = 0;

  @Test
  public void testCheckIndexExists() throws Exception {
    Database db = Mockito.mock( Database.class );
    ResultSet rs = Mockito.mock( ResultSet.class );
    DatabaseMetaData dmd = Mockito.mock( DatabaseMetaData.class );
    DatabaseMeta dm = Mockito.mock( DatabaseMeta.class );
    Mockito.when( dm.getQuotedSchemaTableCombination( "", "FOO" ) ).thenReturn( "FOO" );
    Mockito.when( rs.next() ).thenAnswer( new Answer<Boolean>() {
      public Boolean answer( InvocationOnMock invocation ) throws Throwable {
        rowCnt++;
        return new Boolean( rowCnt < 3 );
      }
    } );
    Mockito.when( db.getDatabaseMetaData() ).thenReturn( dmd );
    Mockito.when( dmd.getIndexInfo( null, null, "FOO", false, true ) ).thenReturn( rs );
    Mockito.when( rs.getString( "COLUMN_NAME" ) ).thenAnswer( new Answer<String>() {
      @Override
      public String answer( InvocationOnMock invocation ) throws Throwable {
        if ( rowCnt == 1 ) {
          return "ROW1COL2";
        } else if ( rowCnt == 2 ) {
          return "ROW2COL2";
        } else {
          return null;
        }
      }
    } );
    Mockito.when( db.getDatabaseMeta() ).thenReturn( dm );
    assertTrue( odbcMeta.checkIndexExists( db, "", "FOO", new String[] { "ROW1COL2", "ROW2COL2" } ) );
    assertFalse( odbcMeta.checkIndexExists( db, "", "FOO", new String[] { "ROW2COL2", "NOTTHERE" } ) );
    assertFalse( odbcMeta.checkIndexExists( db, "", "FOO", new String[] { "NOTTHERE", "ROW1COL2" } ) );

  }

}
