/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.HopLoggingEventListener;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Ignore;
import org.mockito.Spy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class GreenplumValueMetaBaseTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private static final String TEST_NAME = "TEST_NAME";
  private static final String LOG_FIELD = "LOG_FIELD";
  public static final int MAX_TEXT_FIELD_LEN = 5;

  // Get PKG from class under test
  private Class<?> PKG = ValueMetaBase.PKG;

  private class StoreLoggingEventListener implements HopLoggingEventListener {

    private List<HopLoggingEvent> events = new ArrayList<>();

    @Override
    public void eventAdded( HopLoggingEvent event ) {
      events.add( event );
    }

    public List<HopLoggingEvent> getEvents() {
      return events;
    }
  }

  private StoreLoggingEventListener listener;

  @Spy
  private DatabaseMeta databaseMetaSpy = spy( new DatabaseMeta() );
  private PreparedStatement preparedStatementMock = mock( PreparedStatement.class );
  private ResultSet resultSet;
  private DatabaseMeta dbMeta;
  private ValueMetaBase valueMetaBase;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.addPluginType( DatabasePluginType.getInstance() );
    PluginRegistry.init();
    HopLogStore.init();
  }

  @Before
  public void setUp() {
    listener = new StoreLoggingEventListener();
    HopLogStore.getAppender().addLoggingEventListener( listener );

    valueMetaBase = new ValueMetaBase();
    dbMeta = spy( new DatabaseMeta() );
    resultSet = mock( ResultSet.class );
  }

  @After
  public void tearDown() {
    HopLogStore.getAppender().removeLoggingEventListener( listener );
    listener = new StoreLoggingEventListener();
  }



  @Test
  public void testMetdataPreviewSqlRealToPentahoNumber() throws SQLException, HopDatabaseException {
    doReturn( Types.REAL ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 3 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 2 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isNumber() );
    assertEquals( 2, valueMeta.getPrecision() );
    assertEquals( 3, valueMeta.getLength() );
  }

  @Ignore
  @Test
  public void testMetdataPreviewSqlNumericWithUndefinedSizeUsingGreenplum() throws SQLException, HopDatabaseException {
    doReturn( Types.NUMERIC ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 0 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 0 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    doReturn( mock( GreenplumDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBigNumber() );
    assertEquals( -1, valueMeta.getPrecision() );
    assertEquals( -1, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlNumericWithStrictBigNumberInterpretationUsingOracle() throws SQLException, HopDatabaseException {
    doReturn( Types.NUMERIC ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 38 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 0 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    doReturn( mock( OracleDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    when( ( (OracleDatabaseMeta) dbMeta.getDatabaseInterface() ).strictBigNumberInterpretation() ).thenReturn( true );
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBigNumber() );
  }

  @Test
  public void testMetdataPreviewSqlNumericWithoutStrictBigNumberInterpretationUsingOracle() throws SQLException, HopDatabaseException {
    doReturn( Types.NUMERIC ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 38 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 0 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    doReturn( mock( OracleDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    when( ( (OracleDatabaseMeta) dbMeta.getDatabaseInterface() ).strictBigNumberInterpretation() ).thenReturn( false );
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isInteger() );
  }

  @Test
  public void testMetdataPreviewSqlTimestampToPentahoDate() throws SQLException, HopDatabaseException {
    doReturn( Types.TIMESTAMP ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 19 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    doReturn( mock( OracleDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    doReturn( true ).when( dbMeta ).supportsTimestampDataType();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isDate() );
    assertEquals( -1, valueMeta.getPrecision() );
    assertEquals( 19, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewUnsupportedSqlTimestamp() throws SQLException, HopDatabaseException {
    doReturn( Types.TIMESTAMP ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 19 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    doReturn( false ).when( dbMeta ).supportsTimestampDataType();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( !valueMeta.isDate() );
  }

  @Test
  public void testMetdataPreviewSqlTimeToPentahoDate() throws SQLException, HopDatabaseException {
    doReturn( Types.TIME ).when( resultSet ).getInt( "DATA_TYPE" );
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isDate() );
  }

  @Test
  public void testMetdataPreviewSqlTimeToPentahoIntegerUsingMySQLVariant() throws SQLException, HopDatabaseException {
    doReturn( Types.TIME ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( MySQLDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    doReturn( true ).when( dbMeta ).isMySQLVariant();
    doReturn( mock( Properties.class ) ).when( dbMeta ).getConnectionProperties();
    when( dbMeta.getConnectionProperties().getProperty( "yearIsDateType" ) ).thenReturn( "false" );
    doReturn( "YEAR" ).when( resultSet ).getString( "TYPE_NAME" );
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isInteger() );
    assertEquals( 0, valueMeta.getPrecision() );
    assertEquals( 4, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlBooleanToPentahoBoolean() throws SQLException, HopDatabaseException {
    doReturn( Types.BOOLEAN ).when( resultSet ).getInt( "DATA_TYPE" );
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBoolean() );
  }

  @Test
  public void testMetdataPreviewSqlBitToPentahoBoolean() throws SQLException, HopDatabaseException {
    doReturn( Types.BIT ).when( resultSet ).getInt( "DATA_TYPE" );
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBoolean() );
  }

  @Test
  public void testMetdataPreviewSqlBinaryToPentahoBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.BINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
  }

  @Test
  public void testMetdataPreviewSqlBlobToPentahoBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.BLOB ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
    assertTrue( valueMeta.isBinary() );
  }

  @Test
  public void testMetdataPreviewSqlVarBinaryToPentahoBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.VARBINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
  }

  @Test
  public void testMetdataPreviewSqlVarBinaryToPentahoStringUsingOracle() throws SQLException, HopDatabaseException {
    doReturn( Types.VARBINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 16 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( OracleDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
    assertEquals( 16, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlVarBinaryToPentahoBinaryUsingMySQLVariant() throws SQLException, HopDatabaseException {
    doReturn( Types.VARBINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 16 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( MySQLDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    doReturn( true ).when( dbMeta ).isMySQLVariant();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
    assertEquals( -1, valueMeta.getLength() );
  }

  @Test
  public void testMetdataPreviewSqlLongVarBinaryToPentahoBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.LONGVARBINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
  }

  @Test
  public void testMetdataPreviewSqlLongVarBinaryToPentahoStringUsingOracle() throws SQLException, HopDatabaseException {
    doReturn( Types.LONGVARBINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( OracleDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isString() );
  }

  @Test
  public void testMetdataPreviewSqlDoubleToPentahoNumberUsingMySQL() throws SQLException, HopDatabaseException {
    doReturn( Types.DOUBLE ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 22 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( MySQLDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
    doReturn( true ).when( dbMeta ).isMySQLVariant();
    ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isNumber() );
    assertEquals( -1, valueMeta.getLength() );
  }
}
