/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.HopLoggingEventListener;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.*;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class Vertica5DatabaseMetaTest extends VerticaDatabaseMetaTest {
  @ClassRule
  public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private static final String TEST_NAME = "TEST_NAME";
  private static final String LOG_FIELD = "LOG_FIELD";
  public static final int MAX_TEXT_FIELD_LEN = 5;

  // Get PKG from class under test
  private Class<?> PKG = ValueMetaBase.PKG;
  private StoreLoggingEventListener listener;

  @Spy
  private DatabaseMeta databaseMetaSpy = spy( new DatabaseMeta() );
  private PreparedStatement preparedStatementMock = mock( PreparedStatement.class );
  private ResultSet resultSet;
  private DatabaseMeta dbMeta;
  private ValueMetaInterface valueMetaBase;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.addPluginType( DatabasePluginType.getInstance() );
    PluginRegistry.init();
    HopLogStore.init();
  }

  @Before
  public void setUp() throws HopPluginException {
    listener = new StoreLoggingEventListener();
    HopLogStore.getAppender().addLoggingEventListener( listener );

    valueMetaBase = ValueMetaFactory.createValueMeta( ValueMetaInterface.TYPE_NONE);

    dbMeta = spy( new DatabaseMeta() );
    resultSet = mock( ResultSet.class );
  }

  @Test
  public void testOverridesToVerticaDatabaseMeta() throws Exception {
    Vertica5DatabaseMeta nativeMeta = new Vertica5DatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    Vertica5DatabaseMeta odbcMeta = new Vertica5DatabaseMeta();
    odbcMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_ODBC );

    assertEquals( "com.vertica.jdbc.Driver", nativeMeta.getDriverClass() );
    assertEquals( "sun.jdbc.odbc.JdbcOdbcDriver", odbcMeta.getDriverClass() );
    assertFalse( nativeMeta.supportsTimeStampToDateConversion() );

    ResultSet resultSet = mock( ResultSet.class );
    ResultSetMetaData metaData = mock( ResultSetMetaData.class );
    when( resultSet.getMetaData() ).thenReturn( metaData );

    when( resultSet.getTimestamp( 1 ) ).thenReturn( new java.sql.Timestamp( 65535 ) );
    when( resultSet.getTime( 2 ) ).thenReturn( new java.sql.Time( 1000 ) );
    when( resultSet.getDate( 3 ) ).thenReturn( new java.sql.Date( ( 65535 * 2 ) ) );
    ValueMetaTimestamp ts = new ValueMetaTimestamp( "FOO" );
    ts.setOriginalColumnType( java.sql.Types.TIMESTAMP );
    ValueMetaDate tm = new ValueMetaDate( "BAR" );
    tm.setOriginalColumnType( java.sql.Types.TIME );
    ValueMetaDate dt = new ValueMetaDate( "WIBBLE" );
    dt.setOriginalColumnType( java.sql.Types.DATE );


    Object rtn = null;
    rtn = nativeMeta.getValueFromResultSet( resultSet, ts, 0 );
    assertNotNull( rtn );
    assertEquals( "java.sql.Timestamp", rtn.getClass().getName() );

    rtn = nativeMeta.getValueFromResultSet( resultSet, tm, 1 );
    assertNotNull( rtn );
    assertEquals( "java.sql.Time", rtn.getClass().getName() );

    rtn = nativeMeta.getValueFromResultSet( resultSet, dt, 2 );
    assertNotNull( rtn );
    assertEquals( "java.sql.Date", rtn.getClass().getName() );

    when( resultSet.wasNull() ).thenReturn( true );
    rtn = nativeMeta.getValueFromResultSet( resultSet, new ValueMetaString( "WOBBLE" ), 3 );
    assertNull( rtn );

    // Verify that getDate, getTime, and getTimestamp were respectively called once
    Mockito.verify( resultSet, Mockito.times( 1 ) ).getDate( Mockito.anyInt() );
    Mockito.verify( resultSet, Mockito.times( 1 ) ).getTime( Mockito.anyInt() );
    Mockito.verify( resultSet, Mockito.times( 1 ) ).getTimestamp( Mockito.anyInt() );

  }

  @Test
  public void testGetBinaryWithLength_WhenBinarySqlTypesOfVertica() throws Exception {
    final int binaryColumnIndex = 1;
    final int varbinaryColumnIndex = 2;
    final int expectedBinarylength = 1;
    final int expectedVarBinarylength = 80;

    ValueMetaBase obj = new ValueMetaBase();
    DatabaseMeta dbMeta = spy( new DatabaseMeta() );
    DatabaseInterface databaseInterface = new Vertica5DatabaseMeta();
    dbMeta.setDatabaseInterface( databaseInterface );

    ResultSetMetaData metaData = mock( ResultSetMetaData.class );

    when( resultSet.getMetaData() ).thenReturn( metaData );
    when( metaData.getColumnType( binaryColumnIndex ) ).thenReturn( Types.BINARY );
    when( metaData.getPrecision( binaryColumnIndex ) ).thenReturn( expectedBinarylength );
    when( metaData.getColumnDisplaySize( binaryColumnIndex ) ).thenReturn( expectedBinarylength * 2 );

    when( metaData.getColumnType( varbinaryColumnIndex ) ).thenReturn( Types.BINARY );
    when( metaData.getPrecision( varbinaryColumnIndex ) ).thenReturn( expectedVarBinarylength );
    when( metaData.getColumnDisplaySize( varbinaryColumnIndex ) ).thenReturn( expectedVarBinarylength * 2 );

    // get value meta for binary type
    ValueMetaInterface binaryValueMeta =
            obj.getValueFromSQLType( dbMeta, TEST_NAME, metaData, binaryColumnIndex, false, false );
    assertNotNull( binaryValueMeta );
    assertTrue( TEST_NAME.equals( binaryValueMeta.getName() ) );
    assertTrue( ValueMetaInterface.TYPE_BINARY == binaryValueMeta.getType() );
    assertTrue( expectedBinarylength == binaryValueMeta.getLength() );
    assertFalse( binaryValueMeta.isLargeTextField() );

    // get value meta for varbinary type
    ValueMetaInterface varbinaryValueMeta =
            obj.getValueFromSQLType( dbMeta, TEST_NAME, metaData, varbinaryColumnIndex, false, false );
    assertNotNull( varbinaryValueMeta );
    assertTrue( TEST_NAME.equals( varbinaryValueMeta.getName() ) );
    assertTrue( ValueMetaInterface.TYPE_BINARY == varbinaryValueMeta.getType() );
    assertTrue( expectedVarBinarylength == varbinaryValueMeta.getLength() );
    assertFalse( varbinaryValueMeta.isLargeTextField() );

  }

  @Test
  public void testVerticaTimeType() throws Exception {
    // PDI-12244
    ResultSetMetaData metaData = mock( ResultSetMetaData.class );
    ValueMetaInterface valueMetaInterface = mock( ValueMetaInternetAddress.class );

    when( resultSet.getMetaData() ).thenReturn( metaData );
    when( metaData.getColumnType( 1 ) ).thenReturn( Types.TIME );
    when( resultSet.getTime( 1 ) ).thenReturn( new Time( 0 ) );
    when( valueMetaInterface.getOriginalColumnType() ).thenReturn( Types.TIME );
    when( valueMetaInterface.getType() ).thenReturn( ValueMetaInterface.TYPE_DATE );

    DatabaseInterface databaseInterface = new Vertica5DatabaseMeta();
    Object ret = databaseInterface.getValueFromResultSet( resultSet, valueMetaInterface, 0 );
    assertEquals( new Time( 0 ), ret );
  }


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
}
