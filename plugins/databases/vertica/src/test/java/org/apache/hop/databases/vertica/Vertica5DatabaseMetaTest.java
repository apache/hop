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

package org.apache.hop.databases.vertica;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.IHopLoggingEventListener;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.*;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
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
  private IValueMeta valueMetaBase;
  private IVariables variables;


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
    variables = spy( new Variables() );

    valueMetaBase = ValueMetaFactory.createValueMeta( IValueMeta.TYPE_NONE );

    dbMeta = spy( new DatabaseMeta() );
    resultSet = mock( ResultSet.class );
  }

  @Test
  public void testOverridesToVerticaDatabaseMeta() throws Exception {
    Vertica5DatabaseMeta nativeMeta = new Vertica5DatabaseMeta();
    nativeMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );

    assertEquals( "com.vertica.jdbc.Driver", nativeMeta.getDriverClass() );
    assertFalse( nativeMeta.supportsTimeStampToDateConversion() );

    ResultSet resultSet = mock( ResultSet.class );
    ResultSetMetaData metaData = mock( ResultSetMetaData.class );
    when( resultSet.getMetaData() ).thenReturn( metaData );

    when( resultSet.getTimestamp( 1 ) ).thenReturn( new Timestamp( 65535 ) );
    when( resultSet.getTime( 2 ) ).thenReturn( new Time( 1000 ) );
    when( resultSet.getDate( 3 ) ).thenReturn( new Date( ( 65535 * 2 ) ) );
    ValueMetaTimestamp ts = new ValueMetaTimestamp( "FOO" );
    ts.setOriginalColumnType( Types.TIMESTAMP );
    ValueMetaDate tm = new ValueMetaDate( "BAR" );
    tm.setOriginalColumnType( Types.TIME );
    ValueMetaDate dt = new ValueMetaDate( "WIBBLE" );
    dt.setOriginalColumnType( Types.DATE );


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
    IDatabase iDatabase = new Vertica5DatabaseMeta();
    dbMeta.setIDatabase( iDatabase );

    ResultSetMetaData metaData = mock( ResultSetMetaData.class );

    when( resultSet.getMetaData() ).thenReturn( metaData );
    when( metaData.getColumnType( binaryColumnIndex ) ).thenReturn( Types.BINARY );
    when( metaData.getPrecision( binaryColumnIndex ) ).thenReturn( expectedBinarylength );
    when( metaData.getColumnDisplaySize( binaryColumnIndex ) ).thenReturn( expectedBinarylength * 2 );

    when( metaData.getColumnType( varbinaryColumnIndex ) ).thenReturn( Types.BINARY );
    when( metaData.getPrecision( varbinaryColumnIndex ) ).thenReturn( expectedVarBinarylength );
    when( metaData.getColumnDisplaySize( varbinaryColumnIndex ) ).thenReturn( expectedVarBinarylength * 2 );

    // get value meta for binary type
    IValueMeta binaryValueMeta =
      obj.getValueFromSqlType( variables, dbMeta, TEST_NAME, metaData, binaryColumnIndex, false, false );
    assertNotNull( binaryValueMeta );
    assertTrue( TEST_NAME.equals( binaryValueMeta.getName() ) );
    assertTrue( IValueMeta.TYPE_BINARY == binaryValueMeta.getType() );
    assertTrue( expectedBinarylength == binaryValueMeta.getLength() );
    assertFalse( binaryValueMeta.isLargeTextField() );

    // get value meta for varbinary type
    IValueMeta varbinaryValueMeta =
      obj.getValueFromSqlType( variables, dbMeta, TEST_NAME, metaData, varbinaryColumnIndex, false, false );
    assertNotNull( varbinaryValueMeta );
    assertTrue( TEST_NAME.equals( varbinaryValueMeta.getName() ) );
    assertTrue( IValueMeta.TYPE_BINARY == varbinaryValueMeta.getType() );
    assertTrue( expectedVarBinarylength == varbinaryValueMeta.getLength() );
    assertFalse( varbinaryValueMeta.isLargeTextField() );

  }

  @Test
  public void testVerticaTimeType() throws Exception {
    // PDI-12244
    ResultSetMetaData metaData = mock( ResultSetMetaData.class );
    IValueMeta iValueMeta = mock( ValueMetaInternetAddress.class );

    when( resultSet.getMetaData() ).thenReturn( metaData );
    when( metaData.getColumnType( 1 ) ).thenReturn( Types.TIME );
    when( resultSet.getTime( 1 ) ).thenReturn( new Time( 0 ) );
    when( iValueMeta.getOriginalColumnType() ).thenReturn( Types.TIME );
    when( iValueMeta.getType() ).thenReturn( IValueMeta.TYPE_DATE );

    IDatabase iDatabase = new Vertica5DatabaseMeta();
    Object ret = iDatabase.getValueFromResultSet( resultSet, iValueMeta, 0 );
    assertEquals( new Time( 0 ), ret );
  }


  private class StoreLoggingEventListener implements IHopLoggingEventListener {

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
