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

package org.apache.hop.core.row.value;

import org.apache.hop.core.database.*;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.*;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.*;
import org.mockito.Spy;

import java.sql.*;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class NettezaValueMetaBaseTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

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

    valueMetaBase = new ValueMetaBase( );
    dbMeta = spy( new DatabaseMeta() );
    resultSet = mock( ResultSet.class );
  }

  @After
  public void tearDown() {
    HopLogStore.getAppender().removeLoggingEventListener( listener );
    listener = new StoreLoggingEventListener();
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

  /**
   * PDI-10877 Table input step returns no data when pulling a timestamp column from IBM Netezza
   *
   * @throws Exception
   */
  @Ignore
  @Test
  public void testGetValueFromSqlTypeNetezza() throws Exception {
    ValueMetaBase obj = new ValueMetaBase();
    DatabaseInterface databaseInterface = new NetezzaDatabaseMeta();

    ResultSetMetaData metaData = mock( ResultSetMetaData.class );
    when( resultSet.getMetaData() ).thenReturn( metaData );

    when( metaData.getColumnType( 1 ) ).thenReturn( Types.DATE );
    when( metaData.getColumnType( 2 ) ).thenReturn( Types.TIME );

    obj.type = ValueMetaInterface.TYPE_DATE;
    // call to testing method
    obj.getValueFromResultSet( databaseInterface, resultSet, 0 );
    // for jdbc Date type getDate method called
    verify( resultSet, times( 1 ) ).getDate( anyInt() );

    obj.getValueFromResultSet( databaseInterface, resultSet, 1 );
    // for jdbc Time type getTime method called
    verify( resultSet, times( 1 ) ).getTime( anyInt() );
  }

}
