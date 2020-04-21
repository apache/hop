/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.databases.postgresql;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.*;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.*;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.*;
import org.mockito.Spy;
import org.owasp.encoder.Encode;
import org.w3c.dom.Node;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class PostgreSQLValueMetaBaseTest {
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
  private IValueMeta valueMetaBase;

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

    valueMetaBase = ValueMetaFactory.createValueMeta( IValueMeta.TYPE_NONE );

    dbMeta = spy( new DatabaseMeta() );
    resultSet = mock( ResultSet.class );
  }

  @After
  public void tearDown() {
    HopLogStore.getAppender().removeLoggingEventListener( listener );
    listener = new StoreLoggingEventListener();
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


  /**
   * When data is shorter than value meta length all is good. Values well bellow DB max text field length.
   */
  @Test
  public void test_PDI_17126_Postgres() throws Exception {
    String data = StringUtils.repeat( "*", 10 );
    initValueMeta( new PostgreSQLDatabaseMeta(), 20, data );

    verify( preparedStatementMock, times( 1 ) ).setString( 0, data );
  }

  /**
   * When data is longer than value meta length all is good as well. Values well bellow DB max text field length.
   */
  @Test
  public void test_Pdi_17126_postgres_DataLongerThanMetaLength() throws Exception {
    String data = StringUtils.repeat( "*", 20 );
    initValueMeta( new PostgreSQLDatabaseMeta(), 10, data );

    verify( preparedStatementMock, times( 1 ) ).setString( 0, data );
  }

  /**
   * Only truncate when the data is larger that what is supported by the DB.
   * For test purposes we're mocking it at 1KB instead of the real value which is 2GB for PostgreSQL
   */
  @Test
  public void test_Pdi_17126_postgres_truncate() throws Exception {
    List<HopLoggingEvent> events = listener.getEvents();
    assertEquals( 0, events.size() );

    databaseMetaSpy.setIDatabase( new PostgreSQLDatabaseMeta() );
    doReturn( 1024 ).when( databaseMetaSpy ).getMaxTextFieldLength();
    doReturn( false ).when( databaseMetaSpy ).supportsSetCharacterStream();

    String data = StringUtils.repeat( "*", 2048 );

    ValueMetaBase valueMetaString = new ValueMetaBase( LOG_FIELD, IValueMeta.TYPE_STRING, 2048, 0 );
    valueMetaString.setPreparedStatementValue( databaseMetaSpy, preparedStatementMock, 0, data );

    verify( preparedStatementMock, never() ).setString( 0, data );
    verify( preparedStatementMock, times( 1 ) ).setString( anyInt(), anyString() );

    // check that truncated string was logged
    assertEquals( 1, events.size() );
    assertEquals( "ValueMetaBase - Truncating 1024 symbols of original message in 'LOG_FIELD' field",
      events.get( 0 ).getMessage().toString() );
  }

  private void initValueMeta( BaseDatabaseMeta dbMeta, int length, Object data ) throws HopDatabaseException {
    ValueMetaBase valueMetaString = new ValueMetaBase( LOG_FIELD, IValueMeta.TYPE_STRING, length, 0 );
    databaseMetaSpy.setIDatabase( dbMeta );
    valueMetaString.setPreparedStatementValue( databaseMetaSpy, preparedStatementMock, 0, data );
  }


  @Test
  public void testMetdataPreviewSqlNumericWithUndefinedSizeUsingPostgesSQL() throws SQLException, HopDatabaseException {
    doReturn( Types.NUMERIC ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( 0 ).when( resultSet ).getInt( "COLUMN_SIZE" );
    doReturn( mock( Object.class ) ).when( resultSet ).getObject( "DECIMAL_DIGITS" );
    doReturn( 0 ).when( resultSet ).getInt( "DECIMAL_DIGITS" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertFalse( valueMeta.isBigNumber() ); // TODO: VALIDATE!
    assertEquals( 0, valueMeta.getPrecision() ); // TODO: VALIDATE!
    assertEquals( 0, valueMeta.getLength() );// TODO: VALIDATE!
  }

  @Test
  public void testMetdataPreviewSqlBinaryToHopBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.BINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
  }

  @Test
  public void testMetdataPreviewSqlBlobToHopBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.BLOB ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
    assertTrue( valueMeta.isBinary() );
  }

  @Test
  public void testMetdataPreviewSqlVarBinaryToHopBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.VARBINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
  }

  @Test
  public void testMetdataPreviewSqlLongVarBinaryToHopBinary() throws SQLException, HopDatabaseException {
    doReturn( Types.LONGVARBINARY ).when( resultSet ).getInt( "DATA_TYPE" );
    doReturn( mock( PostgreSQLDatabaseMeta.class ) ).when( dbMeta ).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
    assertTrue( valueMeta.isBinary() );
  }
}
