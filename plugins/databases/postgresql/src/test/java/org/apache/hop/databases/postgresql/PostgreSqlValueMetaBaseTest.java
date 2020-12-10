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

package org.apache.hop.databases.postgresql;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.IHopLoggingEventListener;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Spy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PostgreSqlValueMetaBaseTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  private static final String TEST_NAME = "TEST_NAME";
  private static final String LOG_FIELD = "LOG_FIELD";
  public static final int MAX_TEXT_FIELD_LEN = 5;

  // Get PKG from class under test
  private Class<?> PKG = ValueMetaBase.PKG;
  private StoreLoggingEventListener listener;

  @Spy private DatabaseMeta databaseMetaSpy = spy(new DatabaseMeta());
  private PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
  private ResultSet resultSet;
  private DatabaseMeta dbMeta;
  private IValueMeta valueMetaBase;
  private IVariables variables;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
    HopLogStore.init();
  }

  @Before
  public void setUp() throws HopPluginException {
    listener = new StoreLoggingEventListener();
    HopLogStore.getAppender().addLoggingEventListener(listener);

    valueMetaBase = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_NONE);

    dbMeta = spy(new DatabaseMeta());
    resultSet = mock(ResultSet.class);
    variables = spy(new Variables());
  }

  @After
  public void tearDown() {
    HopLogStore.getAppender().removeLoggingEventListener(listener);
    listener = new StoreLoggingEventListener();
  }

  private class StoreLoggingEventListener implements IHopLoggingEventListener {

    private List<HopLoggingEvent> events = new ArrayList<>();

    @Override
    public void eventAdded(HopLoggingEvent event) {
      events.add(event);
    }

    public List<HopLoggingEvent> getEvents() {
      return events;
    }
  }

  /**
   * When data is shorter than value meta length all is good. Values well bellow DB max text field
   * length.
   */
  @Test
  public void test_PDI_17126_Postgres() throws Exception {
    String data = StringUtils.repeat("*", 10);
    initValueMeta(new PostgreSqlDatabaseMeta(), 20, data);

    verify(preparedStatementMock, times(1)).setString(0, data);
  }

  /**
   * When data is longer than value meta length all is good as well. Values well bellow DB max text
   * field length.
   */
  @Test
  public void test_Pdi_17126_postgres_DataLongerThanMetaLength() throws Exception {
    String data = StringUtils.repeat("*", 20);
    initValueMeta(new PostgreSqlDatabaseMeta(), 10, data);

    verify(preparedStatementMock, times(1)).setString(0, data);
  }

  /**
   * Only truncate when the data is larger that what is supported by the DB. For test purposes we're
   * mocking it at 1KB instead of the real value which is 2GB for PostgreSQL
   */
  @Test
  public void test_Pdi_17126_postgres_truncate() throws Exception {
    List<HopLoggingEvent> events = listener.getEvents();
    assertEquals(0, events.size());

    databaseMetaSpy.setIDatabase(new PostgreSqlDatabaseMeta());
    doReturn(1024).when(databaseMetaSpy).getMaxTextFieldLength();
    doReturn(false).when(databaseMetaSpy).supportsSetCharacterStream();

    String data = StringUtils.repeat("*", 2048);

    ValueMetaBase valueMetaString = new ValueMetaBase(LOG_FIELD, IValueMeta.TYPE_STRING, 2048, 0);
    valueMetaString.setPreparedStatementValue(databaseMetaSpy, preparedStatementMock, 0, data);

    verify(preparedStatementMock, never()).setString(0, data);
    verify(preparedStatementMock, times(1)).setString(anyInt(), anyString());

    // check that truncated string was logged
    assertEquals(1, events.size());
    assertEquals(
        "ValueMetaBase - Truncating 1024 symbols of original message in 'LOG_FIELD' field",
        events.get(0).getMessage().toString());
  }

  private void initValueMeta(BaseDatabaseMeta dbMeta, int length, Object data)
      throws HopDatabaseException {
    ValueMetaBase valueMetaString = new ValueMetaBase(LOG_FIELD, IValueMeta.TYPE_STRING, length, 0);
    databaseMetaSpy.setIDatabase(dbMeta);
    valueMetaString.setPreparedStatementValue(databaseMetaSpy, preparedStatementMock, 0, data);
  }

  @Test
  public void testMetdataPreviewSqlNumericWithUndefinedSizeUsingPostgesSql()
      throws SQLException, HopDatabaseException {
    doReturn(Types.NUMERIC).when(resultSet).getInt("DATA_TYPE");
    doReturn(0).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(0).when(resultSet).getInt("DECIMAL_DIGITS");
    doReturn(mock(PostgreSqlDatabaseMeta.class)).when(dbMeta).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertFalse(valueMeta.isBigNumber()); // TODO: VALIDATE!
    assertEquals(0, valueMeta.getPrecision()); // TODO: VALIDATE!
    assertEquals(0, valueMeta.getLength()); // TODO: VALIDATE!
  }

  @Test
  public void testMetdataPreviewSqlBinaryToHopBinary() throws SQLException, HopDatabaseException {
    doReturn(Types.BINARY).when(resultSet).getInt("DATA_TYPE");
    doReturn(mock(PostgreSqlDatabaseMeta.class)).when(dbMeta).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBinary());
  }

  @Test
  public void testMetdataPreviewSqlBlobToHopBinary() throws SQLException, HopDatabaseException {
    doReturn(Types.BLOB).when(resultSet).getInt("DATA_TYPE");
    doReturn(mock(PostgreSqlDatabaseMeta.class)).when(dbMeta).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBinary());
    assertTrue(valueMeta.isBinary());
  }

  @Test
  public void testMetdataPreviewSqlVarBinaryToHopBinary()
      throws SQLException, HopDatabaseException {
    doReturn(Types.VARBINARY).when(resultSet).getInt("DATA_TYPE");
    doReturn(mock(PostgreSqlDatabaseMeta.class)).when(dbMeta).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBinary());
  }

  @Test
  public void testMetdataPreviewSqlLongVarBinaryToHopBinary()
      throws SQLException, HopDatabaseException {
    doReturn(Types.LONGVARBINARY).when(resultSet).getInt("DATA_TYPE");
    doReturn(mock(PostgreSqlDatabaseMeta.class)).when(dbMeta).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, dbMeta, resultSet);
    assertTrue(valueMeta.isBinary());
  }
}
