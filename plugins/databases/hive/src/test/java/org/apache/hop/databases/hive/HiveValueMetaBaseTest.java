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
 *
 */

package org.apache.hop.databases.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class HiveValueMetaBaseTest {
  protected static final String TEST_NAME = "TEST_NAME";
  protected static final String LOG_FIELD = "LOG_FIELD";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
  private ResultSet resultSet;
  private DatabaseMeta databaseMeta;
  private IValueMeta valueMetaBase;
  private IVariables variables;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
    // HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws HopPluginException {
    valueMetaBase = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_NONE);
    databaseMeta = spy(new DatabaseMeta());
    resultSet = mock(ResultSet.class);
    variables = spy(new Variables());
  }

  protected void initValueMeta(BaseDatabaseMeta dbMeta, int length, Object data)
      throws HopDatabaseException {
    IValueMeta valueMetaString = new ValueMetaString(LOG_FIELD, length, 0);
    databaseMeta.setIDatabase(dbMeta);
    valueMetaString.setPreparedStatementValue(databaseMeta, preparedStatementMock, 0, data);
  }

  @Test
  void testGetValueFromSqlTypeBinaryHive() throws Exception {

    final int binaryColumnIndex = 1;
    ValueMetaBase valueMetaBase = new ValueMetaBase();
    DatabaseMeta dbMeta = spy(new DatabaseMeta());
    IDatabase iDatabase = new HiveDatabaseMeta();
    dbMeta.setIDatabase(iDatabase);

    ResultSetMetaData metaData = mock(ResultSetMetaData.class);

    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnType(binaryColumnIndex)).thenReturn(Types.LONGVARBINARY);

    IValueMeta binaryValueMeta =
        valueMetaBase.getValueFromSqlType(
            variables, dbMeta, TEST_NAME, metaData, binaryColumnIndex, false, false);
    assertEquals(IValueMeta.TYPE_BINARY, binaryValueMeta.getType());
    assertTrue(binaryValueMeta.isBinary());
  }

  @Test
  void testMetaDataPreviewSqlVarBinaryToHopBinaryUsingHiveVariant()
      throws SQLException, HopDatabaseException {
    doReturn(Types.VARBINARY).when(resultSet).getInt("DATA_TYPE");
    doReturn(16).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(HiveDatabaseMeta.class)).when(databaseMeta).getIDatabase();
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, databaseMeta, resultSet);
    assertTrue(valueMeta.isBinary());
    assertEquals(-1, valueMeta.getLength());
  }
}
