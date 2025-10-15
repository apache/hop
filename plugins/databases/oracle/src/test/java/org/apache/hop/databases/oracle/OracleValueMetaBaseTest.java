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

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class OracleValueMetaBaseTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private DatabaseMeta databaseMeta;
  private IValueMeta valueMetaBase;
  private ResultSet resultSet;
  private IVariables variables;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() throws HopException {
    valueMetaBase = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_NONE);
    databaseMeta = spy(DatabaseMeta.class);
    databaseMeta.setIDatabase(spy(OracleDatabaseMeta.class));
    resultSet = mock(ResultSet.class);
    variables = spy(new Variables());
  }

  @Test
  void testMetadataPreviewSqlVarBinaryToString() throws SQLException, HopDatabaseException {
    when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.VARBINARY);
    when(resultSet.getInt("COLUMN_SIZE")).thenReturn(16);

    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, databaseMeta, resultSet);
    assertTrue(valueMeta.isString());
    assertEquals(16, valueMeta.getLength());
  }

  @Test
  void testMetadataPreviewSqlLongVarBinaryToString() throws SQLException, HopDatabaseException {
    when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.LONGVARBINARY);

    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, databaseMeta, resultSet);
    assertTrue(valueMeta.isString());
  }

  @Test
  void testMetadataPreviewSqlNumericWithStrictBigNumberInterpretation()
      throws SQLException, HopDatabaseException {

    when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.NUMERIC);
    when(resultSet.getInt("COLUMN_SIZE")).thenReturn(38);
    when(resultSet.getInt("DECIMAL_DIGITS")).thenReturn(0);
    when(databaseMeta.getIDatabase().isStrictBigNumberInterpretation()).thenReturn(true);

    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, databaseMeta, resultSet);
    assertTrue(valueMeta.isBigNumber());
  }

  @Test
  void testMetadataPreviewSqlNumericWithoutStrictBigNumberInterpretation()
      throws SQLException, HopDatabaseException {
    when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.NUMERIC);
    when(resultSet.getInt("COLUMN_SIZE")).thenReturn(38);
    when(resultSet.getInt("DECIMAL_DIGITS")).thenReturn(0);
    when(databaseMeta.getIDatabase().isStrictBigNumberInterpretation()).thenReturn(false);

    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, databaseMeta, resultSet);
    assertTrue(valueMeta.isInteger());
  }

  @Test
  void testMetadataPreviewSqlTimestampToDate() throws SQLException, HopDatabaseException {
    when(resultSet.getInt("DATA_TYPE")).thenReturn(Types.TIMESTAMP);
    when(resultSet.getInt("DECIMAL_DIGITS")).thenReturn(19);
    when(resultSet.getObject("DECIMAL_DIGITS")).thenReturn(mock(Object.class));
    when(databaseMeta.supportsTimestampDataType()).thenReturn(true);

    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, databaseMeta, resultSet);
    assertTrue(valueMeta.isDate());
    assertEquals(-1, valueMeta.getPrecision());
    assertEquals(19, valueMeta.getLength());
  }

  @Test
  void testMetdataPreviewSqlDoubleWithTooBigLengthAndPrecision()
      throws SQLException, HopDatabaseException {
    doReturn(Types.DOUBLE).when(resultSet).getInt("DATA_TYPE");
    doReturn(128).when(resultSet).getInt("COLUMN_SIZE");
    doReturn(mock(Object.class)).when(resultSet).getObject("DECIMAL_DIGITS");
    doReturn(127).when(resultSet).getInt("DECIMAL_DIGITS");
    IValueMeta valueMeta = valueMetaBase.getMetadataPreview(variables, databaseMeta, resultSet);
    assertTrue(valueMeta.isBigNumber());
    assertEquals(-1, valueMeta.getPrecision());
    assertEquals(-1, valueMeta.getLength());
  }
}
