/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Which JDBC call Oracle uses to write a string, per column type.
 *
 * <p>The column type comes from the statement's own parameter metadata, which is what the Oracle
 * driver fills in by parsing the SQL and describing the target table. Every case goes through
 * {@link DatabaseTypeMapper#getBinding}, the same lookup {@code Database.setValue} does, so these
 * also cover the dialect actually declaring the binding.
 */
class OraclePreparedStatementBindingTest {

  private static final String LOG_FIELD = "LOG_FIELD";

  private OracleDatabaseMeta oracleDatabaseMeta;
  private PreparedStatement preparedStatementMock;
  private ParameterMetaData parameterMetaData;

  @BeforeEach
  void setUp() throws SQLException {
    oracleDatabaseMeta = new OracleDatabaseMeta();
    preparedStatementMock = mock(PreparedStatement.class);
    parameterMetaData = mock(ParameterMetaData.class);
    when(preparedStatementMock.getParameterMetaData()).thenReturn(parameterMetaData);
    when(parameterMetaData.getParameterCount()).thenReturn(1);
  }

  /** What the driver describes parameter 1 as. */
  private void column(int sqlType, String typeName) throws SQLException {
    when(parameterMetaData.getParameterType(1)).thenReturn(sqlType);
    when(parameterMetaData.getParameterTypeName(1)).thenReturn(typeName);
  }

  /** Binds through the declared rule, the way the insert path reaches it. */
  private void write(IValueMeta valueMeta, Object value) throws Exception {
    IValueBinding binding = DatabaseTypeMapper.getBinding(oracleDatabaseMeta, valueMeta);
    assertNotNull(binding, "Oracle should declare a binding for strings");
    binding.write(oracleDatabaseMeta, valueMeta, preparedStatementMock, 1, value);
  }

  @Test
  void testVarchar2UsesSetString() throws Exception {
    column(Types.VARCHAR, "VARCHAR2");
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, 20, 0), data);

    verify(preparedStatementMock, times(1)).setString(1, data);
    verify(preparedStatementMock, never()).setNString(anyInt(), any());
    verify(preparedStatementMock, never()).setCharacterStream(anyInt(), any(), anyLong());
  }

  @Test
  void testNvarchar2UsesSetNString() throws Exception {
    column(Types.NVARCHAR, "NVARCHAR2");
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, 20, 0), data);

    verify(preparedStatementMock, times(1)).setNString(1, data);
    verify(preparedStatementMock, never()).setString(anyInt(), any());
    verify(preparedStatementMock, never()).setCharacterStream(anyInt(), any(), anyLong());
  }

  @Test
  void testNcharUsesSetNString() throws Exception {
    column(Types.NCHAR, "NCHAR");
    String data = "ab";
    write(new ValueMetaString(LOG_FIELD, 2, 0), data);

    verify(preparedStatementMock, times(1)).setNString(1, data);
  }

  /** A driver that reports the national type only by name is still understood. */
  @Test
  void testNationalTypeNameOverridesAVarcharCode() throws Exception {
    column(Types.VARCHAR, "NVARCHAR2");
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, 20, 0), data);

    verify(preparedStatementMock, times(1)).setNString(1, data);
  }

  @Test
  void testClobUsesCharacterStream() throws Exception {
    column(Types.CLOB, "CLOB");
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, DatabaseMeta.CLOB_LENGTH, 0), data);

    verify(preparedStatementMock, times(1)).setCharacterStream(anyInt(), any(), anyLong());
    verify(preparedStatementMock, never()).setString(anyInt(), any());
  }

  @Test
  void testNclobUsesNCharacterStream() throws Exception {
    column(Types.NCLOB, "NCLOB");
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, DatabaseMeta.CLOB_LENGTH, 0), data);

    verify(preparedStatementMock, times(1)).setNCharacterStream(anyInt(), any(), anyLong());
    verify(preparedStatementMock, never()).setString(anyInt(), any());
  }

  /** The same for a LOB, where the column size Oracle reports is 4000 whatever the value holds. */
  @Test
  void testLongClobIsWrittenWhole() throws Exception {
    column(Types.NCLOB, "NCLOB");
    String data = StringUtils.repeat("*", 7000);
    write(new ValueMetaString(LOG_FIELD, 4000, 0), data);

    ArgumentCaptor<Long> length = ArgumentCaptor.forClass(Long.class);
    verify(preparedStatementMock).setNCharacterStream(anyInt(), any(), length.capture());
    assertEquals(7000L, length.getValue());
  }

  /**
   * The value is written whole, however wide the column is: fitting it is Oracle's business, and it
   * says so with ORA-12899 rather than having Hop quietly drop the overflow.
   */
  @Test
  void testValuesAreWrittenWholeRatherThanCutToTheColumnWidth() throws Exception {
    column(Types.NVARCHAR, "NVARCHAR2");
    String data = StringUtils.repeat("*", 100);
    write(new ValueMetaString(LOG_FIELD, 20, 0), data);

    verify(preparedStatementMock, times(1)).setNString(1, data);
  }

  /** A null is still a null: the binding does what ValueMetaBase did, rather than streaming it. */
  @Test
  void testNullBindsAsNullVarchar() throws Exception {
    column(Types.NVARCHAR, "NVARCHAR2");
    write(new ValueMetaString(LOG_FIELD, 20, 0), null);

    verify(preparedStatementMock, times(1)).setNull(1, Types.VARCHAR);
    verify(preparedStatementMock, never()).setNString(anyInt(), any());
  }

  /**
   * A driver that cannot describe its parameters -- too old, or a statement its parser does not
   * take -- leaves the value written the way Hop always wrote it. A value out of a CLOB carries
   * CLOB_LENGTH wherever it is going; with no column type to say otherwise there is no reason to
   * believe the target is a LOB, and streaming into a VARCHAR2 is what raises ORA-01461 on a mixed
   * batch.
   */
  @Test
  void testUnknownColumnTypeFallsBackToSetString() throws Exception {
    when(preparedStatementMock.getParameterMetaData())
        .thenThrow(new SQLException("Unsupported feature"));
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, DatabaseMeta.CLOB_LENGTH, 0), data);

    verify(preparedStatementMock, times(1)).setString(1, data);
    verify(preparedStatementMock, never()).setCharacterStream(anyInt(), any(), anyLong());
    verify(preparedStatementMock, never()).setNCharacterStream(anyInt(), any(), anyLong());
  }

  /** A parameter the driver has no type for is bound as before; the others are unaffected. */
  @Test
  void testAParameterWithoutATypeFallsBackToSetString() throws Exception {
    when(parameterMetaData.getParameterType(1)).thenThrow(new SQLException("no type"));
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, 20, 0), data);

    verify(preparedStatementMock, times(1)).setString(1, data);
  }

  /** The statement is described once, however many rows go through it. */
  @Test
  void testColumnTypesAreAskedOncePerStatement() throws Exception {
    column(Types.NVARCHAR, "NVARCHAR2");
    IValueMeta valueMeta = new ValueMetaString(LOG_FIELD, 20, 0);
    write(valueMeta, "one");
    write(valueMeta, "two");
    write(valueMeta, "three");

    verify(preparedStatementMock, times(1)).getParameterMetaData();
    verify(parameterMetaData, times(1)).getParameterType(1);
    verify(preparedStatementMock, times(3)).setNString(anyInt(), any());
  }

  /** So is a failed description: an old driver is not asked again for every row. */
  @Test
  void testAFailedDescriptionIsNotRetriedPerRow() throws Exception {
    when(preparedStatementMock.getParameterMetaData())
        .thenThrow(new SQLException("Unsupported feature"));
    IValueMeta valueMeta = new ValueMetaString(LOG_FIELD, 20, 0);
    write(valueMeta, "one");
    write(valueMeta, "two");

    verify(preparedStatementMock, times(1)).getParameterMetaData();
    verify(preparedStatementMock, times(2)).setString(anyInt(), any());
  }

  /**
   * Reading was never broken, so the binding declines it and the caller falls back to the value
   * type's own handling.
   */
  @Test
  void testReadingIsLeftToTheValueType() {
    IValueBinding binding =
        DatabaseTypeMapper.getBinding(oracleDatabaseMeta, new ValueMetaString(LOG_FIELD, 20, 0));
    assertNotNull(binding);
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            binding.read(
                oracleDatabaseMeta,
                new ValueMetaString(LOG_FIELD, 20, 0),
                mock(ResultSet.class),
                1));
  }
}
