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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
 * <p>Every case goes through {@link DatabaseTypeMapper#getBinding}, the same lookup {@code
 * Database.setValue} does, so these also cover the dialect actually declaring the binding.
 */
class OraclePreparedStatementBindingTest {

  private static final String LOG_FIELD = "LOG_FIELD";

  private OracleDatabaseMeta oracleDatabaseMeta;
  private PreparedStatement preparedStatementMock;

  @BeforeEach
  void setUp() {
    oracleDatabaseMeta = new OracleDatabaseMeta();
    preparedStatementMock = mock(PreparedStatement.class);
  }

  /** Binds through the declared rule, the way the insert path reaches it. */
  private void write(IValueMeta valueMeta, Object value) throws Exception {
    IValueBinding binding = DatabaseTypeMapper.getBinding(oracleDatabaseMeta, valueMeta);
    assertNotNull(binding, "Oracle should declare a binding for strings");
    binding.write(oracleDatabaseMeta, valueMeta, preparedStatementMock, 0, value);
  }

  @Test
  void testOracleShortStringUsesSetString() throws Exception {
    String data = StringUtils.repeat("*", 10);
    write(new ValueMetaString(LOG_FIELD, 20, 0), data);

    verify(preparedStatementMock, times(1)).setString(0, data);
    verify(preparedStatementMock, never()).setCharacterStream(anyInt(), any(), anyLong());
  }

  @Test
  void testOracleLargeStringUsesSetString() throws Exception {
    String data = StringUtils.repeat("*", 2500);
    write(new ValueMetaString(LOG_FIELD, 4000, 0), data);

    verify(preparedStatementMock, times(1)).setString(0, data);
    verify(preparedStatementMock, never()).setCharacterStream(anyInt(), any(), anyLong());
  }

  @Test
  void testOracleNationalTypeUsesSetNString() throws Exception {
    String data = StringUtils.repeat("*", 10);
    ValueMetaString valueMetaString = new ValueMetaString(LOG_FIELD, 20, 0);
    valueMetaString.setOriginalColumnType(Types.NVARCHAR);
    valueMetaString.setOriginalColumnTypeName("NVARCHAR2");
    write(valueMetaString, data);

    verify(preparedStatementMock, times(1)).setNString(0, data);
    verify(preparedStatementMock, never()).setString(0, data);
    verify(preparedStatementMock, never()).setCharacterStream(anyInt(), any(), anyLong());
  }

  @Test
  void testOracleClobUsesCharacterStream() throws Exception {
    String data = StringUtils.repeat("*", 10);
    ValueMetaString valueMetaString = new ValueMetaString(LOG_FIELD, DatabaseMeta.CLOB_LENGTH, 0);
    valueMetaString.setOriginalColumnType(Types.CLOB);
    write(valueMetaString, data);

    verify(preparedStatementMock, times(1)).setCharacterStream(anyInt(), any(), anyLong());
    verify(preparedStatementMock, never()).setString(0, data);
  }

  @Test
  void testOracleNclobUsesNCharacterStream() throws Exception {
    String data = StringUtils.repeat("*", 10);
    ValueMetaString valueMetaString = new ValueMetaString(LOG_FIELD, DatabaseMeta.CLOB_LENGTH, 0);
    valueMetaString.setOriginalColumnType(Types.NCLOB);
    write(valueMetaString, data);

    verify(preparedStatementMock, times(1)).setNCharacterStream(anyInt(), any(), anyLong());
    verify(preparedStatementMock, never()).setString(0, data);
  }

  /** A null is still a null: the binding does what ValueMetaBase did, rather than streaming it. */
  @Test
  void testNullBindsAsNullVarchar() throws Exception {
    ValueMetaString valueMetaString = new ValueMetaString(LOG_FIELD, 20, 0);
    valueMetaString.setOriginalColumnType(Types.NVARCHAR);
    write(valueMetaString, null);

    verify(preparedStatementMock, times(1)).setNull(0, Types.VARCHAR);
    verify(preparedStatementMock, never()).setNString(anyInt(), any());
  }

  /**
   * The value is written whole, however wide the column is: fitting it is Oracle's business, and it
   * says so with ORA-12899 rather than having Hop quietly drop the overflow.
   */
  @Test
  void testValuesAreWrittenWholeRatherThanCutToTheColumnWidth() throws Exception {
    String data = StringUtils.repeat("*", 100);
    ValueMetaString valueMetaString = new ValueMetaString(LOG_FIELD, 20, 0);
    valueMetaString.setOriginalColumnType(Types.NVARCHAR);
    valueMetaString.setOriginalColumnTypeName("NVARCHAR2");
    write(valueMetaString, data);

    verify(preparedStatementMock, times(1)).setNString(0, data);
  }

  /** The same for a LOB, where the column size Oracle reports is 4000 whatever the value holds. */
  @Test
  void testLongClobIsWrittenWhole() throws Exception {
    String data = StringUtils.repeat("*", 7000);
    ValueMetaString valueMetaString = new ValueMetaString(LOG_FIELD, 4000, 0);
    valueMetaString.setOriginalColumnType(Types.NCLOB);
    valueMetaString.setOriginalColumnTypeName("NCLOB");
    write(valueMetaString, data);

    ArgumentCaptor<Long> length = ArgumentCaptor.forClass(Long.class);
    verify(preparedStatementMock).setNCharacterStream(anyInt(), any(), length.capture());
    assertEquals(7000L, length.getValue());
  }

  /**
   * A value out of a CLOB carries CLOB_LENGTH wherever it is going. With no column type to say
   * otherwise there is no reason to believe the target is a LOB, and streaming into a VARCHAR2 is
   * what raises ORA-01461 on a mixed batch, so it is written the way Hop wrote it before.
   */
  @Test
  void aClobLengthValueWithNoKnownColumnIsNotStreamed() throws Exception {
    String data = StringUtils.repeat("*", 10);
    ValueMetaString valueMetaString = new ValueMetaString(LOG_FIELD, DatabaseMeta.CLOB_LENGTH, 0);
    write(valueMetaString, data);

    verify(preparedStatementMock, times(1)).setString(0, data);
    verify(preparedStatementMock, never()).setCharacterStream(anyInt(), any(), anyLong());
    verify(preparedStatementMock, never()).setNCharacterStream(anyInt(), any(), anyLong());
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
