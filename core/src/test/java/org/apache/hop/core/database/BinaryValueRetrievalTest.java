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
package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseTypeRuleRegistry;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Which JDBC getter a binary column is read with.
 *
 * <p>Hop has one binary value type, so BINARY, VARBINARY, LONGVARBINARY and BLOB all become {@link
 * IValueMeta#TYPE_BINARY} and only the JDBC type the driver reported still says how to fetch them.
 * The specification maps BLOB to {@code java.sql.Blob} and the other three to {@code byte[]}.
 *
 * <p>This used to be decided by {@code isSupportsGetBlob()}, one flag for the whole connection,
 * which fetched VARBINARY columns as Blobs on every dialect that did not opt out. Issue #8207.
 */
class BinaryValueRetrievalTest {

  private static final byte[] BYTES = {1, 2, 3};

  /** A dialect with no opinion, which is what a Generic connection is. */
  @DatabaseMetaPlugin(type = "BINARY_STANDARD", typeDescription = "Standard binary dialect")
  static class StandardDialect extends NoneDatabaseMeta {}

  /** A dialect whose driver cannot serve a Blob, saying so the way DB2 does. */
  @DatabaseMetaPlugin(type = "BINARY_AS_BYTES", typeDescription = "Bytes only binary dialect")
  static class BytesOnlyDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules()
          .bind(
              IValueMeta.TYPE_BINARY,
              new IValueBinding() {
                @Override
                public Object read(
                    IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
                    throws SQLException {
                  return resultSet.getBytes(index);
                }

                @Override
                public void write(
                    IDatabase database,
                    IValueMeta valueMeta,
                    PreparedStatement preparedStatement,
                    int index,
                    Object value) {
                  throw new UnsupportedOperationException("This binding only reads values");
                }
              })
          .build();
    }
  }

  /** A dialect that has not migrated and still answers the deprecated flag. */
  @DatabaseMetaPlugin(type = "BINARY_LEGACY_FLAG", typeDescription = "Legacy flag dialect")
  static class LegacyFlagDialect extends NoneDatabaseMeta {
    @Override
    @Deprecated(since = "2.20")
    public boolean isSupportsGetBlob() {
      return false;
    }
  }

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    // Binding rules are cached per dialect class.
    DatabaseTypeRuleRegistry.clearCache();
  }

  private static IValueMeta binaryColumn(int originalColumnType) {
    IValueMeta valueMeta = new ValueMetaBinary("b");
    valueMeta.setOriginalColumnType(originalColumnType);
    return valueMeta;
  }

  /** A result set whose driver refuses the Blob conversion, the way SAP HANA's does. */
  private static ResultSet resultSetRefusingBlob() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getBytes(1)).thenReturn(BYTES);
    when(resultSet.getBlob(anyInt()))
        .thenThrow(
            new SQLException("Cannot convert SQL type VARBINARY to Java type java.sql.Blob"));
    return resultSet;
  }

  private static ResultSet resultSetServingBlob() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    Blob blob = mock(Blob.class);
    when(blob.length()).thenReturn((long) BYTES.length);
    when(blob.getBytes(1L, BYTES.length)).thenReturn(BYTES);
    when(resultSet.getBlob(1)).thenReturn(blob);
    when(resultSet.getBytes(1)).thenReturn(BYTES);
    return resultSet;
  }

  @Test
  void aVarbinaryColumnIsReadAsBytes() throws Exception {
    ResultSet resultSet = resultSetRefusingBlob();

    Object value =
        new StandardDialect().getValueFromResultSet(resultSet, binaryColumn(Types.VARBINARY), 0);

    assertArrayEquals(BYTES, (byte[]) value);
    verify(resultSet, never()).getBlob(anyInt());
  }

  @Test
  void aBinaryColumnIsReadAsBytes() throws Exception {
    ResultSet resultSet = resultSetRefusingBlob();

    Object value =
        new StandardDialect().getValueFromResultSet(resultSet, binaryColumn(Types.BINARY), 0);

    assertArrayEquals(BYTES, (byte[]) value);
    verify(resultSet, never()).getBlob(anyInt());
  }

  @Test
  void aLongVarbinaryColumnIsReadAsBytes() throws Exception {
    ResultSet resultSet = resultSetRefusingBlob();

    Object value =
        new StandardDialect()
            .getValueFromResultSet(resultSet, binaryColumn(Types.LONGVARBINARY), 0);

    assertArrayEquals(BYTES, (byte[]) value);
    verify(resultSet, never()).getBlob(anyInt());
  }

  @Test
  void aBlobColumnIsReadAsABlob() throws Exception {
    ResultSet resultSet = resultSetServingBlob();

    Object value =
        new StandardDialect().getValueFromResultSet(resultSet, binaryColumn(Types.BLOB), 0);

    assertArrayEquals(BYTES, (byte[]) value);
    verify(resultSet).getBlob(1);
    verify(resultSet, never()).getBytes(anyInt());
  }

  @Test
  void aNullBlobReadsAsNull() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getBlob(1)).thenReturn(null);

    assertNull(new StandardDialect().getValueFromResultSet(resultSet, binaryColumn(Types.BLOB), 0));
  }

  /**
   * A value meta that did not come from a result set has no JDBC type to go on, so nothing says the
   * column is a BLOB and the wider of the two getters is used.
   */
  @Test
  void aColumnOfUnknownJdbcTypeIsReadAsBytes() throws Exception {
    ResultSet resultSet = resultSetRefusingBlob();

    Object value =
        new StandardDialect().getValueFromResultSet(resultSet, new ValueMetaBinary("b"), 0);

    assertArrayEquals(BYTES, (byte[]) value);
    verify(resultSet, never()).getBlob(anyInt());
  }

  @Test
  void aDialectDeclaringBinaryAsBytesReadsEvenABlobColumnAsBytes() throws Exception {
    ResultSet resultSet = resultSetServingBlob();

    Object value =
        new BytesOnlyDialect().getValueFromResultSet(resultSet, binaryColumn(Types.BLOB), 0);

    assertArrayEquals(BYTES, (byte[]) value);
    verify(resultSet, never()).getBlob(anyInt());
  }

  /**
   * The deprecated flag is not consulted. Every dialect that answered it was working around the
   * defect above rather than a driver that cannot serve a Blob, so honouring it would keep them on
   * the wrong getter; one that really needs bytes declares a binding, as above.
   */
  @Test
  void theDeprecatedFlagIsIgnored() throws Exception {
    ResultSet resultSet = resultSetServingBlob();

    Object value =
        new LegacyFlagDialect().getValueFromResultSet(resultSet, binaryColumn(Types.BLOB), 0);

    assertArrayEquals(BYTES, (byte[]) value);
    verify(resultSet).getBlob(1);
    verify(resultSet, never()).getBytes(anyInt());
  }
}
