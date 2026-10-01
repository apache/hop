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

package org.apache.hop.databases.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.database.types.ServerInfo;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * MySQL's VECTOR column, and the binding that makes it writable.
 *
 * <p>Both were checked against a MySQL 9.2 server: an INSERT of '[1,2,3]' into a VECTOR column is
 * refused with "Value of type 'string, size: 7' cannot be converted to 'vector' type", while the
 * same value sent as little endian float32 bytes is accepted and reads back as the vector it was.
 */
class MySqlVectorTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private String definitionOf(MySqlDatabaseMeta database, int dimension) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(database);
    IValueMeta vector = new VectorValueMeta("EMBEDDING", dimension);
    return databaseMeta.getFieldDefinition(vector, null, null, false, false, false).trim();
  }

  @Test
  void aKnownDimensionIsWrittenIntoTheColumn() {
    assertEquals("VECTOR(1536)", definitionOf(new MySqlDatabaseMeta(), 1536));
  }

  @Test
  void anUnknownDimensionFallsBackToText() {
    // MySQL has no spelling for a vector of unknown length, so the column has to be one that can
    // hold the canonical text form instead. It must not be sized from the dimension.
    assertEquals("LONGTEXT", definitionOf(new MySqlDatabaseMeta(), 0));
    assertEquals("LONGTEXT", definitionOf(new MySqlDatabaseMeta(), -1));
  }

  @Test
  void aServerOlderThanNineHasNoVectorType() {
    MySqlDatabaseMeta database = new MySqlDatabaseMeta();
    database.setServerInfo(new ServerInfo(8, 4, Set.of("VARCHAR")));

    assertFalse(database.isColumnTypeAvailable("VECTOR"));
    assertEquals("LONGTEXT", definitionOf(database, 1536));
  }

  @Test
  void theBindingWritesTheStorageFormat() throws SQLException, HopValueException {
    IValueMeta vector = new VectorValueMeta("EMBEDDING", 3);
    IValueBinding binding = DatabaseTypeMapper.getBinding(new MySqlDatabaseMeta(), vector);
    assertNotNull(binding, "a sized vector needs the binding, or the INSERT is refused");

    PreparedStatement statement = mock(PreparedStatement.class);
    binding.write(new MySqlDatabaseMeta(), vector, statement, 1, new float[] {0.1f, 0.2f, 0.3f});

    ArgumentCaptor<byte[]> written = ArgumentCaptor.forClass(byte[].class);
    verify(statement).setBytes(org.mockito.ArgumentMatchers.eq(1), written.capture());

    ByteBuffer buffer = ByteBuffer.wrap(written.getValue()).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(12, written.getValue().length, "four bytes per dimension");
    assertEquals(0.1f, buffer.getFloat());
    assertEquals(0.2f, buffer.getFloat());
    assertEquals(0.3f, buffer.getFloat());
  }

  @Test
  void theBindingWritesNullAsNull() throws SQLException, HopValueException {
    IValueMeta vector = new VectorValueMeta("EMBEDDING", 3);
    IValueBinding binding = DatabaseTypeMapper.getBinding(new MySqlDatabaseMeta(), vector);

    PreparedStatement statement = mock(PreparedStatement.class);
    binding.write(new MySqlDatabaseMeta(), vector, statement, 1, null);

    verify(statement).setNull(1, Types.VARBINARY);
  }

  @Test
  void aVectorGoingIntoATextColumnIsStillWrittenAsText() {
    // The column and the binding turn on the same condition. Without a dimension the column is
    // text, so the value has to stay text as well, which means no binding at all.
    IValueMeta unsized = new VectorValueMeta("EMBEDDING", 0);
    assertNull(DatabaseTypeMapper.getBinding(new MySqlDatabaseMeta(), unsized));
  }

  @Test
  void dorisAndMariaDbDoNotInheritTheMySqlVectorColumn() {
    // Doris has no VECTOR type at all, and MariaDB's is its own and unverified here. Neither may
    // pick up MySQL's by way of extending its dialect.
    assertTrue(
        MySqlDatabaseMeta.BASE_TYPE_RULES.stream()
            .noneMatch(
                rule ->
                    rule.getColumnType(
                            null,
                            new MySqlDatabaseMeta(),
                            new VectorValueMeta("EMBEDDING", 1536),
                            null)
                        != null),
        "the shared MySQL rules must not contain a vector column type");
  }

  /** A vector field without the plugin that defines the type. */
  private static final class VectorValueMeta extends ValueMetaBase {
    private VectorValueMeta(String name, int dimension) {
      super(name, IValueMeta.TYPE_VECTOR, dimension, 0);
    }

    @Override
    public Object convertData(IValueMeta meta, Object data) {
      // The real value type parses its own text form; the binding only needs the floats.
      return data;
    }
  }
}
