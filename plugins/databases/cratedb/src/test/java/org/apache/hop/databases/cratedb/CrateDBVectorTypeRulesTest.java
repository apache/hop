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

package org.apache.hop.databases.cratedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * CrateDB has a vector type of its own, and must not inherit pgvector's from the PostgreSQL dialect
 * it extends.
 *
 * <p>Checked against CrateDB 6.4: a FLOAT_VECTOR column refuses a text value with "Cannot convert
 * VALUES element ... of type `text` to `float_vector`" and accepts a float array.
 */
class CrateDBVectorTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private String definitionOf(int dimension) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new CrateDBDatabaseMeta());
    IValueMeta vector = new VectorValueMeta("EMBEDDING", dimension);
    return databaseMeta.getFieldDefinition(vector, null, null, false, false, false).trim();
  }

  @Test
  void aKnownDimensionBecomesAFloatVectorAndNotPgvectorsType() {
    assertEquals("FLOAT_VECTOR(1536)", definitionOf(1536));
  }

  @Test
  void anUnknownDimensionFallsBackToText() {
    // FLOAT_VECTOR takes no unsized form, and a dimension is not something to guess at.
    assertEquals("TEXT", definitionOf(0));
    assertEquals("TEXT", definitionOf(-1));
  }

  @Test
  void theBindingWritesThePrimitiveFloatArray() throws SQLException, HopValueException {
    // Not a boxed Float[]: the driver answers that with "Can't infer the SQL type to use for an
    // instance of [Ljava.lang.Float;", which is a runtime failure on the very first row.
    IValueMeta vector = new VectorValueMeta("EMBEDDING", 3);
    IValueBinding binding = DatabaseTypeMapper.getBinding(new CrateDBDatabaseMeta(), vector);
    assertNotNull(binding, "a sized vector needs the binding, or the INSERT is refused");

    PreparedStatement statement = mock(PreparedStatement.class);
    binding.write(new CrateDBDatabaseMeta(), vector, statement, 1, new float[] {0.1f, 0.2f});

    verify(statement).setObject(1, new float[] {0.1f, 0.2f});
  }

  private static final class VectorValueMeta extends ValueMetaBase {
    private VectorValueMeta(String name, int dimension) {
      super(name, IValueMeta.TYPE_VECTOR, dimension, 0);
    }

    @Override
    public Object convertData(IValueMeta meta, Object data) {
      return data;
    }
  }
}
