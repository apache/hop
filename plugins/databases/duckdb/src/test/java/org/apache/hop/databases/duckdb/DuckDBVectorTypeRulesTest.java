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

package org.apache.hop.databases.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * DuckDB writes an embedding as a float array.
 *
 * <p>Checked against DuckDB 1.4: both FLOAT[3] and FLOAT[] accept the canonical text form of a
 * vector through a prepared statement, so unlike MySQL and CrateDB no binding is needed.
 */
class DuckDBVectorTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private String definitionOf(int dimension) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DuckDBDatabaseMeta());
    IValueMeta vector = new VectorValueMeta("EMBEDDING", dimension);
    return databaseMeta.getFieldDefinition(vector, null, null, false, false, false).trim();
  }

  @Test
  void aKnownDimensionBecomesAFixedSizeArray() {
    assertEquals("FLOAT[1536]", definitionOf(1536));
  }

  @Test
  void anUnknownDimensionBecomesAPlainList() {
    assertEquals("FLOAT[]", definitionOf(0));
    assertEquals("FLOAT[]", definitionOf(-1));
  }

  private static final class VectorValueMeta extends ValueMetaBase {
    private VectorValueMeta(String name, int dimension) {
      super(name, IValueMeta.TYPE_VECTOR, dimension, 0);
    }
  }
}
