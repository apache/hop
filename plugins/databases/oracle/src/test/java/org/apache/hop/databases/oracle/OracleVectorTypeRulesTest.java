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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.types.ServerInfo;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Oracle's VECTOR column.
 *
 * <p>Both spellings were checked against an Oracle Free 23 server: VECTOR(3, FLOAT32) and VECTOR(*,
 * *) are accepted, a text literal converts into either, and the unsized column holds vectors of
 * differing dimensions.
 */
class OracleVectorTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private String definitionOf(OracleDatabaseMeta database, int dimension) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(database);
    IValueMeta vector = new VectorValueMeta("EMBEDDING", dimension);
    return databaseMeta.getFieldDefinition(vector, null, null, false, false, false).trim();
  }

  @Test
  void aKnownDimensionIsWrittenWithItsFormat() {
    assertEquals("VECTOR(1536, FLOAT32)", definitionOf(new OracleDatabaseMeta(), 1536));
  }

  @Test
  void anUnknownDimensionUsesOraclesOwnWildcard() {
    assertEquals("VECTOR(*, *)", definitionOf(new OracleDatabaseMeta(), 0));
    assertEquals("VECTOR(*, *)", definitionOf(new OracleDatabaseMeta(), -1));
  }

  @Test
  void aServerOlderThan23HasNoVectorType() {
    OracleDatabaseMeta database = new OracleDatabaseMeta();
    database.setServerInfo(new ServerInfo(19, 0, Set.of("VARCHAR2", "CLOB")));

    assertFalse(database.isColumnTypeAvailable("VECTOR"));
    assertEquals("CLOB", definitionOf(database, 1536));
  }

  @Test
  void a23aiServerKeepsTheVectorColumn() {
    OracleDatabaseMeta database = new OracleDatabaseMeta();
    database.setServerInfo(new ServerInfo(23, 0, Set.of("VARCHAR2", "VECTOR")));

    assertTrue(database.isColumnTypeAvailable("VECTOR"));
    assertEquals("VECTOR(768, FLOAT32)", definitionOf(database, 768));
  }

  private static final class VectorValueMeta extends ValueMetaBase {
    private VectorValueMeta(String name, int dimension) {
      super(name, IValueMeta.TYPE_VECTOR, dimension, 0);
    }
  }
}
