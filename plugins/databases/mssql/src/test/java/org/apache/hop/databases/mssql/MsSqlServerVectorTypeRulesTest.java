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

package org.apache.hop.databases.mssql;

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
 * SQL Server's VECTOR column, which arrived in 2025.
 *
 * <p>Checked against a SQL Server 2025 server, major version 17: VECTOR(3) is accepted and takes
 * the canonical text form of a vector as it stands, so no binding is needed, while a column
 * declared as plain VECTOR is refused with "Cannot find data type vector". A vector whose dimension
 * Hop does not know therefore has to go somewhere else.
 */
class MsSqlServerVectorTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private String definitionOf(MsSqlServerDatabaseMeta database, int dimension) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(database);
    IValueMeta vector = new VectorValueMeta("EMBEDDING", dimension);
    return databaseMeta.getFieldDefinition(vector, null, null, false, false, false).trim();
  }

  @Test
  void aKnownDimensionIsWrittenIntoTheColumn() {
    assertEquals("VECTOR(1536)", definitionOf(new MsSqlServerDatabaseMeta(), 1536));
  }

  @Test
  void anUnknownDimensionFallsBackToText() {
    // There is no unsized VECTOR on SQL Server, and the fallback must not size the column from
    // the dimension: a three dimension vector needs far more than three characters.
    assertEquals("VARCHAR(MAX)", definitionOf(new MsSqlServerDatabaseMeta(), 0));
    assertEquals("VARCHAR(MAX)", definitionOf(new MsSqlServerDatabaseMeta(), -1));
  }

  @Test
  void aServerOlderThan2025HasNoVectorType() {
    MsSqlServerDatabaseMeta database = new MsSqlServerDatabaseMeta();
    database.setServerInfo(new ServerInfo(16, 0, Set.of("NVARCHAR")));

    assertFalse(database.isColumnTypeAvailable("VECTOR"));
    assertEquals("VARCHAR(MAX)", definitionOf(database, 1536));
  }

  @Test
  void a2025ServerKeepsTheVectorColumn() {
    MsSqlServerDatabaseMeta database = new MsSqlServerDatabaseMeta();
    database.setServerInfo(new ServerInfo(17, 0, Set.of("NVARCHAR", "VECTOR")));

    assertTrue(database.isColumnTypeAvailable("VECTOR"));
    assertEquals("VECTOR(768)", definitionOf(database, 768));
  }

  private static final class VectorValueMeta extends ValueMetaBase {
    private VectorValueMeta(String name, int dimension) {
      super(name, IValueMeta.TYPE_VECTOR, dimension, 0);
    }
  }
}
