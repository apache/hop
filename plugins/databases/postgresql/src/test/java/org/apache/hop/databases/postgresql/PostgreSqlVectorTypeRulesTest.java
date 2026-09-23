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

package org.apache.hop.databases.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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

/**
 * The pgvector column type: what a vector field is written as, and when.
 *
 * <p>The type names checked here were taken from a pgvector/pgvector:pg16 server rather than from
 * the documentation: a bare vector column is accepted and holds vectors of differing dimensions,
 * and only a sized one can carry an index.
 */
class PostgreSqlVectorTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private String definitionOf(PostgreSqlDatabaseMeta database, int dimension) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(database);
    IValueMeta vector = new VectorValueMeta("EMBEDDING");
    vector.setLength(dimension);
    return databaseMeta.getFieldDefinition(vector, null, null, false, false, false).trim();
  }

  @Test
  void aKnownDimensionIsWrittenIntoTheColumn() {
    assertEquals("VECTOR(1536)", definitionOf(new PostgreSqlDatabaseMeta(), 1536));
  }

  @Test
  void anUnknownDimensionStillReachesAVectorColumn() {
    // pgvector accepts a column declared without dimensions; it just cannot be indexed. That is
    // better than text, which cannot hold a vector at all.
    assertEquals("VECTOR", definitionOf(new PostgreSqlDatabaseMeta(), 0));
    assertEquals("VECTOR", definitionOf(new PostgreSqlDatabaseMeta(), -1));
  }

  @Test
  void aServerWithoutTheExtensionFallsBackToText() {
    PostgreSqlDatabaseMeta database = new PostgreSqlDatabaseMeta();
    database.setServerInfo(new ServerInfo(16, 2, Set.of("TEXT", "JSONB")));

    assertFalse(database.isColumnTypeAvailable("VECTOR"));
    assertEquals("TEXT", definitionOf(database, 1536));
  }

  @Test
  void aServerWithTheExtensionKeepsTheVectorColumn() {
    PostgreSqlDatabaseMeta database = new PostgreSqlDatabaseMeta();
    // What the driver reports on a database where CREATE EXTENSION vector has been run.
    database.setServerInfo(new ServerInfo(16, 2, Set.of("TEXT", "JSONB", "VECTOR", "HALFVEC")));

    assertTrue(database.isColumnTypeAvailable("VECTOR"));
    assertEquals("VECTOR(768)", definitionOf(database, 768));
  }

  @Test
  void withNoConnectionTheTypeStands() {
    // Generating DDL offline must not silently downgrade the column: not knowing is not a no.
    assertTrue(new PostgreSqlDatabaseMeta().isColumnTypeAvailable("VECTOR"));
  }

  @Test
  void theValueIsSentAsAnUnspecifiedTypeSoTheServerCanReadItIntoTheColumn()
      throws SQLException, HopValueException {
    // The driver sends a plain String as character varying, and PostgreSQL refuses to coerce that
    // into a vector: "column is of type vector but expression is of type character varying". The
    // insert only works because the value goes over as an unspecified type, the way UUID and INET
    // already do on this dialect.
    IValueMeta vector = new VectorValueMeta("EMBEDDING");
    vector.setLength(3);

    IValueBinding binding = DatabaseTypeMapper.getBinding(new PostgreSqlDatabaseMeta(), vector);
    assertNotNull(binding, "without the binding the INSERT into a VECTOR column is rejected");

    PreparedStatement statement = mock(PreparedStatement.class);
    binding.write(new PostgreSqlDatabaseMeta(), vector, statement, 1, "[0.1,0.2,0.3]");

    verify(statement).setObject(1, "[0.1,0.2,0.3]", Types.OTHER);
  }

  /** A vector field without the plugin that defines the type; only the type id matters here. */
  private static final class VectorValueMeta extends ValueMetaBase {
    private VectorValueMeta(String name) {
      super(name, IValueMeta.TYPE_VECTOR);
    }

    @Override
    public String getString(Object object) {
      // The real value type renders the canonical form; the binding only passes it along.
      return (String) object;
    }
  }
}
