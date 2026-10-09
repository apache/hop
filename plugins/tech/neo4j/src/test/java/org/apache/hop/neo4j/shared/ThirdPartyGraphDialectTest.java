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

package org.apache.hop.neo4j.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.apache.hop.neo4j.bolt.MemgraphGraphDialect;
import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerTabBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A graph database plugin which isn't known to the Neo4j plugin defines its own dialect. Before the
 * dialect SPI, any dialect but the built-in ones silently became Neo4j.
 */
class ThirdPartyGraphDialectTest {

  /** The dialect of a made up database with its own index syntax. */
  static class AcmeGraphDialect extends CypherGraphDialect {
    AcmeGraphDialect() {
      super("ACME");
    }

    @Override
    public String getCreateIndexStatement(GraphIndexDefinition index) {
      return "ACME CREATE INDEX ON " + quote(index.objectName());
    }

    @Override
    public String getCreateNodeKeyIndexStatement(String label, List<String> keyProperties) {
      return "ACME KEY " + quote(label);
    }

    @Override
    public boolean isExistingOrMissingIndexError(Throwable error) {
      return error.getMessage().contains("ACME: exists");
    }
  }

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testThirdPartyDialectIsUsedForStatements() throws Exception {
    AcmeGraphDialect acme = new AcmeGraphDialect();
    FakeGraphDatabase database = new FakeGraphDatabase(acme, null);
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("acme", database));

    NamedGraphConnection connection =
        NeoConnectionUtils.findGraphConnection(metadataProvider, "acme");
    assertSame(acme, connection.getDialect());
    assertSame(acme, connection.getDialect(Variables.getADefaultVariableSpace()));

    // The Graph index action
    Neo4jIndex action = new Neo4jIndex("index");
    action.setConnectionName("acme");
    action
        .getIndexUpdates()
        .add(new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, "i", "My Label", "id"));
    action.setMetadataProvider(metadataProvider);
    Result result = action.execute(new Result(), 0);
    assertTrue(result.getResult());
    assertEquals(List.of("ACME CREATE INDEX ON `My Label`"), database.executed);

    // The indexes Graph Output and Neo4j Output create
    NeoConnectionUtils.createNodeIndex(
        new LogChannel("test"), database.newConnection(), List.of("Person"), List.of("id"));
    assertEquals("ACME KEY `Person`", database.executed.get(1));
  }

  /** The dialect decides which errors mean the index exists already. */
  @Test
  void testThirdPartyExistingIndexIsNotAnError() throws Exception {
    FakeGraphDatabase database = new FakeGraphDatabase(new AcmeGraphDialect(), "ACME: exists");
    NeoConnectionUtils.createNodeIndex(
        new LogChannel("test"), database.newConnection(), List.of("Person"), List.of("id"));
    assertEquals(List.of("ACME KEY `Person`"), database.executed);
  }

  /** A dialect which only has an ID supports nothing: it doesn't become Neo4j. */
  @Test
  void testUnknownDialectIsNeutral() throws Exception {
    IGraphDialect unknown = () -> "UNKNOWN";
    FakeGraphDatabase database = new FakeGraphDatabase(unknown, null);
    NamedGraphConnection connection =
        new NamedGraphConnection("unknown", null, new GraphDatabaseMeta("unknown", database));
    assertSame(unknown, connection.getDialect());

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                Neo4jIndex.generateCreateIndexCypher(
                    new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, "i", "Person", "id"),
                    connection.getDialect()));
    assertTrue(e.getMessage().contains("not supported by UNKNOWN"), e.getMessage());

    NeoConnectionUtils.createNodeIndex(
        new LogChannel("test"), database.newConnection(), List.of("Person"), List.of("id"));
    assertTrue(database.executed.isEmpty());

    assertEquals("$v", unknown.vectorValue("$v"));
    assertTrue(
        !NeoExecutionViewerTabBase.buildPathToRootCypher(true, unknown).contains("shortestPath"));
  }

  /** Creating a Memgraph vector index twice, or dropping it twice, is not an error. */
  @Test
  void testMemgraphVectorIndexIsIdempotent() throws Exception {
    for (String error :
        List.of(
            "Vector index bolt_it_docs already exists.",
            "Vector index bolt_it_docs does not exist.")) {
      FakeGraphDatabase database = new FakeGraphDatabase(MemgraphGraphDialect.INSTANCE, error);
      NamedGraphConnection connection =
          new NamedGraphConnection("memgraph", null, new GraphDatabaseMeta("memgraph", database));
      NeoConnectionUtils.runSchemaStatement(
          connection,
          new LogChannel("test"),
          Variables.getADefaultVariableSpace(),
          "CREATE VECTOR INDEX `bolt_it_docs` ON :`Doc`(`e`) WITH CONFIG {}",
          "Creating index");
      assertEquals(1, database.executed.size());
    }
    FakeGraphDatabase failing =
        new FakeGraphDatabase(MemgraphGraphDialect.INSTANCE, "Invalid input");
    assertThrows(
        HopException.class,
        () ->
            NeoConnectionUtils.runSchemaStatement(
                new NamedGraphConnection(
                    "memgraph", null, new GraphDatabaseMeta("memgraph", failing)),
                new LogChannel("test"),
                Variables.getADefaultVariableSpace(),
                "DROP VECTOR INDEX `x`",
                "Dropping index"));
  }

  /**
   * A connection error is never an existing or missing index, whatever its message says: the schema
   * action fails.
   */
  @Test
  void testMemgraphConnectionErrorFails() {
    FakeGraphDatabase database = new FakeGraphDatabase(MemgraphGraphDialect.INSTANCE, null);
    database.connectErrorMessage = "Unable to connect: database 'x' already exists";
    NamedGraphConnection connection =
        new NamedGraphConnection("memgraph", null, new GraphDatabaseMeta("memgraph", database));
    assertThrows(
        HopException.class,
        () ->
            NeoConnectionUtils.runSchemaStatement(
                connection,
                new LogChannel("test"),
                Variables.getADefaultVariableSpace(),
                "CREATE VECTOR INDEX `v` ON :`Doc`(`e`) WITH CONFIG {}",
                "Creating index"));
    assertTrue(database.executed.isEmpty());
  }

  /** The error Memgraph gives for a vector index which exists already is tolerated. */
  @Test
  void testMemgraphStatementErrorIsTolerated() throws Exception {
    FakeGraphDatabase database =
        new FakeGraphDatabase(MemgraphGraphDialect.INSTANCE, "Given vector index already exists.");
    NeoConnectionUtils.runSchemaStatement(
        new NamedGraphConnection("memgraph", null, new GraphDatabaseMeta("memgraph", database)),
        new LogChannel("test"),
        Variables.getADefaultVariableSpace(),
        "CREATE VECTOR INDEX `v` ON :`Doc`(`e`) WITH CONFIG {}",
        "Creating index");
    assertEquals(1, database.executed.size());
  }
}
