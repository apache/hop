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

package org.apache.hop.neo4j.bolt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.graph.GraphDatabasePluginType;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.hop.core.graph.GraphSchemaSampler;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Reads the schema of a real Neo4j 5 server and searches its vector index, through a Neo4j graph
 * database connection.
 *
 * <p>Skipped when Docker is unavailable or the container cannot start in time.
 */
class Neo4jBoltIT {
  private static Neo4jContainer<?> neo4j;
  private static GraphDatabaseMeta graphDatabaseMeta;
  private static final IVariables variables = Variables.getADefaultVariableSpace();

  @BeforeAll
  static void setUp() throws Exception {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(), "Docker is required for Neo4jBoltIT");
    neo4j =
        new Neo4jContainer<>(DockerImageName.parse("neo4j:5.26"))
            .withAdminPassword("hop-it-password")
            .withEnv("NEO4J_server_memory_heap_initial__size", "256m")
            .withEnv("NEO4J_server_memory_heap_max__size", "512m")
            .withEnv("NEO4J_server_memory_pagecache_size", "64m")
            .withStartupTimeout(Duration.ofMinutes(5));
    try {
      neo4j.start();
    } catch (Exception e) {
      abort("Neo4j container did not become ready: " + e.getMessage());
    }

    HopClientEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            Neo4jGraphDatabase.class.getName(),
            GraphDatabasePluginType.class,
            GraphDatabasePlugin.class);
    Neo4jGraphDatabase type = (Neo4jGraphDatabase) GraphDatabaseMeta.createGraphDatabase("NEO4J");
    type.setServer(neo4j.getHost());
    type.setBoltPort(Integer.toString(neo4j.getMappedPort(7687)));
    type.setAutomatic(false);
    type.setProtocol("bolt");
    type.setUsername("neo4j");
    type.setPassword("hop-it-password");
    graphDatabaseMeta = new GraphDatabaseMeta("neo4j", type);

    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      connection.execute(
          "CREATE (:Person {name: 'a', age: 3, tags: ['x']})-[:KNOWS {since: 2020}]->"
              + "(:Person:Employee {name: 'b'}) CREATE (:Empty)",
          Map.of());
      connection.execute(
          "CREATE CONSTRAINT person_name IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE",
          Map.of());
    }
  }

  @AfterAll
  static void tearDown() {
    if (neo4j != null) {
      neo4j.stop();
    }
  }

  private static GraphSchemaEntry entry(GraphSchema schema, String name, String property) {
    return schema.entries().stream()
        .filter(e -> e.name().equals(name) && Objects.equals(e.property(), property))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + "." + property + " not in " + schema));
  }

  /** The schema procedures of Neo4j, with the ends of the relationships and the indexes. */
  @Test
  void testSchema() throws Exception {
    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      GraphSchema schema = connection.getSchema(10);
      assertFalse(schema.sampled());
      checkSchema(schema);
      assertEquals(List.of("List<String>"), entry(schema, "Person", "tags").propertyTypes());
      assertTrue(entry(schema, "Empty", null).propertyTypes().isEmpty());
      assertEquals(true, schema.isUnique(entry(schema, "Person", "name")));
      assertEquals(false, schema.isIndexed(entry(schema, "Person", "age")));

      // The sample, for Neo4j without the procedures
      GraphSchema sampled =
          GraphSchemaSampler.sample(connection, null, null, CypherGraphDialect::quote, 100);
      assertTrue(sampled.sampled());
      checkSchema(sampled);
    }
  }

  private static void checkSchema(GraphSchema schema) {
    assertEquals(true, entry(schema, "Person", "name").mandatory());
    assertEquals(false, entry(schema, "Person", "age").mandatory());
    assertEquals(List.of("Integer"), entry(schema, "Person", "age").propertyTypes());
    assertEquals(true, entry(schema, "Employee", "name").mandatory());
    GraphSchemaEntry since = entry(schema, "KNOWS", "since");
    assertEquals(GraphObjectType.RELATIONSHIP, since.elementType());
    assertEquals(List.of("Integer"), since.propertyTypes());
    assertTrue(since.startLabels().contains("Person"), since.toString());
    assertTrue(since.endLabels().contains("Employee"), since.toString());
  }

  /**
   * The statement Graph vector search runs ranks the nodes by similarity, scored the same way on
   * every database: the cosine similarity, or 1 / (1 + squared euclidean distance).
   */
  @Test
  void testVectorSearch() throws Exception {
    Neo4jGraphDialect dialect = Neo4jGraphDialect.INSTANCE;
    for (GraphVectorSimilarity similarity : GraphVectorSimilarity.values()) {
      String indexName = "vs_it_docs_" + similarity.name().toLowerCase();
      String label = "VsITDoc" + similarity.name();
      GraphVectorIndexDefinition index =
          new GraphVectorIndexDefinition(
              indexName, GraphObjectType.NODE, label, List.of("embedding"), 3, similarity, null);
      try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
        connection.execute(dialect.getCreateVectorIndexStatement(index), Map.of());
        List<List<Double>> vectors =
            List.of(List.of(1.0, 0.0, 0.0), List.of(0.0, 1.0, 0.0), List.of(0.7, 0.7, 0.0));
        for (int i = 0; i < vectors.size(); i++) {
          connection.execute(
              "CREATE (:" + label + " {id: $id, embedding: " + dialect.vectorValue("$e") + "})",
              Map.of("id", (long) i + 1, "e", vectors.get(i)));
        }
        connection.execute("CALL db.awaitIndexes(120)", Map.of());

        GraphStatement search =
            dialect.getVectorSearchStatement(
                new GraphVectorSearchDefinition(
                    indexName, null, null, 2, List.of("id"), similarity));
        Map<String, Object> parameters = new HashMap<>(search.parameters());
        parameters.put(GraphVectorSearchDefinition.PARAMETER_VECTOR, List.of(0.9, 0.1, 0.0));
        List<Map<String, Object>> hits = connection.execute(search.statement(), parameters);
        System.out.println("NEO4J-VECTOR-SEARCH " + similarity + " " + hits);
        assertEquals(2, hits.size(), hits.toString());
        assertEquals(1L, hits.get(0).get("p0"));
        assertEquals(3L, hits.get(1).get("p0"));
        // Opposite vectors aren't checked here: with the quantized vectors of Neo4j 5.26 the
        // scores of the three dimensional test vectors are too far off
        assertScores(similarity, hits);

        connection.execute(dialect.getDropVectorIndexStatement(index), Map.of());
      }
    }
  }

  /**
   * A vector index on relationships, created and dropped with the statements of the Graph index
   * action, searched with the statement of Graph vector search over relationships.
   */
  @Test
  void testRelationshipVectorSearch() throws Exception {
    Neo4jGraphDialect dialect = Neo4jGraphDialect.INSTANCE;
    NamedGraphConnection named = new NamedGraphConnection("it", null, graphDatabaseMeta);
    IndexUpdate create =
        IndexUpdate.vector(
            UpdateType.CREATE,
            ObjectType.RELATIONSHIP,
            "vs it similar",
            "VS IT SIMILAR",
            "embed ding",
            "3",
            GraphVectorSimilarity.COSINE);
    IndexUpdate drop = new IndexUpdate(create);
    drop.setType(UpdateType.DROP);
    NeoConnectionUtils.runSchemaStatement(
        named,
        LogChannel.GENERAL,
        variables,
        Neo4jIndex.generateCreateIndexCypher(create, dialect),
        "Creating index");
    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      connection.execute("CREATE (:VsITRelDoc {id: 0})", Map.of());
      List<List<Double>> vectors =
          List.of(List.of(1.0, 0.0, 0.0), List.of(0.0, 1.0, 0.0), List.of(0.7, 0.7, 0.0));
      for (int i = 0; i < vectors.size(); i++) {
        connection.execute(
            "MATCH (a:VsITRelDoc {id: 0}) CREATE (a)-[:`VS IT SIMILAR` {id: $id, `embed ding`: "
                + dialect.vectorValue("$e")
                + "}]->(:VsITRelDoc {id: $id})",
            Map.of("id", (long) i + 1, "e", vectors.get(i)));
      }
      connection.execute("CALL db.awaitIndexes(120)", Map.of());

      GraphStatement search =
          dialect.getVectorSearchStatement(
              new GraphVectorSearchDefinition(
                  "vs it similar",
                  null,
                  null,
                  2,
                  List.of("id"),
                  GraphVectorSimilarity.COSINE,
                  GraphObjectType.RELATIONSHIP));
      Map<String, Object> parameters = new HashMap<>(search.parameters());
      parameters.put(GraphVectorSearchDefinition.PARAMETER_VECTOR, List.of(0.9, 0.1, 0.0));
      List<Map<String, Object>> hits = connection.execute(search.statement(), parameters);
      System.out.println("NEO4J-RELATIONSHIP-VECTOR-SEARCH " + hits);
      assertEquals(2, hits.size(), hits.toString());
      assertEquals(1L, hits.get(0).get("p0"));
      assertEquals(3L, hits.get(1).get("p0"));
      assertScores(GraphVectorSimilarity.COSINE, hits);
    }
    NeoConnectionUtils.runSchemaStatement(
        named,
        LogChannel.GENERAL,
        variables,
        Neo4jIndex.generateDropIndexCypher(drop, dialect),
        "Dropping index");
  }

  /**
   * Neo4j 5 quantizes the vectors in its vector indexes: the scores are off by up to 0.02 for these
   * vectors, still far from the other definitions of the score.
   */
  private static final double SCORE_TOLERANCE = 3e-2;

  /**
   * The score for the query vector [0.9, 0.1, 0] and the stored vectors [1, 0, 0] and [0.7, 0.7,
   * 0]: the cosine similarity, or 1 / (1 + squared euclidean distance).
   */
  private static void assertScores(
      GraphVectorSimilarity similarity, List<Map<String, Object>> hits) {
    double first = ((Number) hits.get(0).get("score")).doubleValue();
    double second = ((Number) hits.get(1).get("score")).doubleValue();
    if (similarity == GraphVectorSimilarity.COSINE) {
      assertEquals(0.9 / Math.sqrt(0.82), first, SCORE_TOLERANCE, hits.toString());
      assertEquals(0.7 / Math.sqrt(0.82 * 0.98), second, SCORE_TOLERANCE, hits.toString());
    } else {
      assertEquals(1 / 1.02, first, SCORE_TOLERANCE, hits.toString());
      assertEquals(1 / 1.4, second, SCORE_TOLERANCE, hits.toString());
    }
  }
}
