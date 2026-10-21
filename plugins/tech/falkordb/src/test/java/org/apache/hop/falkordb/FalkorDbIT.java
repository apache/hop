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

package org.apache.hop.falkordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerTabBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Runs a FalkorDB graph database connection against a real FalkorDB server. */
class FalkorDbIT {
  private static GenericContainer<?> falkordb;
  private static FalkorDbGraphDatabase graphDatabase;
  private static final IVariables variables = Variables.getADefaultVariableSpace();

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(), "Docker is required for FalkorDbIT");
    falkordb =
        new GenericContainer<>(DockerImageName.parse("falkordb/falkordb:6.0.1"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(Duration.ofMinutes(3));
    try {
      falkordb.start();
    } catch (Exception e) {
      abort("FalkorDB container did not become ready: " + e.getMessage());
    }
    graphDatabase = new FalkorDbGraphDatabase();
    graphDatabase.setHostname(falkordb.getHost());
    graphDatabase.setPort(Integer.toString(falkordb.getMappedPort(6379)));
    graphDatabase.setGraphName("hop_it");
  }

  @AfterAll
  static void tearDown() {
    if (falkordb != null) {
      falkordb.stop();
    }
  }

  @Test
  void testConnection() throws Exception {
    assertFalse(graphDatabase.test(variables, "falkordb").isEmpty());
  }

  /** Connections share the client resources, which stay running when they are closed. */
  @Test
  void testConnectionsShareClientResources() throws Exception {
    FalkorDbGraphConnection first =
        (FalkorDbGraphConnection) graphDatabase.connect(LogChannel.GENERAL, variables, "it");
    FalkorDbGraphConnection second =
        (FalkorDbGraphConnection) graphDatabase.connect(LogChannel.GENERAL, variables, "it");
    assertSame(first.getClient().getResources(), second.getClient().getResources());
    assertSame(FalkorDbClientResources.get(), first.getClient().getResources());
    first.close();
    second.close();
    assertFalse(FalkorDbClientResources.get().eventExecutorGroup().isShuttingDown());
    try (IGraphConnection third = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      assertEquals(1L, third.execute("RETURN 1 AS one", Map.of()).get(0).get("one"));
    }
  }

  /** The way Neo4j Output writes: UNWIND over a list of maps in one statement. */
  @Test
  void testUnwindWriteAndRead() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      List<Map<String, Object>> props = new ArrayList<>();
      for (long id = 1; id <= 3; id++) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", "Person's " + id);
        row.put("score", id * 1.5);
        props.add(row);
      }
      connection.executeWrite(
          tx -> {
            tx.execute(
                "UNWIND $props AS pr MERGE (n:Person {id: pr.id}) SET n.name = pr.name, n.score = pr.score",
                Map.of("props", props));
            return null;
          });
      connection.execute(
          "MATCH (a:Person {id: 1}), (b:Person {id: 2}) MERGE (a)-[:KNOWS {since: $since}]->(b)",
          Map.of("since", 2020L));

      List<Map<String, Object>> rows =
          connection.execute(
              "MATCH (n:Person) RETURN n.id AS id, n.name AS name, n.score AS score ORDER BY id",
              Map.of());
      assertEquals(3, rows.size());
      assertEquals(1L, rows.get(0).get("id"));
      assertEquals("Person's 1", rows.get(0).get("name"));

      List<Map<String, Object>> graph =
          connection.execute("MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b", Map.of());
      GraphNodeValue a = (GraphNodeValue) graph.get(0).get("a");
      assertEquals(List.of("Person"), a.labels());
      assertEquals("Person's 1", a.properties().get("name"));
      GraphRelationshipValue r = (GraphRelationshipValue) graph.get(0).get("r");
      assertEquals("KNOWS", r.type());
      assertEquals(2020L, r.properties().get("since"));
      assertEquals(a.id(), r.startNodeId());

      // Typed values: lists stay lists, doubles doubles, dates dates
      Map<String, Object> typed =
          connection
              .execute(
                  "MATCH p=(a:Person {id: 1})-[:KNOWS]->(b) RETURN labels(a) AS labels, a.score AS score,"
                      + " date('2024-01-02') AS d, p",
                  Map.of())
              .get(0);
      assertEquals(List.of("Person"), typed.get("labels"));
      assertEquals(1.5d, typed.get("score"));
      assertEquals(java.time.LocalDate.of(2024, 1, 2), typed.get("d"));
      GraphPathValue path = (GraphPathValue) typed.get("p");
      assertEquals(2, path.nodes().size());
      assertEquals(1, path.relationships().size());
    }
  }

  /**
   * A Vector property arrives as a list of numbers. FalkorDB stores it as a vector, and indexes it,
   * only when the statement wraps it in vecf32(), which Graph output does for this dialect.
   */
  @Test
  void testVectorWriteSearchAndRead() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      connection.execute(
          "CREATE VECTOR INDEX FOR (d:Doc) ON (d.embedding) OPTIONS {dimension: 3, similarityFunction: 'cosine'}",
          Map.of());
      connection.execute(
          "MERGE (d:Doc {id: $id}) SET d.embedding = vecf32($e)",
          Map.of("id", 1L, "e", List.of(1.0, 0.0, 0.0)));
      connection.execute(
          "MERGE (d:Doc {id: $id}) SET d.embedding = vecf32($e)",
          Map.of("id", 2L, "e", List.of(0.0, 1.0, 0.0)));
      // Without vecf32() the list is stored as a plain array and stays out of the index
      connection.execute(
          "MERGE (d:Doc {id: $id}) SET d.embedding = $e",
          Map.of("id", 3L, "e", List.of(0.9, 0.1, 0.0)));

      List<Map<String, Object>> nearest =
          connection.execute(
              "CALL db.idx.vector.queryNodes('Doc', 'embedding', 3, vecf32($q)) YIELD node, score"
                  + " RETURN node.id AS id ORDER BY score",
              Map.of("q", List.of(0.9, 0.1, 0.0)));
      assertEquals(2, nearest.size(), nearest.toString());
      assertEquals(1L, nearest.get(0).get("id"));

      Object stored =
          connection
              .execute("MATCH (d:Doc {id: 1}) RETURN d.embedding AS e", Map.of())
              .get(0)
              .get("e");
      assertEquals(List.of(1.0, 0.0, 0.0), stored);
    }
  }

  /**
   * The index statements of the dialect are accepted, with labels and properties which need
   * quoting. Creating what exists and dropping what doesn't fails with errors the dialect ignores.
   */
  @Test
  void testIdempotentQuotedIndexStatements() throws Exception {
    FalkorDbGraphDialect dialect = FalkorDbGraphDialect.INSTANCE;
    GraphIndexDefinition index =
        new GraphIndexDefinition(null, GraphObjectType.NODE, "IT Label-x", List.of("my prop"));
    GraphVectorIndexDefinition vector =
        new GraphVectorIndexDefinition(
            null,
            GraphObjectType.NODE,
            "IT Label-x",
            List.of("embed ding"),
            3,
            GraphVectorSimilarity.COSINE,
            null);
    GraphVectorIndexDefinition relationshipVector =
        new GraphVectorIndexDefinition(
            null,
            GraphObjectType.RELATIONSHIP,
            "IT TYPE-x",
            List.of("embed ding"),
            3,
            GraphVectorSimilarity.EUCLIDEAN,
            null);
    List<String> creates =
        List.of(
            dialect.getCreateIndexStatement(index),
            dialect.getCreateVectorIndexStatement(vector),
            dialect.getCreateVectorIndexStatement(relationshipVector));
    List<String> drops =
        List.of(
            dialect.getDropIndexStatement(index),
            dialect.getDropVectorIndexStatement(vector),
            dialect.getDropVectorIndexStatement(relationshipVector));
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      for (List<String> statements : List.of(creates, creates, drops, drops)) {
        for (String statement : statements) {
          try {
            connection.execute(statement, Map.of());
          } catch (Exception e) {
            System.out.println(
                "FALKORDB-ERROR [" + statement + "] " + Const.getSimpleStackTrace(e));
            assertTrue(dialect.isExistingOrMissingIndexError(e), Const.getSimpleStackTrace(e));
          }
        }
      }
    }
  }

  private static GraphSchemaEntry entry(GraphSchema schema, String name, String property) {
    return schema.entries().stream()
        .filter(e -> e.name().equals(name) && Objects.equals(e.property(), property))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + "." + property + " not in " + schema));
  }

  /** The labels and types from the procedures, their properties from a sample of each. */
  @Test
  void testSchema() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      connection.execute(
          "CREATE (:SchemaITPerson {name: 'a', age: 3})-[:SCHEMA_IT_KNOWS {since: 2020}]->"
              + "(:SchemaITPerson:SchemaITEmployee {name: 'b'}), (:SchemaITEmpty)",
          Map.of());
      connection.execute(
          FalkorDbGraphDialect.INSTANCE.getCreateNodeIndexStatement(
              null, "SchemaITPerson", List.of("name")),
          Map.of());

      GraphSchema schema = connection.getSchema(100);

      assertTrue(schema.sampled());
      assertEquals(true, entry(schema, "SchemaITPerson", "name").mandatory());
      assertEquals(false, entry(schema, "SchemaITPerson", "age").mandatory());
      assertEquals(List.of("Integer"), entry(schema, "SchemaITPerson", "age").propertyTypes());
      assertEquals(true, entry(schema, "SchemaITEmployee", "name").mandatory());
      assertNull(entry(schema, "SchemaITEmpty", null).mandatory());
      GraphSchemaEntry since = entry(schema, "SCHEMA_IT_KNOWS", "since");
      assertTrue(since.isRelationship());
      assertEquals(List.of("SchemaITPerson"), since.startLabels());
      assertTrue(since.endLabels().contains("SchemaITEmployee"), since.toString());
      assertEquals(true, schema.isIndexed(entry(schema, "SchemaITPerson", "name")));
      assertEquals(false, schema.isIndexed(entry(schema, "SchemaITPerson", "age")));
    }
  }

  /**
   * The statement Graph vector search runs ranks the nodes by similarity: FalkorDB's distance
   * turned into the cosine similarity or 1 / (1 + squared euclidean distance).
   */
  @Test
  void testVectorSearch() throws Exception {
    FalkorDbGraphDialect dialect = FalkorDbGraphDialect.INSTANCE;
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      for (GraphVectorSimilarity similarity : GraphVectorSimilarity.values()) {
        String label = "VsITDoc" + similarity.name();
        connection.execute(
            dialect.getCreateVectorIndexStatement(
                new GraphVectorIndexDefinition(
                    null, GraphObjectType.NODE, label, List.of("embedding"), 3, similarity, null)),
            Map.of());
        List<List<Double>> vectors =
            List.of(List.of(1.0, 0.0, 0.0), List.of(0.0, 1.0, 0.0), List.of(0.7, 0.7, 0.0));
        for (int i = 0; i < vectors.size(); i++) {
          connection.execute(
              "CREATE (:" + label + " {id: $id, embedding: " + dialect.vectorValue("$e") + "})",
              Map.of("id", (long) i + 1, "e", vectors.get(i)));
        }
        GraphStatement search =
            dialect.getVectorSearchStatement(
                new GraphVectorSearchDefinition(
                    null, label, "embedding", 2, List.of("id"), similarity));
        Map<String, Object> parameters = new HashMap<>(search.parameters());
        parameters.put(GraphVectorSearchDefinition.PARAMETER_VECTOR, List.of(0.9, 0.1, 0.0));
        List<Map<String, Object>> hits = connection.execute(search.statement(), parameters);
        System.out.println("FALKORDB-VECTOR-SEARCH " + similarity + " " + hits);
        assertEquals(2, hits.size(), hits.toString());
        assertEquals(1L, hits.get(0).get("p0"));
        assertEquals(3L, hits.get(1).get("p0"));
        assertScores(similarity, hits);
        if (similarity == GraphVectorSimilarity.COSINE) {
          assertOppositeVectorsRankLast(connection, search);
        }
      }
    }
  }

  /**
   * A vector index on relationships, created and dropped with the statements the Graph index action
   * gets from the dialect, searched over relationships for cosine and euclidean indexes.
   */
  @Test
  void testRelationshipVectorSearch() throws Exception {
    FalkorDbGraphDialect dialect = FalkorDbGraphDialect.INSTANCE;
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      for (GraphVectorSimilarity similarity : GraphVectorSimilarity.values()) {
        String type = "VS IT SIMILAR " + similarity.name();
        GraphVectorIndexDefinition index =
            new GraphVectorIndexDefinition(
                null,
                GraphObjectType.RELATIONSHIP,
                type,
                List.of("embed ding"),
                3,
                similarity,
                null);
        connection.execute(dialect.getCreateVectorIndexStatement(index), Map.of());
        connection.execute("CREATE (:VsITRelDoc {id: 0, similarity: $s})", Map.of("s", type));
        List<List<Double>> vectors =
            List.of(List.of(1.0, 0.0, 0.0), List.of(0.0, 1.0, 0.0), List.of(0.7, 0.7, 0.0));
        for (int i = 0; i < vectors.size(); i++) {
          connection.execute(
              "MATCH (a:VsITRelDoc {id: 0, similarity: $s}) CREATE (a)-[:`"
                  + type
                  + "` {id: $id, `embed ding`: "
                  + dialect.vectorValue("$e")
                  + "}]->(:VsITRelDoc {id: $id})",
              Map.of("s", type, "id", (long) i + 1, "e", vectors.get(i)));
        }
        GraphStatement search =
            dialect.getVectorSearchStatement(
                new GraphVectorSearchDefinition(
                    null,
                    type,
                    "embed ding",
                    2,
                    List.of("id"),
                    similarity,
                    GraphObjectType.RELATIONSHIP));
        Map<String, Object> parameters = new HashMap<>(search.parameters());
        parameters.put(GraphVectorSearchDefinition.PARAMETER_VECTOR, List.of(0.9, 0.1, 0.0));
        List<Map<String, Object>> hits = connection.execute(search.statement(), parameters);
        System.out.println("FALKORDB-RELATIONSHIP-VECTOR-SEARCH " + similarity + " " + hits);
        assertEquals(2, hits.size(), hits.toString());
        assertEquals(1L, hits.get(0).get("p0"));
        assertEquals(3L, hits.get(1).get("p0"));
        assertScores(similarity, hits);

        connection.execute(dialect.getDropVectorIndexStatement(index), Map.of());
      }
    }
  }

  /**
   * FalkorDB stores the vectors as 32-bit floats. Its vector index has returned the cosine
   * similarity 1.2e-4 away from the exact value, which nightly integration build 2336 rejected at
   * 1e-4. 1e-3 still leaves the cosine and euclidean scores for these vectors more than 0.01 apart.
   */
  private static final double SCORE_TOLERANCE = 1e-3;

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

  /**
   * The cosine similarity runs from -1 to 1: for the query vector [-1, 0, 0] the stored vectors [0,
   * 1, 0], [0.7, 0.7, 0] and [1, 0, 0] score 0, -0.71 and -1, and the opposite vector ranks last.
   */
  private static void assertOppositeVectorsRankLast(
      IGraphConnection connection, GraphStatement search) throws Exception {
    Map<String, Object> parameters = new HashMap<>(search.parameters());
    parameters.put("k", 3L);
    parameters.put(GraphVectorSearchDefinition.PARAMETER_VECTOR, List.of(-1.0, 0.0, 0.0));
    List<Map<String, Object>> hits = connection.execute(search.statement(), parameters);
    assertEquals(3, hits.size(), hits.toString());
    assertEquals(
        List.of(2L, 3L, 1L), hits.stream().map(hit -> hit.get("p0")).toList(), hits.toString());
    assertEquals(
        0.0, ((Number) hits.get(0).get("score")).doubleValue(), SCORE_TOLERANCE, hits.toString());
    assertEquals(
        -0.7 / Math.sqrt(0.98),
        ((Number) hits.get(1).get("score")).doubleValue(),
        SCORE_TOLERANCE,
        hits.toString());
    assertEquals(
        -1.0, ((Number) hits.get(2).get("score")).doubleValue(), SCORE_TOLERANCE, hits.toString());
  }

  /**
   * Neo4j logging writes Execution nodes with the same IDs as the execution information location,
   * and with a type. The paths of the execution viewer keep to the nodes of the execution
   * information location (issue #8704).
   */
  @Test
  void testExecutionPathsSkipNeo4jLoggingNodes() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      connection.execute("MATCH (n:Execution) DETACH DELETE n", Map.of());
      connection.execute(
          "CREATE (w:Execution {id: 'w', failed: true})-[:EXECUTES]->"
              + "(p:Execution {id: 'p', parentId: 'w', failed: true})-[:EXECUTES]->"
              + "(f:Execution {id: 'f', parentId: 'p', failed: true}), "
              + "(p)-[:EXECUTES]->(o:Execution {id: 'o', parentId: 'p', failed: false})",
          Map.of());
      connection.execute(
          "CREATE (lw:Execution {id: 'w', type: 'WORKFLOW', failed: true})-[:EXECUTES]->"
              + "(lp:Execution {id: 'p', type: 'PIPELINE', failed: true})-[:EXECUTES]->"
              + "(lf:Execution {id: 'f', type: 'TRANSFORM', failed: true})",
          Map.of());
      IGraphDialect dialect = connection.getGraphDialect();
      try {
        assertEquals(
            List.of(List.of("w", "p", "f")),
            executionPaths(
                connection, NeoExecutionViewerTabBase.buildPathToRootCypher(true, dialect), "f"));
        assertEquals(
            List.of(List.of("w", "p", "f")),
            executionPaths(
                connection, NeoExecutionViewerTabBase.buildPathToFailedCypher(dialect), "w"));
      } finally {
        connection.execute("MATCH (n:Execution) DETACH DELETE n", Map.of());
      }
    }
  }

  /** The IDs of the nodes of each path, from the start of the path to its end. */
  private static List<List<String>> executionPaths(
      IGraphConnection connection, String cypher, String executionId) throws Exception {
    List<List<String>> paths = new ArrayList<>();
    for (Map<String, Object> row : connection.execute(cypher, Map.of("executionId", executionId))) {
      if (row.get("p") instanceof GraphPathValue path) {
        List<String> ids = new ArrayList<>();
        for (GraphNodeValue node : path.nodes()) {
          ids.add(String.valueOf(node.properties().get("id")));
        }
        paths.add(ids);
      }
    }
    return paths;
  }
}
