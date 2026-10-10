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

package org.apache.hop.age;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.graph.IGraphTransaction;
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

/** Runs an Apache AGE graph database connection against PostgreSQL with AGE. */
class AgeIT {
  private static GenericContainer<?> age;
  private static AgeGraphDatabase graphDatabase;
  private static final IVariables variables = Variables.getADefaultVariableSpace();

  @BeforeAll
  static void setUp() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required for AgeIT");
    age =
        new GenericContainer<>(DockerImageName.parse("apache/age:release_PG18_1.8.0"))
            .withExposedPorts(5432)
            .withEnv("POSTGRES_USER", "hop")
            .withEnv("POSTGRES_PASSWORD", "hop")
            .withEnv("POSTGRES_DB", "hop")
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
            .withStartupTimeout(Duration.ofMinutes(3));
    try {
      age.start();
    } catch (Exception e) {
      abort("Apache AGE container did not become ready: " + e.getMessage());
    }
    HopClientEnvironment.init();
    graphDatabase = new AgeGraphDatabase();
    graphDatabase.setHostname(age.getHost());
    graphDatabase.setPort(Integer.toString(age.getMappedPort(5432)));
    graphDatabase.setDatabaseName("hop");
    graphDatabase.setUsername("hop");
    graphDatabase.setPassword("hop");
    graphDatabase.setGraphName("hop_it");
  }

  @AfterAll
  static void tearDown() {
    if (age != null) {
      age.stop();
    }
  }

  @Test
  void testConnection() throws Exception {
    assertFalse(graphDatabase.test(variables, "age").isEmpty());
  }

  /** NaN and infinite floats come back as doubles instead of failing the query. */
  @Test
  void testNonNumericFloats() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      Map<String, Object> row =
          connection
              .execute(
                  "RETURN toFloat('NaN') AS n, toFloat('Infinity') AS i,"
                      + " toFloat('-Infinity') AS m, [toFloat('NaN'), 1.5] AS l",
                  Map.of())
              .get(0);
      assertEquals(Double.NaN, row.get("n"));
      assertEquals(Double.POSITIVE_INFINITY, row.get("i"));
      assertEquals(Double.NEGATIVE_INFINITY, row.get("m"));
      assertEquals(List.of(Double.NaN, 1.5d), row.get("l"));
    }
  }

  /** The way Neo4j Output writes: UNWIND over a list of maps, in a transaction. */
  @Test
  void testUnwindWriteAndRead() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      List<Map<String, Object>> props = new ArrayList<>();
      for (long id = 1; id <= 3; id++) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", "Person's " + id);
        props.add(row);
      }
      connection.executeWrite(
          tx -> {
            tx.execute(
                "UNWIND $props AS pr MERGE (n:Person {id: pr.id}) SET n.name = pr.name",
                Map.of("props", props));
            return null;
          });
      connection.execute(
          "MATCH (a:Person {id: 1}), (b:Person {id: 2}) MERGE (a)-[:KNOWS {since: $since}]->(b)",
          Map.of("since", 2020L));

      List<Map<String, Object>> rows =
          connection.execute(
              "MATCH (n:Person) RETURN n.id AS id, n.name AS name ORDER BY n.id", Map.of());
      assertEquals(3, rows.size());
      assertEquals(1L, rows.get(0).get("id"));
      assertEquals("Person's 1", rows.get(0).get("name"));

      Map<String, Object> graph =
          connection
              .execute("MATCH (a)-[r:KNOWS]->(b) RETURN a, r, labels(a) AS l", Map.of())
              .get(0);
      GraphNodeValue a = (GraphNodeValue) graph.get("a");
      assertEquals(List.of("Person"), a.labels());
      GraphRelationshipValue r = (GraphRelationshipValue) graph.get("r");
      assertEquals(2020L, r.properties().get("since"));
      assertEquals(a.id(), r.startNodeId());
      assertEquals(List.of("Person"), graph.get("l"));
    }
  }

  @Test
  void testRollback() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      try (IGraphTransaction transaction = connection.beginTransaction()) {
        transaction.execute("CREATE (:Rollback {id: 1})", Map.of());
        transaction.rollback();
      }
      assertEquals(
          0L,
          connection.execute("MATCH (n:Rollback) RETURN count(n) AS c", Map.of()).get(0).get("c"));
      assertThrows(
          HopException.class,
          () ->
              connection.executeWrite(
                  tx -> {
                    tx.execute("CREATE (:Rollback {id: 2})", Map.of());
                    throw new HopException("Roll back");
                  }));
      assertEquals(
          0L,
          connection.execute("MATCH (n:Rollback) RETURN count(n) AS c", Map.of()).get(0).get("c"));
    }
  }

  /** Property indexes are PostgreSQL indexes on the label tables. */
  @Test
  void testIndexes() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      connection.execute(
          "CREATE (:Indexed {k: 1, a: 2})-[:INDEXED_REL {w: 3}]->(:Indexed {k: 2})", Map.of());
      String url = "jdbc:postgresql://" + age.getHost() + ":" + age.getMappedPort(5432) + "/hop";
      try (Connection jdbc = DriverManager.getConnection(url, "hop", "hop");
          Statement statement = jdbc.createStatement()) {
        statement.execute(
            "CREATE UNIQUE INDEX indexed_k ON hop_it.\"Indexed\" (ag_catalog.agtype_access_operator("
                + "VARIADIC ARRAY[properties, '\"k\"'::ag_catalog.agtype]))");
        statement.execute(
            "CREATE INDEX indexed_rel_all ON hop_it.\"INDEXED_REL\" USING gin (properties)");
      }
      List<GraphIndex> indexes = connection.getIndexes();
      GraphIndex unique =
          indexes.stream().filter(i -> i.name().equals("indexed_k")).findFirst().orElseThrow();
      assertEquals(List.of("Indexed"), unique.labelsOrTypes());
      assertEquals(List.of("k"), unique.properties());
      assertTrue(unique.unique());
      assertFalse(unique.relationship());
      GraphIndex all =
          indexes.stream()
              .filter(i -> i.name().equals("indexed_rel_all"))
              .findFirst()
              .orElseThrow();
      assertTrue(all.relationship());
      assertTrue(all.covers("INDEXED_REL", "w"));
      // Only property indexes: not the indexes on ids which AGE creates
      assertTrue(indexes.stream().noneMatch(i -> i.name().endsWith("_pkey")));
    }
  }

  /**
   * The index statements of the dialect run through executeSchemaStatement: the label is created
   * when it doesn't exist yet, creating and dropping twice does nothing the second time.
   */
  @Test
  void testCreateAndDropIndexStatements() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      IGraphDialect dialect = connection.getGraphDialect();
      GraphIndexDefinition index =
          new GraphIndexDefinition(
              "it_chunk_id", GraphObjectType.NODE, "ItChunk", List.of("id", "the name"));
      String create = dialect.getCreateIndexStatement(index);
      connection.executeSchemaStatement(create, true);
      connection.executeSchemaStatement(create, true);
      GraphIndex created =
          connection.getIndexes().stream()
              .filter(i -> i.name().equals("it_chunk_id"))
              .findFirst()
              .orElseThrow();
      assertEquals(List.of("ItChunk"), created.labelsOrTypes());
      assertEquals(List.of("id", "the name"), created.properties());

      // The label created with the index takes nodes
      connection.execute("CREATE (:ItChunk {id: 1})", Map.of());
      assertEquals(
          1L,
          ((Number)
                  connection
                      .execute("MATCH (c:ItChunk) RETURN count(c) AS n", Map.of())
                      .get(0)
                      .get("n"))
              .longValue());

      String drop = dialect.getDropIndexStatement(index);
      connection.executeSchemaStatement(drop, true);
      connection.executeSchemaStatement(drop, true);
      assertTrue(connection.getIndexes().stream().noneMatch(i -> i.name().equals("it_chunk_id")));
    }
  }

  /** A Vector property arrives as a list of numbers; AGE stores it as an agtype array. */
  @Test
  void testVectorWriteAndRead() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      connection.execute(
          "MERGE (d:Doc {id: $id}) SET d.embedding = $e RETURN d.id AS id",
          Map.of("id", 1L, "e", List.of(0.5, -1.0, 2.25)));
      Object stored =
          connection
              .execute("MATCH (d:Doc {id: 1}) RETURN d.embedding AS e", Map.of())
              .get(0)
              .get("e");
      assertTrue(stored instanceof List<?>, String.valueOf(stored));
      List<?> list = (List<?>) stored;
      assertEquals(3, list.size());
      assertEquals(-1.0, ((Number) list.get(1)).doubleValue());
    }
  }

  private static GraphSchemaEntry entry(GraphSchema schema, String name, String property) {
    return schema.entries().stream()
        .filter(e -> e.name().equals(name) && Objects.equals(e.property(), property))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + "." + property + " not in " + schema));
  }

  /** The labels from the AGE catalog, their properties from a sample of each, with the indexes. */
  @Test
  void testSchema() throws Exception {
    try (IGraphConnection connection = graphDatabase.connect(LogChannel.GENERAL, variables, "it")) {
      connection.execute(
          "CREATE (:SchemaITPerson {name: 'a', age: 3})-[:SCHEMA_IT_KNOWS {since: 2020}]->"
              + "(:SchemaITCompany {name: 'b'})",
          Map.of());
      connection.execute("CREATE (:SchemaITPerson {name: 'c'})", Map.of());
      // AGE indexes are PostgreSQL indexes, created in SQL
      String url = "jdbc:postgresql://" + age.getHost() + ":" + age.getMappedPort(5432) + "/hop";
      try (Connection jdbc = DriverManager.getConnection(url, "hop", "hop");
          Statement statement = jdbc.createStatement()) {
        statement.execute(
            connection
                .getGraphDialect()
                .getCreateNodeIndexStatement("schema_it_name", "SchemaITPerson", List.of("name")));
      }

      GraphSchema schema = connection.getSchema(100);

      assertTrue(schema.sampled());
      assertEquals(true, entry(schema, "SchemaITPerson", "name").mandatory());
      assertEquals(false, entry(schema, "SchemaITPerson", "age").mandatory());
      assertEquals(List.of("Integer"), entry(schema, "SchemaITPerson", "age").propertyTypes());
      assertEquals(List.of("String"), entry(schema, "SchemaITCompany", "name").propertyTypes());
      GraphSchemaEntry since = entry(schema, "SCHEMA_IT_KNOWS", "since");
      assertTrue(since.isRelationship());
      assertEquals(true, since.mandatory());
      assertEquals(List.of("SchemaITPerson"), since.startLabels());
      assertEquals(List.of("SchemaITCompany"), since.endLabels());
      assertEquals(true, schema.isIndexed(entry(schema, "SchemaITPerson", "name")));
      assertEquals(false, schema.isIndexed(entry(schema, "SchemaITPerson", "age")));
      // Not the labels AGE uses itself
      assertTrue(schema.entries().stream().noneMatch(e -> e.name().startsWith("_ag_label")));
    }
  }

  /** Graph output and Graph query start together and both create the graph on a fresh database. */
  @Test
  void testConcurrentConnectCreatesGraphOnce() throws Exception {
    AgeGraphDatabase fresh = new AgeGraphDatabase();
    fresh.setHostname(age.getHost());
    fresh.setPort(Integer.toString(age.getMappedPort(5432)));
    fresh.setDatabaseName("hop");
    fresh.setUsername("hop");
    fresh.setPassword("hop");
    fresh.setGraphName("hop_concurrent_connect");
    runConcurrently(
        8,
        () -> {
          try (IGraphConnection connection = fresh.connect(LogChannel.GENERAL, variables, "it")) {
            connection.execute("RETURN 0", Map.of());
          }
          return null;
        });
    assertEquals(1L, graphCount("hop_concurrent_connect"));
  }

  /**
   * Another process creates the graph between the existence check and the creation: without the
   * in-process lock, the duplicate is detected and the existing graph is used.
   */
  @Test
  void testCreateGraphIfMissingAcrossSessions() throws Exception {
    String url = "jdbc:postgresql://" + age.getHost() + ":" + age.getMappedPort(5432) + "/hop";
    for (boolean autoCommit : new boolean[] {true, false}) {
      String graphName = "hop_concurrent_create_" + autoCommit;
      runConcurrently(
          8,
          () -> {
            try (Connection jdbc = DriverManager.getConnection(url, "hop", "hop")) {
              try (Statement statement = jdbc.createStatement()) {
                statement.execute("LOAD 'age'");
                statement.execute("SET search_path = ag_catalog, \"$user\", public");
              }
              jdbc.setAutoCommit(autoCommit);
              AgeGraphDatabase.createGraphIfMissing(LogChannel.GENERAL, jdbc, graphName);
              // The transaction is still usable after a duplicate
              try (Statement statement = jdbc.createStatement()) {
                statement.execute("SELECT 1");
              }
              if (!autoCommit) {
                jdbc.commit();
              }
            }
            return null;
          });
      assertEquals(1L, graphCount(graphName));
    }
  }

  private static void runConcurrently(int threads, Callable<Void> task) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  return task.call();
                }));
      }
      start.countDown();
      for (Future<Void> future : futures) {
        future.get(2, TimeUnit.MINUTES);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static long graphCount(String graphName) throws Exception {
    String url = "jdbc:postgresql://" + age.getHost() + ":" + age.getMappedPort(5432) + "/hop";
    try (Connection jdbc = DriverManager.getConnection(url, "hop", "hop");
        PreparedStatement ps =
            jdbc.prepareStatement("SELECT count(*) FROM ag_catalog.ag_graph WHERE name = ?")) {
      ps.setString(1, graphName);
      try (ResultSet resultSet = ps.executeQuery()) {
        resultSet.next();
        return resultSet.getLong(1);
      }
    }
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
