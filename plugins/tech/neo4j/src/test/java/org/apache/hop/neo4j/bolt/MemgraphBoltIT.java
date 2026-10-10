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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.graph.GraphDatabasePluginType;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.hop.core.graph.GraphSchemaSampler;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.neo4j.actions.constraint.ConstraintUpdate;
import org.apache.hop.neo4j.actions.constraint.Neo4jConstraint;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerTabBase;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.neo4j.shared.NeoHopData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Connects to a real Memgraph server with a Memgraph graph database connection, both through the
 * generic graph connection and through the Neo4j connection form the Neo4j transforms use.
 *
 * <p>Skipped when Docker is unavailable or the container cannot start in time.
 */
class MemgraphBoltIT {
  private static GenericContainer<?> memgraph;
  private static GraphDatabaseMeta graphDatabaseMeta;
  private static final IVariables variables = Variables.getADefaultVariableSpace();

  @BeforeAll
  static void setUp() throws Exception {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is required for MemgraphBoltIT");
    memgraph =
        new GenericContainer<>(DockerImageName.parse("memgraph/memgraph:3.6.0"))
            .withExposedPorts(7687)
            // For SHOW SCHEMA INFO
            .withCommand("--schema-info-enabled=true")
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(Duration.ofMinutes(3));
    try {
      memgraph.start();
    } catch (Exception e) {
      abort("Memgraph container did not become ready: " + e.getMessage());
    }

    HopClientEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            MemgraphGraphDatabase.class.getName(),
            GraphDatabasePluginType.class,
            GraphDatabasePlugin.class);

    MemgraphGraphDatabase type =
        (MemgraphGraphDatabase) GraphDatabaseMeta.createGraphDatabase("MEMGRAPH");
    type.setServer(memgraph.getHost());
    type.setBoltPort(Integer.toString(memgraph.getMappedPort(7687)));
    type.setUsername("memgraph");
    type.setPassword("memgraph");
    graphDatabaseMeta = new GraphDatabaseMeta("memgraph", type);
  }

  @AfterAll
  static void tearDown() {
    if (memgraph != null) {
      memgraph.stop();
    }
  }

  @Test
  void testConnection() throws Exception {
    String url = graphDatabaseMeta.test(variables);
    assertTrue(url.startsWith("bolt://"), url);
  }

  @Test
  void testExecute() throws Exception {
    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      connection.execute("CREATE (:BoltIT { name : $name })", Map.of("name", "graph-connection"));
      List<Map<String, Object>> rows =
          connection.execute(
              "MATCH (n:BoltIT { name : $name }) RETURN n.name AS name",
              Map.of("name", "graph-connection"));
      assertEquals(1, rows.size());
      assertEquals("graph-connection", rows.get(0).get("name"));
    }
  }

  /** The Neo4j transforms get the connection as a Neo4j connection: check that path too. */
  @Test
  void testNeoConnectionForm() throws Exception {
    NeoConnection neo =
        ((BoltGraphDatabase) graphDatabaseMeta.getGraphDatabase()).toNeoConnection("memgraph");
    try (Driver driver = neo.getDriver(LogChannel.GENERAL, variables);
        Session session = neo.getSession(LogChannel.GENERAL, driver, variables)) {
      long count =
          session.executeWrite(
              tx -> {
                tx.run("UNWIND range(1, 10) AS i CREATE (:BoltITBatch { id : i })");
                return tx.run("MATCH (n:BoltITBatch) RETURN count(n) AS c")
                    .single()
                    .get("c")
                    .asLong();
              });
      assertEquals(10L, count);
    }
  }

  /** The index and constraint statements of the Memgraph dialect are accepted by Memgraph. */
  @Test
  void testDialectStatements() throws Exception {
    org.apache.hop.neo4j.actions.constraint.ObjectType node =
        org.apache.hop.neo4j.actions.constraint.ObjectType.NODE;
    List<String> statements =
        List.of(
            Neo4jIndex.generateCreateIndexCypher(
                new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, null, "BoltITSchema", "a, b"),
                MemgraphGraphDialect.INSTANCE),
            Neo4jIndex.generateCreateIndexCypher(
                new IndexUpdate(UpdateType.CREATE, ObjectType.RELATIONSHIP, null, "LINK", "w"),
                MemgraphGraphDialect.INSTANCE),
            Neo4jConstraint.generateCreateConstraintCypher(
                new ConstraintUpdate(
                    org.apache.hop.neo4j.actions.constraint.UpdateType.CREATE,
                    node,
                    GraphConstraintType.UNIQUE,
                    null,
                    "BoltITSchema",
                    "id"),
                MemgraphGraphDialect.INSTANCE),
            Neo4jConstraint.generateCreateConstraintCypher(
                new ConstraintUpdate(
                    org.apache.hop.neo4j.actions.constraint.UpdateType.CREATE,
                    node,
                    GraphConstraintType.NOT_NULL,
                    null,
                    "BoltITSchema",
                    "id"),
                MemgraphGraphDialect.INSTANCE),
            Neo4jConstraint.generateDropConstraintCypher(
                new ConstraintUpdate(
                    org.apache.hop.neo4j.actions.constraint.UpdateType.DROP,
                    node,
                    GraphConstraintType.NOT_NULL,
                    null,
                    "BoltITSchema",
                    "id"),
                MemgraphGraphDialect.INSTANCE),
            Neo4jIndex.generateDropIndexCypher(
                new IndexUpdate(UpdateType.DROP, ObjectType.NODE, null, "BoltITSchema", "a, b"),
                MemgraphGraphDialect.INSTANCE));
    // Memgraph only accepts index and constraint changes in auto-commit transactions
    //
    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      for (String statement : statements) {
        connection.execute(statement, Map.of());
      }
      List<Map<String, Object>> constraints = connection.execute("SHOW CONSTRAINT INFO", Map.of());
      assertEquals(1, constraints.size(), constraints.toString());
    }
  }

  /**
   * The whole path of a Vector field: converted for the graph database, stored, found through a
   * Memgraph vector index, and read back into the float[] of a Vector field.
   */
  @Test
  void testVectorWriteSearchAndRead() throws Exception {
    IValueMeta vector = new ValueMetaBase("embedding", IValueMeta.TYPE_VECTOR) {};
    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      // The statement the Graph index action generates for Memgraph
      IndexUpdate index =
          IndexUpdate.vector(
              UpdateType.CREATE,
              ObjectType.NODE,
              "bolt_it_docs",
              "BoltITDoc",
              "embedding",
              "3",
              GraphVectorSimilarity.COSINE);
      connection.execute(
          Neo4jIndex.generateCreateIndexCypher(index, MemgraphGraphDialect.INSTANCE), Map.of());
      float[][] embeddings = {{1f, 0f, 0f}, {0f, 1f, 0f}};
      for (int i = 0; i < embeddings.length; i++) {
        Object value = GraphPropertyType.Vector.convertFromHop(vector, embeddings[i]);
        connection.execute(
            "MERGE (d:BoltITDoc {id: $id}) SET d.embedding = "
                + MemgraphGraphDialect.INSTANCE.vectorValue("$e"),
            Map.of("id", (long) i + 1, "e", value));
      }

      List<Map<String, Object>> nearest =
          connection.execute(
              "CALL vector_search.search('bolt_it_docs', 1, $q) YIELD node, similarity"
                  + " RETURN node.id AS id",
              Map.of(
                  "q",
                  GraphPropertyType.Vector.convertFromHop(vector, new float[] {0.9f, 0.1f, 0f})));
      assertEquals(1L, nearest.get(0).get("id"));

      Object stored =
          connection
              .execute("MATCH (d:BoltITDoc {id: 2}) RETURN d.embedding AS e", Map.of())
              .get(0)
              .get("e");
      assertArrayEquals(
          new float[] {0f, 1f, 0f},
          (float[]) NeoHopData.convertToHopValue("e", stored, vector),
          0f);

      index.setType(UpdateType.DROP);
      connection.execute(
          Neo4jIndex.generateDropIndexCypher(index, MemgraphGraphDialect.INSTANCE), Map.of());
    }
  }

  /**
   * Creating what exists and dropping what doesn't is not an error, as with IF [NOT] EXISTS on
   * Neo4j. Labels and properties with spaces and backticks are quoted.
   */
  @Test
  void testIdempotentQuotedSchemaStatements() throws Exception {
    NamedGraphConnection connection = new NamedGraphConnection("it", null, graphDatabaseMeta);
    IndexUpdate index =
        new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, null, "Bolt IT`Label", "my prop");
    IndexUpdate vector =
        IndexUpdate.vector(
            UpdateType.CREATE,
            ObjectType.NODE,
            "bolt it vectors",
            "Bolt IT`Label",
            "embed ding",
            "3",
            GraphVectorSimilarity.COSINE);
    IndexUpdate edgeVector =
        IndexUpdate.vector(
            UpdateType.CREATE,
            ObjectType.RELATIONSHIP,
            "bolt it edge vectors",
            "BOLT IT`TYPE",
            "embed ding",
            "3",
            GraphVectorSimilarity.EUCLIDEAN);
    ConstraintUpdate constraint =
        new ConstraintUpdate(
            org.apache.hop.neo4j.actions.constraint.UpdateType.CREATE,
            org.apache.hop.neo4j.actions.constraint.ObjectType.NODE,
            GraphConstraintType.UNIQUE,
            null,
            "Bolt IT`Label",
            "my id");
    MemgraphGraphDialect dialect = MemgraphGraphDialect.INSTANCE;
    List<String> creates =
        List.of(
            Neo4jIndex.generateCreateIndexCypher(index, dialect),
            Neo4jIndex.generateCreateIndexCypher(vector, dialect),
            Neo4jIndex.generateCreateIndexCypher(edgeVector, dialect),
            Neo4jConstraint.generateCreateConstraintCypher(constraint, dialect));
    List<String> drops =
        List.of(
            Neo4jIndex.generateDropIndexCypher(index, dialect),
            Neo4jIndex.generateDropIndexCypher(vector, dialect),
            Neo4jIndex.generateDropIndexCypher(edgeVector, dialect),
            Neo4jConstraint.generateDropConstraintCypher(constraint, dialect));
    for (List<String> statements : List.of(creates, creates, drops, drops)) {
      for (String statement : statements) {
        NeoConnectionUtils.runSchemaStatement(
            connection, LogChannel.GENERAL, variables, statement, "Schema change");
      }
    }
    // The errors Memgraph gives for what exists already or doesn't exist
    try (IGraphConnection graph = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      for (List<String> statements : List.of(creates, creates, drops, drops)) {
        for (String statement : statements) {
          try {
            graph.execute(statement, Map.of());
          } catch (Exception e) {
            System.out.println(
                "MEMGRAPH-ERROR [" + statement + "] " + Const.getSimpleStackTrace(e));
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

  /**
   * The schema from SHOW SCHEMA INFO, and the sample which Memgraph without schema information
   * falls back on, with the indexes.
   */
  @Test
  void testSchema() throws Exception {
    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
      connection.execute(
          "CREATE (:SchemaITPerson {name: 'a', age: 3})-[:SCHEMA_IT_KNOWS {since: 2020}]->"
              + "(:SchemaITPerson:SchemaITEmployee {name: 'b'})",
          Map.of());
      connection.execute(
          MemgraphGraphDialect.INSTANCE.getCreateNodeIndexStatement(
              null, "SchemaITPerson", List.of("name")),
          Map.of());

      GraphSchema schema = connection.getSchema(100);
      assertTrue(!schema.sampled(), "SHOW SCHEMA INFO is enabled");
      checkSchema(schema);
      assertEquals(true, schema.isIndexed(entry(schema, "SchemaITPerson", "name")));
      assertEquals(false, schema.isIndexed(entry(schema, "SchemaITPerson", "age")));

      GraphSchema sampled =
          GraphSchemaSampler.sample(connection, null, null, CypherGraphDialect::quote, 1000);
      assertTrue(sampled.sampled());
      checkSchema(sampled);
    }
  }

  private static void checkSchema(GraphSchema schema) {
    assertEquals(true, entry(schema, "SchemaITPerson", "name").mandatory());
    assertEquals(false, entry(schema, "SchemaITPerson", "age").mandatory());
    assertEquals(List.of("Integer"), entry(schema, "SchemaITPerson", "age").propertyTypes());
    assertEquals(List.of("String"), entry(schema, "SchemaITEmployee", "name").propertyTypes());
    GraphSchemaEntry since = entry(schema, "SCHEMA_IT_KNOWS", "since");
    assertTrue(since.isRelationship());
    assertEquals(true, since.mandatory());
    assertEquals(List.of("SchemaITPerson"), since.startLabels());
    assertTrue(since.endLabels().contains("SchemaITEmployee"), since.toString());
  }

  /**
   * The statement Graph vector search runs ranks the nodes by similarity, scored the same way on
   * every database: the cosine similarity, or 1 / (1 + squared euclidean distance).
   */
  @Test
  void testVectorSearch() throws Exception {
    MemgraphGraphDialect dialect = MemgraphGraphDialect.INSTANCE;
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

        GraphStatement search =
            dialect.getVectorSearchStatement(
                new GraphVectorSearchDefinition(
                    indexName, null, null, 2, List.of("id"), similarity));
        Map<String, Object> parameters = new HashMap<>(search.parameters());
        parameters.put(GraphVectorSearchDefinition.PARAMETER_VECTOR, List.of(0.9, 0.1, 0.0));
        List<Map<String, Object>> hits = connection.execute(search.statement(), parameters);
        System.out.println("MEMGRAPH-VECTOR-SEARCH " + similarity + " " + hits);
        assertEquals(2, hits.size(), hits.toString());
        assertEquals(1L, hits.get(0).get("p0"));
        assertEquals(3L, hits.get(1).get("p0"));
        assertScores(similarity, hits);
        if (similarity == GraphVectorSimilarity.COSINE) {
          assertOppositeVectorsRankLast(connection, search);
        }

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
    MemgraphGraphDialect dialect = MemgraphGraphDialect.INSTANCE;
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
      System.out.println("MEMGRAPH-RELATIONSHIP-VECTOR-SEARCH " + hits);
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

  /** The vectors are stored as 32 bit floats. */
  private static final double SCORE_TOLERANCE = 1e-4;

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
    try (IGraphConnection connection = graphDatabaseMeta.connect(LogChannel.GENERAL, variables)) {
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
