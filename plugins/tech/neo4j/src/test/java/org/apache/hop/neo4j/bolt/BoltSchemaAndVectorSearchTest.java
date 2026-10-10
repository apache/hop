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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.junit.jupiter.api.Test;

/** Schema introspection and vector search statements of Neo4j, Memgraph and Neptune. */
class BoltSchemaAndVectorSearchTest {

  /** Answers statements with canned rows, fails the others. */
  static class ScriptedConnection implements IGraphConnection {
    final Map<String, List<Map<String, Object>>> answers = new LinkedHashMap<>();
    final List<String> executed = new ArrayList<>();
    final IGraphDialect dialect;

    ScriptedConnection(IGraphDialect dialect) {
      this.dialect = dialect;
    }

    @Override
    public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
        throws HopException {
      executed.add(statement);
      List<Map<String, Object>> rows = answers.get(statement);
      if (rows == null) {
        throw new HopException("Unknown statement: " + statement);
      }
      return rows;
    }

    @Override
    public IGraphTransaction beginTransaction() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T executeWrite(IGraphTransactionWork<T> work) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IGraphDialect getGraphDialect() {
      return dialect;
    }

    @Override
    public void close() {}
  }

  static Map<String, Object> row(Object... keyValues) {
    Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      row.put((String) keyValues[i], keyValues[i + 1]);
    }
    return row;
  }

  static GraphSchemaEntry entry(GraphSchema schema, String name, String property) {
    return schema.entries().stream()
        .filter(e -> e.name().equals(name) && Objects.equals(e.property(), property))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + "." + property + " not in " + schema));
  }

  /** The rows Neo4j 5.26 returns for a small graph. */
  @Test
  void testNeo4jSchema() throws HopException {
    ScriptedConnection connection = new ScriptedConnection(Neo4jGraphDialect.INSTANCE);
    connection.answers.put(
        "CALL db.schema.nodeTypeProperties()",
        List.of(
            row(
                "nodeType",
                ":`Empty`",
                "nodeLabels",
                List.of("Empty"),
                "propertyName",
                null,
                "propertyTypes",
                null,
                "mandatory",
                false),
            row(
                "nodeType",
                ":`Employee`:`Person`",
                "nodeLabels",
                List.of("Employee", "Person"),
                "propertyName",
                "name",
                "propertyTypes",
                List.of("String"),
                "mandatory",
                true),
            row(
                "nodeType",
                ":`Person`",
                "nodeLabels",
                List.of("Person"),
                "propertyName",
                "name",
                "propertyTypes",
                List.of("String"),
                "mandatory",
                true),
            row(
                "nodeType",
                ":`Person`",
                "nodeLabels",
                List.of("Person"),
                "propertyName",
                "emb",
                "propertyTypes",
                List.of("DoubleArray"),
                "mandatory",
                true)));
    connection.answers.put(
        "CALL db.schema.relTypeProperties()",
        List.of(
            row(
                "relType",
                ":`KNOWS`",
                "propertyName",
                "since",
                "propertyTypes",
                List.of("Long"),
                "mandatory",
                true),
            row(
                "relType",
                ":`LIKES`",
                "propertyName",
                null,
                "propertyTypes",
                null,
                "mandatory",
                false)));
    connection.answers.put(
        "CALL db.schema.visualization()",
        List.of(
            row(
                "nodes",
                List.of(
                    new GraphNodeValue("-1", List.of("Person"), Map.of()),
                    new GraphNodeValue("-2", List.of("Employee"), Map.of())),
                "relationships",
                List.of(
                    new GraphRelationshipValue("-3", "KNOWS", "-1", "-1", Map.of()),
                    new GraphRelationshipValue("-4", "KNOWS", "-1", "-2", Map.of())))));

    GraphSchema schema = connection.getSchema(10);

    assertFalse(schema.sampled());
    assertNull(entry(schema, "Empty", null).mandatory());
    assertEquals(true, entry(schema, "Person", "name").mandatory());
    // Not in the combination with Employee
    assertEquals(false, entry(schema, "Person", "emb").mandatory());
    assertEquals(List.of("List<Float>"), entry(schema, "Person", "emb").propertyTypes());
    assertEquals(true, entry(schema, "Employee", "name").mandatory());
    GraphSchemaEntry since = entry(schema, "KNOWS", "since");
    assertEquals(GraphObjectType.RELATIONSHIP, since.elementType());
    assertEquals(List.of("Integer"), since.propertyTypes());
    assertEquals(List.of("Person"), since.startLabels());
    assertEquals(List.of("Person", "Employee"), since.endLabels());
    assertTrue(entry(schema, "LIKES", null).startLabels().isEmpty());
    // This connection doesn't list indexes
    assertNull(schema.indexes());
  }

  /** Without the schema procedures, a sample. */
  @Test
  void testNeo4jSchemaFallback() throws HopException {
    ScriptedConnection connection = new ScriptedConnection(Neo4jGraphDialect.INSTANCE);
    connection.answers.put(
        "MATCH (n) WITH n LIMIT 5 RETURN labels(n) AS labels, properties(n) AS properties",
        List.of(row("labels", List.of("A"), "properties", Map.of("x", 1L))));
    connection.answers.put(
        "MATCH (a)-[r]->(b) WITH a, r, b LIMIT 5 RETURN type(r) AS type, labels(a) AS"
            + " startLabels, labels(b) AS endLabels, properties(r) AS properties",
        List.of());

    GraphSchema schema = Neo4jGraphDialect.INSTANCE.getSchema(connection, 5);

    assertTrue(schema.sampled());
    assertEquals(List.of("Integer"), entry(schema, "A", "x").propertyTypes());
  }

  /** The JSON of SHOW SCHEMA INFO of Memgraph 3.6. */
  @Test
  void testMemgraphSchema() throws HopException {
    String json =
        """
        {"edge_indexes":[],"edges":[{"count":1,"end_node_labels":["Employee","Person"],\
        "properties":[{"count":1,"filling_factor":100.0,"key":"since","types":[{"count":1,\
        "type":"Integer"}]}],"start_node_labels":["Person"],"type":"KNOWS"}],"enums":[],\
        "node_constraints":[],"node_indexes":[],"nodes":[{"count":1,"labels":["Empty"],\
        "properties":[]},{"count":2,"labels":["Person"],"properties":[{"count":2,\
        "filling_factor":100.0,"key":"name","types":[{"count":2,"type":"String"}]},\
        {"count":1,"filling_factor":50.0,"key":"emb","types":[{"count":1,"type":"List"}]}]},\
        {"count":1,"labels":["Employee","Person"],"properties":[{"count":1,"filling_factor":100.0,\
        "key":"name","types":[{"count":1,"type":"String"}]}]}]}""";
    ScriptedConnection connection = new ScriptedConnection(MemgraphGraphDialect.INSTANCE);
    connection.answers.put("SHOW SCHEMA INFO", List.of(row("schema", json)));

    GraphSchema schema = MemgraphGraphDialect.INSTANCE.getSchema(connection, 5);

    assertFalse(schema.sampled());
    assertNull(entry(schema, "Empty", null).property());
    assertEquals(true, entry(schema, "Person", "name").mandatory());
    assertEquals(false, entry(schema, "Person", "emb").mandatory());
    assertEquals(List.of("List"), entry(schema, "Person", "emb").propertyTypes());
    assertEquals(true, entry(schema, "Employee", "name").mandatory());
    GraphSchemaEntry since = entry(schema, "KNOWS", "since");
    assertEquals(List.of("Person"), since.startLabels());
    assertEquals(List.of("Employee", "Person"), since.endLabels());
  }

  /** Without --schema-info-enabled, a sample. */
  @Test
  void testMemgraphSchemaFallback() throws HopException {
    ScriptedConnection connection = new ScriptedConnection(MemgraphGraphDialect.INSTANCE);
    connection.answers.put(
        "MATCH (n) WITH n LIMIT 1000 RETURN labels(n) AS labels, properties(n) AS properties",
        List.of(row("labels", List.of("A"), "properties", Map.of("x", "y"))));
    connection.answers.put(
        "MATCH (a)-[r]->(b) WITH a, r, b LIMIT 1000 RETURN type(r) AS type, labels(a) AS"
            + " startLabels, labels(b) AS endLabels, properties(r) AS properties",
        List.of());

    GraphSchema schema = MemgraphGraphDialect.INSTANCE.getSchema(connection, 0);

    assertEquals("SHOW SCHEMA INFO", connection.executed.get(0));
    assertTrue(schema.sampled());
    assertEquals(List.of("String"), entry(schema, "A", "x").propertyTypes());
  }

  @Test
  void testUnquoteType() {
    assertEquals("KNOWS", BoltSchemas.unquoteType(":`KNOWS`"));
    assertEquals("we`ird", BoltSchemas.unquoteType(":`we``ird`"));
    assertEquals("PLAIN", BoltSchemas.unquoteType("PLAIN"));
  }

  @Test
  void testVectorSearchStatements() throws HopException {
    GraphVectorSearchDefinition byName =
        new GraphVectorSearchDefinition("doc index", null, null, 3, List.of("id"), null);

    GraphStatement neo4j = Neo4jGraphDialect.INSTANCE.getVectorSearchStatement(byName);
    assertEquals(
        "CALL db.index.vector.queryNodes($index, $k, $vector) YIELD node, score"
            + " RETURN 2 * score - 1 AS score, node.`id` AS p0 ORDER BY score DESC",
        neo4j.statement());
    assertEquals(Map.of("index", "doc index", "k", 3L), neo4j.parameters());

    GraphStatement memgraph = MemgraphGraphDialect.INSTANCE.getVectorSearchStatement(byName);
    assertEquals(
        "CALL vector_search.search($index, $k, $vector) YIELD node, distance"
            + " RETURN 1.0 - distance AS score, node.`id` AS p0 ORDER BY score DESC",
        memgraph.statement());
    assertEquals(Map.of("index", "doc index", "k", 3L), memgraph.parameters());

    // Euclidean: Neo4j's score is 1 / (1 + squared distance), Memgraph's l2sq distance is squared
    GraphVectorSearchDefinition euclidean =
        new GraphVectorSearchDefinition(
            "doc index", null, null, 3, List.of("id"), GraphVectorSimilarity.EUCLIDEAN);
    assertTrue(
        Neo4jGraphDialect.INSTANCE
            .getVectorSearchStatement(euclidean)
            .statement()
            .contains("YIELD node, score RETURN score AS score,"));
    assertTrue(
        MemgraphGraphDialect.INSTANCE
            .getVectorSearchStatement(euclidean)
            .statement()
            .contains("YIELD node, distance RETURN 1.0 / (1.0 + distance) AS score,"));

    assertTrue(Neo4jGraphDialect.INSTANCE.isSupportingVectorSearch());
    assertTrue(MemgraphGraphDialect.INSTANCE.isSupportingVectorSearch());
    assertFalse(NeptuneGraphDialect.INSTANCE.isSupportingVectorSearch());
    HopException neptune =
        assertThrows(
            HopException.class,
            () -> NeptuneGraphDialect.INSTANCE.getVectorSearchStatement(byName));
    assertTrue(neptune.getMessage().contains("NEPTUNE"), neptune.getMessage());

    // Neo4j and Memgraph search by index name
    GraphVectorSearchDefinition byLabel =
        new GraphVectorSearchDefinition(null, "Doc", "embedding", 3, null, null);
    assertThrows(
        HopException.class, () -> MemgraphGraphDialect.INSTANCE.getVectorSearchStatement(byLabel));
  }

  @Test
  void testRelationshipVectorSearchStatements() throws HopException {
    GraphVectorSearchDefinition byName =
        new GraphVectorSearchDefinition(
            "rel index",
            null,
            null,
            3,
            List.of("id", "my weight"),
            null,
            GraphObjectType.RELATIONSHIP);

    assertTrue(Neo4jGraphDialect.INSTANCE.isSupportingRelationshipVectorSearch());
    GraphStatement neo4j = Neo4jGraphDialect.INSTANCE.getVectorSearchStatement(byName);
    assertEquals(
        "CALL db.index.vector.queryRelationships($index, $k, $vector) YIELD relationship, score"
            + " RETURN 2 * score - 1 AS score, relationship.`id` AS p0,"
            + " relationship.`my weight` AS p1"
            + " ORDER BY score DESC",
        neo4j.statement());
    assertEquals(Map.of("index", "rel index", "k", 3L), neo4j.parameters());

    assertTrue(MemgraphGraphDialect.INSTANCE.isSupportingRelationshipVectorSearch());
    GraphStatement memgraph = MemgraphGraphDialect.INSTANCE.getVectorSearchStatement(byName);
    assertEquals(
        "CALL vector_search.search_edges($index, $k, $vector) YIELD edge, distance"
            + " RETURN 1.0 - distance AS score, edge.`id` AS p0, edge.`my weight` AS p1"
            + " ORDER BY score DESC",
        memgraph.statement());
    assertEquals(Map.of("index", "rel index", "k", 3L), memgraph.parameters());

    assertFalse(NeptuneGraphDialect.INSTANCE.isSupportingRelationshipVectorSearch());
    assertThrows(
        HopException.class, () -> NeptuneGraphDialect.INSTANCE.getVectorSearchStatement(byName));

    // Searching relationships is refused by a dialect which only searches nodes
    MemgraphGraphDialect nodesOnly =
        new MemgraphGraphDialect() {
          @Override
          public boolean isSupportingRelationshipVectorSearch() {
            return false;
          }
        };
    HopException e =
        assertThrows(HopException.class, () -> nodesOnly.getVectorSearchStatement(byName));
    assertTrue(e.getMessage().contains("over relationships"), e.getMessage());
    // Nodes are still searched
    nodesOnly.getVectorSearchStatement(
        new GraphVectorSearchDefinition("doc index", null, null, 3, null, null));
  }

  /** Neptune reads its schema from a sample, like every Cypher database without a catalog. */
  @Test
  void testNeptuneSchemaIsSampled() throws HopException {
    ScriptedConnection connection = new ScriptedConnection(NeptuneGraphDialect.INSTANCE);
    connection.answers.put(
        "MATCH (n) WITH n LIMIT 7 RETURN labels(n) AS labels, properties(n) AS properties",
        List.of());
    connection.answers.put(
        "MATCH (a)-[r]->(b) WITH a, r, b LIMIT 7 RETURN type(r) AS type, labels(a) AS"
            + " startLabels, labels(b) AS endLabels, properties(r) AS properties",
        List.of(
            row(
                "type",
                "R",
                "startLabels",
                List.of("A"),
                "endLabels",
                List.of("B"),
                "properties",
                Map.of())));
    assertTrue(NeptuneGraphDialect.INSTANCE.isSupportingSchemaIntrospection());
    GraphSchema schema = NeptuneGraphDialect.INSTANCE.getSchema(connection, 7);
    assertEquals(List.of("A"), entry(schema, "R", null).startLabels());
  }
}
