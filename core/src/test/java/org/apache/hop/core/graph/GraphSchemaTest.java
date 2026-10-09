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

package org.apache.hop.core.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class GraphSchemaTest {

  /** A connection answering statements with canned rows, recording the statements. */
  static class ScriptedConnection implements IGraphConnection {
    final Map<String, List<Map<String, Object>>> answers = new LinkedHashMap<>();
    final List<String> executed = new ArrayList<>();
    IGraphDialect dialect = CypherGraphDialect.DEFAULT;
    List<GraphIndex> indexes;

    @Override
    public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
        throws HopException {
      executed.add(statement);
      List<Map<String, Object>> rows = answers.get(statement);
      if (rows == null) {
        throw new HopException("Unexpected statement: " + statement);
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
    public List<GraphIndex> getIndexes() {
      return indexes;
    }

    @Override
    public void close() {}
  }

  private static Map<String, Object> row(Object... keyValues) {
    Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      row.put((String) keyValues[i], keyValues[i + 1]);
    }
    return row;
  }

  private static GraphSchemaEntry entry(GraphSchema schema, String name, String property) {
    return schema.entries().stream()
        .filter(e -> e.name().equals(name) && java.util.Objects.equals(e.property(), property))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + "." + property + " in " + schema));
  }

  @Test
  void testTypeNames() {
    assertEquals("String", GraphSchemaBuilder.typeName("a"));
    assertEquals("Integer", GraphSchemaBuilder.typeName(1L));
    assertEquals("Float", GraphSchemaBuilder.typeName(1.5d));
    assertEquals("Boolean", GraphSchemaBuilder.typeName(true));
    assertEquals("Date", GraphSchemaBuilder.typeName(LocalDate.now()));
    assertEquals("DateTime", GraphSchemaBuilder.typeName(ZonedDateTime.now()));
    assertEquals("DateTime", GraphSchemaBuilder.typeName(new java.util.Date()));
    assertEquals("Vector", GraphSchemaBuilder.typeName(new float[] {1f}));
    assertEquals("List<Float>", GraphSchemaBuilder.typeName(List.of(1.0, 2.0)));
    assertEquals("List", GraphSchemaBuilder.typeName(List.of(1.0, "a")));
    assertEquals("List", GraphSchemaBuilder.typeName(List.of()));
    assertEquals("Map", GraphSchemaBuilder.typeName(Map.of("a", 1)));
    assertNull(GraphSchemaBuilder.typeName(null));

    assertEquals("Integer", GraphSchemaBuilder.normalizeTypeName("Long"));
    assertEquals("Integer", GraphSchemaBuilder.normalizeTypeName("Int"));
    assertEquals("Float", GraphSchemaBuilder.normalizeTypeName("Double"));
    // The GQL type names of Neo4j 2025 and later
    assertEquals("String", GraphSchemaBuilder.normalizeTypeName("STRING NOT NULL"));
    assertEquals("Integer", GraphSchemaBuilder.normalizeTypeName("INTEGER"));
    assertEquals("DateTime", GraphSchemaBuilder.normalizeTypeName("ZONED DATETIME NOT NULL"));
    assertEquals(
        "List<String>", GraphSchemaBuilder.normalizeTypeName("LIST<STRING NOT NULL> NOT NULL"));
    assertEquals("List", GraphSchemaBuilder.normalizeTypeName("LIST<ANY>"));
    assertEquals("Vector", GraphSchemaBuilder.normalizeTypeName("VECTOR<FLOAT32>(3) NOT NULL"));
    assertEquals("List<String>", GraphSchemaBuilder.normalizeTypeName("StringArray"));
    assertEquals("List<Float>", GraphSchemaBuilder.normalizeTypeName("DoubleArray"));
    assertEquals("List", GraphSchemaBuilder.normalizeTypeName("List[Any]"));
    assertEquals("List<Integer>", GraphSchemaBuilder.normalizeTypeName("List[Int]"));
    assertEquals("DateTime", GraphSchemaBuilder.normalizeTypeName("ZonedDateTime"));
    assertEquals("ByteArray", GraphSchemaBuilder.normalizeTypeName("ByteArray"));
  }

  /** A property is mandatory when every sampled node of the label has it. */
  @Test
  void testSampledMandatory() {
    GraphSchemaBuilder builder = new GraphSchemaBuilder();
    builder.addSampledNode("Person", Map.of("name", "a", "age", 3L));
    builder.addSampledNode("Person", row("name", "b", "age", null));
    builder.addElement(GraphObjectType.NODE, "Empty");
    builder.addSampledRelationship(
        "KNOWS", List.of("Person"), List.of("Person", "Employee"), Map.of());
    GraphSchema schema = builder.build(null, true);

    assertTrue(schema.sampled());
    assertEquals(true, entry(schema, "Person", "name").mandatory());
    assertEquals(false, entry(schema, "Person", "age").mandatory());
    assertEquals(List.of("Integer"), entry(schema, "Person", "age").propertyTypes());
    GraphSchemaEntry empty = entry(schema, "Empty", null);
    assertNull(empty.mandatory());
    GraphSchemaEntry knows = entry(schema, "KNOWS", null);
    assertTrue(knows.isRelationship());
    assertEquals(List.of("Person"), knows.startLabels());
    assertEquals(List.of("Person", "Employee"), knows.endLabels());
    // Nodes first, sorted by label
    assertEquals("Empty", schema.entries().get(0).name());
    assertEquals(GraphObjectType.RELATIONSHIP, schema.entries().get(3).elementType());
    // Indexes unknown
    assertNull(schema.isIndexed(entry(schema, "Person", "name")));
  }

  /**
   * Label combinations as Neo4j describes them: mandatory for a label only if mandatory in every
   * combination with the label.
   */
  @Test
  void testGroupMandatory() {
    GraphSchemaBuilder builder = new GraphSchemaBuilder();
    builder.addGroupProperty(
        GraphObjectType.NODE, "Person", ":Person", "name", List.of("String"), true);
    builder.addGroupProperty(
        GraphObjectType.NODE, "Person", ":Person", "age", List.of("Long"), true);
    builder.addGroupProperty(
        GraphObjectType.NODE, "Person", ":Employee:Person", "name", List.of("String"), true);
    builder.addGroupProperty(
        GraphObjectType.NODE, "Employee", ":Employee:Person", "name", List.of("String"), true);
    builder.addGroupProperty(GraphObjectType.NODE, "Thing", ":Thing", "x", List.of("Long"), null);
    GraphSchema schema = builder.build(List.of(), false);

    assertEquals(true, entry(schema, "Person", "name").mandatory());
    assertEquals(false, entry(schema, "Person", "age").mandatory());
    assertEquals(List.of("Integer"), entry(schema, "Person", "age").propertyTypes());
    assertEquals(true, entry(schema, "Employee", "name").mandatory());
    assertNull(entry(schema, "Thing", "x").mandatory());
    assertFalse(schema.sampled());
  }

  @Test
  void testIndexes() {
    GraphSchema schema =
        new GraphSchemaBuilder()
            .addSampledNode("Person", Map.of("id", 1L, "name", "a"))
            .build(
                List.of(
                    new GraphIndex("id", false, List.of("Person"), List.of("id"), true),
                    new GraphIndex("name", true, List.of("Person"), List.of("name"), false)),
                true);
    assertEquals(true, schema.isIndexed(entry(schema, "Person", "id")));
    assertEquals(true, schema.isUnique(entry(schema, "Person", "id")));
    // The index on name is on relationships
    assertEquals(false, schema.isIndexed(entry(schema, "Person", "name")));
  }

  /** Without lists of labels and types, the first nodes and relationships are sampled. */
  @Test
  void testSampleAnyLabel() throws HopException {
    ScriptedConnection connection = new ScriptedConnection();
    connection.answers.put(
        "MATCH (n) WITH n LIMIT 10 RETURN labels(n) AS labels, properties(n) AS properties",
        List.of(
            row("labels", List.of("Person", "Employee"), "properties", Map.of("name", "a")),
            row("labels", List.of("Person"), "properties", Map.of("name", "b", "age", 3L))));
    connection.answers.put(
        "MATCH (a)-[r]->(b) WITH a, r, b LIMIT 10 RETURN type(r) AS type, labels(a) AS startLabels,"
            + " labels(b) AS endLabels, properties(r) AS properties",
        List.of(
            row(
                "type",
                "KNOWS",
                "startLabels",
                List.of("Person"),
                "endLabels",
                List.of("Person"),
                "properties",
                Map.of("since", 2020L))));
    connection.indexes =
        List.of(new GraphIndex("", false, List.of("Person"), List.of("name"), false));

    GraphSchema schema = connection.getSchema(10);

    assertTrue(schema.sampled());
    assertEquals(true, entry(schema, "Person", "name").mandatory());
    assertEquals(false, entry(schema, "Person", "age").mandatory());
    assertEquals(true, entry(schema, "Employee", "name").mandatory());
    assertEquals(List.of("Integer"), entry(schema, "KNOWS", "since").propertyTypes());
    assertEquals(true, schema.isIndexed(entry(schema, "Person", "name")));
  }

  /** With lists of labels and types every one is sampled on its own, with quoted names. */
  @Test
  void testSamplePerLabel() throws HopException {
    ScriptedConnection connection = new ScriptedConnection();
    connection.answers.put(
        "MATCH (n:`My Label`) WITH n LIMIT 1000 RETURN properties(n) AS properties",
        List.of(row("properties", Map.of("a", "x"))));
    connection.answers.put(
        "MATCH (n:`Unused`) WITH n LIMIT 1000 RETURN properties(n) AS properties", List.of());
    connection.answers.put(
        "MATCH (a)-[r:`REL`]->(b) WITH a, r, b LIMIT 1000 RETURN labels(a) AS startLabels,"
            + " labels(b) AS endLabels, properties(r) AS properties",
        List.of(
            row("startLabels", List.of("My Label"), "endLabels", List.of(), "properties", null)));

    GraphSchema schema =
        GraphSchemaSampler.sample(
            connection,
            List.of("My Label", "Unused"),
            List.of("REL"),
            CypherGraphDialect::quote,
            0);

    assertEquals(true, entry(schema, "My Label", "a").mandatory());
    assertNull(entry(schema, "Unused", null).property());
    assertEquals(List.of("My Label"), entry(schema, "REL", null).startLabels());
  }

  @Test
  void testNotSupported() {
    IGraphDialect gremlin =
        new IGraphDialect() {
          @Override
          public String getId() {
            return "NOPE";
          }

          @Override
          public boolean isCypher() {
            return false;
          }
        };
    assertFalse(gremlin.isSupportingSchemaIntrospection());
    assertThrows(HopException.class, () -> gremlin.getSchema(new ScriptedConnection(), 10));
    assertFalse(gremlin.isSupportingVectorSearch());
    assertThrows(
        HopException.class,
        () ->
            gremlin.getVectorSearchStatement(
                new GraphVectorSearchDefinition("i", null, null, 5, null, null)));
  }

  @Test
  void testCypherVectorSearchStatement() throws HopException {
    GraphStatement statement =
        CypherGraphDialect.DEFAULT.getVectorSearchStatement(
            new GraphVectorSearchDefinition("docs", null, null, 5, List.of("id", "my text"), null));
    assertEquals(
        "CALL db.index.vector.queryNodes($index, $k, $vector) YIELD node, score"
            + " RETURN 2 * score - 1 AS score, node.`id` AS p0, node.`my text` AS p1"
            + " ORDER BY score DESC",
        statement.statement());
    assertEquals(Map.of("index", "docs", "k", 5L), statement.parameters());
    // Neo4j's euclidean score is 1 / (1 + squared distance) already
    assertTrue(
        CypherGraphDialect.DEFAULT
            .getVectorSearchStatement(
                new GraphVectorSearchDefinition(
                    "docs", null, null, 5, null, GraphVectorSimilarity.EUCLIDEAN))
            .statement()
            .contains("YIELD node, score RETURN score AS score ORDER BY"));
    assertTrue(CypherGraphDialect.DEFAULT.isSupportingVectorSearch());

    // The index name is needed, and at least one hit
    assertThrows(
        HopException.class,
        () ->
            CypherGraphDialect.DEFAULT.getVectorSearchStatement(
                new GraphVectorSearchDefinition("", "Doc", "e", 5, null, null)));
    assertThrows(
        HopException.class,
        () ->
            CypherGraphDialect.DEFAULT.getVectorSearchStatement(
                new GraphVectorSearchDefinition("docs", null, null, 0, null, null)));
  }
}
