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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hop.core.graph.GraphConstraintDefinition;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.core.graph.GraphIndexDefinition;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphVectorIndexDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphDialect;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Characterization of the statements of the Apache AGE dialect, recorded before the dialect SPI
 * with the Graph index and Graph constraint actions of the Neo4j plugin. The same cases as
 * GraphDialectCharacterizationTest in the Neo4j plugin, with the definitions those actions build.
 */
class AgeGraphDialectCharacterizationTest {

  static final String ERROR = "!ERROR";

  static final IGraphDialect DIALECT = new AgeGraphDialect("hop_graph");

  static Map<String, Callable<Object>> cases() {
    GraphObjectType node = GraphObjectType.NODE;
    GraphObjectType rel = GraphObjectType.RELATIONSHIP;
    Map<String, GraphIndexDefinition> indexes = new LinkedHashMap<>();
    indexes.put("A", new GraphIndexDefinition("idx", node, "Person", List.of("name", "age")));
    indexes.put("B", new GraphIndexDefinition("idx", rel, "KNOWS", List.of("since")));
    indexes.put("C", new GraphIndexDefinition("", node, "Person", List.of("name")));
    indexes.put("D", new GraphIndexDefinition("idx", rel, "KNOWS", List.of("a", "b")));

    // Create with the dimensions of the action (null or not positive when it would refuse them),
    // drop without
    GraphVectorSimilarity cosine = GraphVectorSimilarity.COSINE;
    GraphVectorSimilarity euclidean = GraphVectorSimilarity.EUCLIDEAN;
    Map<String, Object[]> vectors = new LinkedHashMap<>();
    vectors.put("V1", new Object[] {"doc_vectors", node, List.of("embedding"), 768, cosine, null});
    vectors.put("V2", new Object[] {"doc_vectors", rel, List.of("embedding"), 3, euclidean, null});
    vectors.put("V3", new Object[] {"", node, List.of("embedding"), 3, cosine, null});
    vectors.put("V4", new Object[] {"doc_vectors", node, List.of("embedding"), 3, euclidean, 50});
    vectors.put("V5", new Object[] {"doc_vectors", node, List.of("embedding"), null, cosine, null});
    vectors.put("V6", new Object[] {"doc_vectors", node, List.of("embedding"), -1, cosine, null});
    vectors.put("V7", new Object[] {"doc_vectors", node, List.of("a", "b"), 3, cosine, null});

    GraphConstraintType unique = GraphConstraintType.UNIQUE;
    GraphConstraintType notNull = GraphConstraintType.NOT_NULL;
    GraphConstraintType nodeKey = GraphConstraintType.NODE_KEY;
    Map<String, GraphConstraintDefinition> constraints = new LinkedHashMap<>();
    constraints.put(
        "K1", new GraphConstraintDefinition("c", node, unique, "Person", List.of("id")));
    constraints.put(
        "K2", new GraphConstraintDefinition("c", node, notNull, "Person", List.of("id")));
    constraints.put(
        "K3", new GraphConstraintDefinition("c", node, nodeKey, "Person", List.of("a", "b")));
    constraints.put("K4", new GraphConstraintDefinition("c", rel, unique, "KNOWS", List.of("id")));
    constraints.put("K5", new GraphConstraintDefinition("c", rel, notNull, "KNOWS", List.of("id")));
    constraints.put("K6", new GraphConstraintDefinition("c", rel, nodeKey, "KNOWS", List.of("id")));
    constraints.put(
        "K7", new GraphConstraintDefinition("c", node, unique, "Person", List.of("a", "b")));
    constraints.put(
        "K8", new GraphConstraintDefinition("c", node, notNull, "Person", List.of("a", "b")));
    constraints.put("K9", new GraphConstraintDefinition("", node, unique, "Person", List.of("id")));

    Map<String, List<List<String>>> nodeKeys = new LinkedHashMap<>();
    nodeKeys.put("N1", List.of(List.of("Person"), List.of("id")));
    nodeKeys.put("N2", List.of(List.of("Person", "Actor"), List.of("first", "last")));
    nodeKeys.put("N3", List.of(List.of("Person"), List.of()));
    nodeKeys.put("N4", List.of(List.of(), List.of("id")));

    List<String> statements =
        List.of(
            "SHOW INDEX INFO",
            "  show constraint info",
            "CREATE INDEX ON :Person(id)",
            "CREATE EDGE INDEX ON :KNOWS(since)",
            "DROP CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE",
            "DROP ALL INDEXES",
            "MATCH (n:Showcase) RETURN n",
            "CREATE (n:Index {id: 1})",
            "SHOW INDEXES",
            "// list the indexes\nSHOW INDEX INFO",
            "/* indexes */ CREATE INDEX ON :Person(id)",
            "// SHOW INDEX INFO\nMATCH (n) RETURN n");

    IGraphDialect d = DIALECT;
    String p = d.getId() + "|";
    Map<String, Callable<Object>> cases = new LinkedHashMap<>();
    for (var e : indexes.entrySet()) {
      cases.put(p + "index.create|" + e.getKey(), () -> d.getCreateIndexStatement(e.getValue()));
      cases.put(p + "index.drop|" + e.getKey(), () -> d.getDropIndexStatement(e.getValue()));
    }
    for (var e : vectors.entrySet()) {
      Object[] v = e.getValue();
      cases.put(
          p + "vector.create|" + e.getKey(),
          () -> d.getCreateVectorIndexStatement(vector(v, (Integer) v[3])));
      cases.put(
          p + "vector.drop|" + e.getKey(), () -> d.getDropVectorIndexStatement(vector(v, null)));
    }
    for (var e : constraints.entrySet()) {
      cases.put(
          p + "constraint.create|" + e.getKey(),
          () -> d.getCreateConstraintStatement(e.getValue()));
      cases.put(
          p + "constraint.drop|" + e.getKey(), () -> d.getDropConstraintStatement(e.getValue()));
    }
    for (var e : nodeKeys.entrySet()) {
      List<String> labels = e.getValue().get(0);
      List<String> keys = e.getValue().get(1);
      // The Neo4j plugin asks the dialect only for a label and key properties
      cases.put(
          p + "nodekey.create|" + e.getKey(),
          () ->
              labels.isEmpty() || keys.isEmpty()
                  ? null
                  : d.getCreateNodeKeyIndexStatement(labels.get(0), keys));
    }
    for (int i = 0; i < statements.size(); i++) {
      String statement = statements.get(i);
      cases.put(p + "autocommit|S" + (i + 1), () -> d.isRequiringAutoCommit(statement));
    }
    cases.put(p + "vectorValue|$p", () -> d.vectorValue("$p"));
    cases.put(p + "vectorValue|pr.p", () -> d.vectorValue("pr.p"));
    cases.put(p + "flag|cypher", d::isCypher);
    cases.put(p + "flag|nodeIndexes", d::isSupportingNodeIndexes);
    cases.put(p + "flag|relationshipIndexes", d::isSupportingRelationshipIndexes);
    cases.put(p + "flag|schemaChangesInTransactions", d::isSupportingSchemaChangesInTransactions);
    cases.put(p + "flag|vectorIndexes", d::isSupportingVectorIndexes);
    cases.put(p + "flag|nodeConstraintTypes", () -> d.getNodeConstraintTypes().toString());
    cases.put(
        p + "flag|relationshipConstraintTypes",
        () -> d.getRelationshipConstraintTypes().toString());
    // The execution paths of the Neo4j plugin only depend on shortestPath() support
    cases.put(p + "path|toRoot", () -> d.isSupportingShortestPath() ? "shortestPath" : "generic");
    cases.put(p + "path|toFailed", () -> d.isSupportingShortestPath() ? "shortestPath" : "generic");
    cases.put(
        p + "executionIndex|one",
        () -> d.getCreateNodeIndexStatement("idx_execution_id", "Execution", List.of("id")));
    cases.put(
        p + "executionIndex|two",
        () ->
            d.getCreateNodeIndexStatement(
                "idx_execution_id", "Execution", List.of("name", "type")));
    return cases;
  }

  @SuppressWarnings("unchecked")
  static GraphVectorIndexDefinition vector(Object[] v, Integer dimensions) {
    return new GraphVectorIndexDefinition(
        (String) v[0],
        (GraphObjectType) v[1],
        "Doc",
        (List<String>) v[2],
        dimensions,
        (GraphVectorSimilarity) v[4],
        (Integer) v[5]);
  }

  static String run(Callable<Object> callable) {
    try {
      return String.valueOf(callable.call());
    } catch (Exception e) {
      return ERROR;
    }
  }

  @TestFactory
  List<DynamicTest> characterization() throws Exception {
    Map<String, Callable<Object>> cases = cases();
    String dump = System.getProperty("characterization.dump");
    if (dump != null) {
      StringBuilder out = new StringBuilder();
      for (var e : cases.entrySet()) {
        out.append(e.getKey())
            .append('\t')
            .append(run(e.getValue()).replace("\n", "\\n"))
            .append('\n');
      }
      Files.writeString(Path.of(dump), out.toString());
    }
    Map<String, String> expected = expected();
    List<DynamicTest> tests = new ArrayList<>();
    for (var e : cases.entrySet()) {
      tests.add(
          DynamicTest.dynamicTest(
              e.getKey(), () -> assertEquals(expected.get(e.getKey()), run(e.getValue()))));
    }
    return tests;
  }

  /**
   * Changed on purpose since the dialect SPI: AGE|executionIndex: the SQL index statement, as for
   * an AGE graph database connection before.
   */
  static Map<String, String> expected() {
    Map<String, String> e = new LinkedHashMap<>();
    e.put("AGE|index.create|A", "!ERROR");
    e.put("AGE|index.drop|A", "!ERROR");
    e.put("AGE|index.create|B", "!ERROR");
    e.put("AGE|index.drop|B", "!ERROR");
    e.put("AGE|index.create|C", "!ERROR");
    e.put("AGE|index.drop|C", "!ERROR");
    e.put("AGE|index.create|D", "!ERROR");
    e.put("AGE|index.drop|D", "!ERROR");
    e.put("AGE|vector.create|V1", "!ERROR");
    e.put("AGE|vector.drop|V1", "!ERROR");
    e.put("AGE|vector.create|V2", "!ERROR");
    e.put("AGE|vector.drop|V2", "!ERROR");
    e.put("AGE|vector.create|V3", "!ERROR");
    e.put("AGE|vector.drop|V3", "!ERROR");
    e.put("AGE|vector.create|V4", "!ERROR");
    e.put("AGE|vector.drop|V4", "!ERROR");
    e.put("AGE|vector.create|V5", "!ERROR");
    e.put("AGE|vector.drop|V5", "!ERROR");
    e.put("AGE|vector.create|V6", "!ERROR");
    e.put("AGE|vector.drop|V6", "!ERROR");
    e.put("AGE|vector.create|V7", "!ERROR");
    e.put("AGE|vector.drop|V7", "!ERROR");
    e.put("AGE|constraint.create|K1", "!ERROR");
    e.put("AGE|constraint.drop|K1", "!ERROR");
    e.put("AGE|constraint.create|K2", "!ERROR");
    e.put("AGE|constraint.drop|K2", "!ERROR");
    e.put("AGE|constraint.create|K3", "!ERROR");
    e.put("AGE|constraint.drop|K3", "!ERROR");
    e.put("AGE|constraint.create|K4", "!ERROR");
    e.put("AGE|constraint.drop|K4", "!ERROR");
    e.put("AGE|constraint.create|K5", "!ERROR");
    e.put("AGE|constraint.drop|K5", "!ERROR");
    e.put("AGE|constraint.create|K6", "!ERROR");
    e.put("AGE|constraint.drop|K6", "!ERROR");
    e.put("AGE|constraint.create|K7", "!ERROR");
    e.put("AGE|constraint.drop|K7", "!ERROR");
    e.put("AGE|constraint.create|K8", "!ERROR");
    e.put("AGE|constraint.drop|K8", "!ERROR");
    e.put("AGE|constraint.create|K9", "!ERROR");
    e.put("AGE|constraint.drop|K9", "!ERROR");
    e.put("AGE|nodekey.create|N1", "null");
    e.put("AGE|nodekey.create|N2", "null");
    e.put("AGE|nodekey.create|N3", "null");
    e.put("AGE|nodekey.create|N4", "null");
    e.put("AGE|autocommit|S1", "false");
    e.put("AGE|autocommit|S2", "false");
    e.put("AGE|autocommit|S3", "false");
    e.put("AGE|autocommit|S4", "false");
    e.put("AGE|autocommit|S5", "false");
    e.put("AGE|autocommit|S6", "false");
    e.put("AGE|autocommit|S7", "false");
    e.put("AGE|autocommit|S8", "false");
    e.put("AGE|autocommit|S9", "false");
    e.put("AGE|autocommit|S10", "false");
    e.put("AGE|autocommit|S11", "false");
    e.put("AGE|autocommit|S12", "false");
    e.put("AGE|vectorValue|$p", "$p");
    e.put("AGE|vectorValue|pr.p", "pr.p");
    e.put("AGE|flag|cypher", "true");
    e.put("AGE|flag|nodeIndexes", "false");
    e.put("AGE|flag|relationshipIndexes", "false");
    e.put("AGE|flag|schemaChangesInTransactions", "true");
    e.put("AGE|flag|vectorIndexes", "false");
    e.put("AGE|flag|nodeConstraintTypes", "[]");
    e.put("AGE|flag|relationshipConstraintTypes", "[]");
    e.put("AGE|path|toRoot", "generic");
    e.put("AGE|path|toFailed", "generic");
    e.put(
        "AGE|executionIndex|one",
        "CREATE INDEX IF NOT EXISTS \"idx_execution_id\" ON \"hop_graph\".\"Execution\" (ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, '\"id\"'::ag_catalog.agtype]))");
    e.put(
        "AGE|executionIndex|two",
        "CREATE INDEX IF NOT EXISTS \"idx_execution_id\" ON \"hop_graph\".\"Execution\" (ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::ag_catalog.agtype]), ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, '\"type\"'::ag_catalog.agtype]))");
    return e;
  }
}
