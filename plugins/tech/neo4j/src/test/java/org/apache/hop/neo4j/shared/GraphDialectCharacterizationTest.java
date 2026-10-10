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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hop.core.graph.GraphConstraintType;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.neo4j.actions.constraint.ConstraintUpdate;
import org.apache.hop.neo4j.actions.constraint.Neo4jConstraint;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.apache.hop.neo4j.bolt.MemgraphGraphDialect;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.apache.hop.neo4j.bolt.NeptuneGraphDialect;
import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerTabBase;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Characterization of the statements generated for the dialects of the Bolt databases, recorded
 * before the dialect SPI. The FalkorDB, AGE and Gremlin dialects have theirs in their own plugins.
 */
class GraphDialectCharacterizationTest {

  static final String ERROR = "!ERROR";

  static final List<IGraphDialect> DIALECTS =
      List.of(
          Neo4jGraphDialect.INSTANCE, MemgraphGraphDialect.INSTANCE, NeptuneGraphDialect.INSTANCE);

  static Map<String, Callable<Object>> cases() {
    Map<String, Callable<Object>> cases = new LinkedHashMap<>();
    Map<String, IndexUpdate> indexes = new LinkedHashMap<>();
    indexes.put(
        "A", new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, "idx", "Person", "name, age"));
    indexes.put(
        "B", new IndexUpdate(UpdateType.CREATE, ObjectType.RELATIONSHIP, "idx", "KNOWS", "since"));
    indexes.put("C", new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, "", "Person", "name"));
    indexes.put(
        "D", new IndexUpdate(UpdateType.CREATE, ObjectType.RELATIONSHIP, "idx", "KNOWS", "a, b"));

    Map<String, IndexUpdate> vectors = new LinkedHashMap<>();
    vectors.put(
        "V1",
        vector(
            ObjectType.NODE,
            "doc_vectors",
            "embedding",
            "768",
            GraphVectorSimilarity.COSINE,
            null));
    vectors.put(
        "V2",
        vector(
            ObjectType.RELATIONSHIP,
            "doc_vectors",
            "embedding",
            "3",
            GraphVectorSimilarity.EUCLIDEAN,
            null));
    vectors.put(
        "V3", vector(ObjectType.NODE, "", "embedding", "3", GraphVectorSimilarity.COSINE, null));
    vectors.put(
        "V4",
        vector(
            ObjectType.NODE,
            "doc_vectors",
            "embedding",
            "3",
            GraphVectorSimilarity.EUCLIDEAN,
            "50"));
    vectors.put(
        "V5",
        vector(
            ObjectType.NODE, "doc_vectors", "embedding", "", GraphVectorSimilarity.COSINE, null));
    vectors.put(
        "V6",
        vector(
            ObjectType.NODE, "doc_vectors", "embedding", "-1", GraphVectorSimilarity.COSINE, null));
    vectors.put(
        "V7",
        vector(ObjectType.NODE, "doc_vectors", "a, b", "3", GraphVectorSimilarity.COSINE, null));

    org.apache.hop.neo4j.actions.constraint.ObjectType node =
        org.apache.hop.neo4j.actions.constraint.ObjectType.NODE;
    org.apache.hop.neo4j.actions.constraint.ObjectType rel =
        org.apache.hop.neo4j.actions.constraint.ObjectType.RELATIONSHIP;
    Map<String, ConstraintUpdate> constraints = new LinkedHashMap<>();
    constraints.put("K1", constraint(node, GraphConstraintType.UNIQUE, "c", "Person", "id"));
    constraints.put("K2", constraint(node, GraphConstraintType.NOT_NULL, "c", "Person", "id"));
    constraints.put("K3", constraint(node, GraphConstraintType.NODE_KEY, "c", "Person", "a, b"));
    constraints.put("K4", constraint(rel, GraphConstraintType.UNIQUE, "c", "KNOWS", "id"));
    constraints.put("K5", constraint(rel, GraphConstraintType.NOT_NULL, "c", "KNOWS", "id"));
    constraints.put("K6", constraint(rel, GraphConstraintType.NODE_KEY, "c", "KNOWS", "id"));
    constraints.put("K7", constraint(node, GraphConstraintType.UNIQUE, "c", "Person", "a, b"));
    constraints.put("K8", constraint(node, GraphConstraintType.NOT_NULL, "c", "Person", "a, b"));
    constraints.put("K9", constraint(node, GraphConstraintType.UNIQUE, "", "Person", "id"));

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

    for (IGraphDialect d : DIALECTS) {
      String p = d.getId() + "|";
      for (var e : indexes.entrySet()) {
        cases.put(
            p + "index.create|" + e.getKey(),
            () -> Neo4jIndex.generateCreateIndexCypher(e.getValue(), d));
        cases.put(
            p + "index.drop|" + e.getKey(),
            () -> Neo4jIndex.generateDropIndexCypher(e.getValue(), d));
      }
      for (var e : vectors.entrySet()) {
        cases.put(
            p + "vector.create|" + e.getKey(),
            () -> Neo4jIndex.generateCreateIndexCypher(e.getValue(), d));
        cases.put(
            p + "vector.drop|" + e.getKey(),
            () -> Neo4jIndex.generateDropIndexCypher(e.getValue(), d));
      }
      for (var e : constraints.entrySet()) {
        cases.put(
            p + "constraint.create|" + e.getKey(),
            () -> Neo4jConstraint.generateCreateConstraintCypher(e.getValue(), d));
        cases.put(
            p + "constraint.drop|" + e.getKey(),
            () -> Neo4jConstraint.generateDropConstraintCypher(e.getValue(), d));
      }
      for (var e : nodeKeys.entrySet()) {
        cases.put(
            p + "nodekey.create|" + e.getKey(),
            () ->
                NeoConnectionUtils.getCreateNodeIndexCypher(
                    e.getValue().get(0), e.getValue().get(1), d));
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
      cases.put(p + "path|toRoot", () -> NeoExecutionViewerTabBase.buildPathToRootCypher(true, d));
      cases.put(p + "path|toFailed", () -> NeoExecutionViewerTabBase.buildPathToFailedCypher(d));
    }
    return cases;
  }

  static IndexUpdate vector(
      ObjectType objectType,
      String name,
      String property,
      String dimensions,
      GraphVectorSimilarity similarity,
      String capacity) {
    IndexUpdate update =
        IndexUpdate.vector(
            UpdateType.CREATE, objectType, name, "Doc", property, dimensions, similarity);
    update.setVectorCapacity(capacity);
    return update;
  }

  static ConstraintUpdate constraint(
      org.apache.hop.neo4j.actions.constraint.ObjectType objectType,
      GraphConstraintType type,
      String name,
      String label,
      String properties) {
    return new ConstraintUpdate(
        org.apache.hop.neo4j.actions.constraint.UpdateType.CREATE,
        objectType,
        type,
        name,
        label,
        properties);
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
    Map<String, String> expected = GraphDialectCharacterizationExpected.expected();
    List<DynamicTest> tests = new ArrayList<>();
    for (var e : cases.entrySet()) {
      tests.add(
          DynamicTest.dynamicTest(
              e.getKey(), () -> assertEquals(expected.get(e.getKey()), run(e.getValue()))));
    }
    return tests;
  }
}
