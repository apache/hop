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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphStatement;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.junit.jupiter.api.Test;

/** The statements were checked against FalkorDB 6, see FalkorDbIT. */
class FalkorDbSchemaAndVectorSearchTest {

  private final FalkorDbGraphDialect falkordb = FalkorDbGraphDialect.INSTANCE;

  @Test
  void testVectorSearchStatement() throws HopException {
    assertTrue(falkordb.isSupportingVectorSearch());
    GraphStatement cosine =
        falkordb.getVectorSearchStatement(
            new GraphVectorSearchDefinition(
                null, "Doc", "embedding", 4, List.of("id", "my text"), null));
    assertEquals(
        "CALL db.idx.vector.queryNodes($label, $attribute, $k, vecf32($vector)) YIELD node, score"
            + " WITH node, 1.0 - score AS similarity RETURN similarity AS score,"
            + " node.`id` AS p0, node.`my text` AS p1 ORDER BY score DESC",
        cosine.statement());
    assertEquals(Map.of("label", "Doc", "attribute", "embedding", "k", 4L), cosine.parameters());

    GraphStatement euclidean =
        falkordb.getVectorSearchStatement(
            new GraphVectorSearchDefinition(
                null, "Doc", "embedding", 4, null, GraphVectorSimilarity.EUCLIDEAN));
    assertTrue(
        euclidean.statement().contains("WITH node, 1.0 / (1.0 + score * score) AS similarity"),
        euclidean.statement());

    // FalkorDB has no index names: the label and property are needed
    assertThrows(
        HopException.class,
        () ->
            falkordb.getVectorSearchStatement(
                new GraphVectorSearchDefinition("index", null, null, 4, null, null)));
  }

  /** Relationships: by relationship type and property, the score converted the same way. */
  @Test
  void testRelationshipVectorSearchStatement() throws HopException {
    assertTrue(falkordb.isSupportingRelationshipVectorSearch());
    GraphStatement cosine =
        falkordb.getVectorSearchStatement(
            new GraphVectorSearchDefinition(
                null,
                "SIMILAR TO",
                "embedding",
                4,
                List.of("id"),
                null,
                GraphObjectType.RELATIONSHIP));
    assertEquals(
        "CALL db.idx.vector.queryRelationships($type, $attribute, $k, vecf32($vector))"
            + " YIELD relationship, score WITH relationship, 1.0 - score AS similarity"
            + " RETURN similarity AS score, relationship.`id` AS p0 ORDER BY score DESC",
        cosine.statement());
    assertEquals(
        Map.of("type", "SIMILAR TO", "attribute", "embedding", "k", 4L), cosine.parameters());

    GraphStatement euclidean =
        falkordb.getVectorSearchStatement(
            new GraphVectorSearchDefinition(
                null,
                "R",
                "embedding",
                4,
                null,
                GraphVectorSimilarity.EUCLIDEAN,
                GraphObjectType.RELATIONSHIP));
    assertTrue(
        euclidean
            .statement()
            .contains("WITH relationship, 1.0 / (1.0 + score * score) AS similarity"),
        euclidean.statement());

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                falkordb.getVectorSearchStatement(
                    new GraphVectorSearchDefinition(
                        "index", null, null, 4, null, null, GraphObjectType.RELATIONSHIP)));
    assertTrue(e.getMessage().contains("relationship type"), e.getMessage());
  }

  /** The labels and types come from the procedures, then every one is sampled. */
  @Test
  void testSchemaStatements() throws HopException {
    List<String> executed = new ArrayList<>();
    Map<String, List<Map<String, Object>>> answers = new LinkedHashMap<>();
    answers.put("CALL db.labels()", List.of(Map.of("label", "Person")));
    answers.put("CALL db.relationshipTypes()", List.of(Map.of("relationshipType", "KNOWS")));
    answers.put(
        "MATCH (n:`Person`) WITH n LIMIT 50 RETURN properties(n) AS properties",
        List.of(Map.of("properties", Map.of("name", "a"))));
    answers.put(
        "MATCH (a)-[r:`KNOWS`]->(b) WITH a, r, b LIMIT 50 RETURN labels(a) AS startLabels,"
            + " labels(b) AS endLabels, properties(r) AS properties",
        List.of(
            Map.of(
                "startLabels",
                List.of("Person"),
                "endLabels",
                List.of("Person"),
                "properties",
                Map.of())));
    IGraphConnection connection =
        new IGraphConnection() {
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
          public void close() {}
        };

    GraphSchema schema = falkordb.getSchema(connection, 50);

    assertEquals(4, executed.size(), executed.toString());
    assertTrue(schema.sampled());
    assertEquals("Person", schema.entries().get(0).name());
    assertEquals("name", schema.entries().get(0).property());
    assertEquals("KNOWS", schema.entries().get(1).name());
    assertEquals(List.of("Person"), schema.entries().get(1).startLabels());
  }
}
