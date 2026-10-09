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
package org.apache.hop.neo4j.actions.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.neo4j.bolt.MemgraphGraphDialect;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.apache.hop.neo4j.bolt.NeptuneGraphDialect;
import org.junit.jupiter.api.Test;

/**
 * The statements were checked against Neo4j 5 and Memgraph 3.6, see the ITs. FalkorDB: see
 * FalkorDbGraphDialectTest.
 */
class Neo4jVectorIndexCypherTest {

  private static IndexUpdate vector(UpdateType type, ObjectType objectType, String dimensions) {
    return IndexUpdate.vector(
        type,
        objectType,
        "doc_vectors",
        "Doc",
        "embedding",
        dimensions,
        GraphVectorSimilarity.COSINE);
  }

  @Test
  void neo4j() throws HopException {
    assertEquals(
        "CREATE VECTOR INDEX `doc_vectors` IF NOT EXISTS FOR (n:`Doc`) ON (n.`embedding`)"
            + " OPTIONS {indexConfig: {`vector.dimensions`: 768,"
            + " `vector.similarity_function`: 'cosine'}}",
        Neo4jIndex.generateCreateIndexCypher(
            vector(UpdateType.CREATE, ObjectType.NODE, "768"), Neo4jGraphDialect.INSTANCE));
    assertEquals(
        "DROP INDEX `doc_vectors` IF EXISTS",
        Neo4jIndex.generateDropIndexCypher(
            vector(UpdateType.DROP, ObjectType.NODE, "768"), Neo4jGraphDialect.INSTANCE));
    assertTrue(
        Neo4jIndex.generateCreateIndexCypher(
                vector(UpdateType.CREATE, ObjectType.RELATIONSHIP, "3"), Neo4jGraphDialect.INSTANCE)
            .contains("FOR ()-[n:`Doc`]-() ON (n.`embedding`)"));
  }

  /** An unnamed Neo4j vector index couldn't be dropped by the action: it needs a name. */
  @Test
  void neo4jVectorIndexNeedsName() {
    IndexUpdate unnamed =
        IndexUpdate.vector(
            UpdateType.CREATE,
            ObjectType.NODE,
            "",
            "Doc",
            "embedding",
            "3",
            GraphVectorSimilarity.COSINE);
    assertThrows(
        HopException.class,
        () -> Neo4jIndex.generateCreateIndexCypher(unnamed, Neo4jGraphDialect.INSTANCE));
    assertThrows(
        HopException.class,
        () -> Neo4jIndex.generateDropIndexCypher(unnamed, Neo4jGraphDialect.INSTANCE));
  }

  @Test
  void memgraph() throws HopException {
    IndexUpdate update = vector(UpdateType.CREATE, ObjectType.NODE, "3");
    update.setVectorSimilarity(GraphVectorSimilarity.EUCLIDEAN);
    assertEquals(
        "CREATE VECTOR INDEX `doc_vectors` ON :`Doc`(`embedding`) WITH CONFIG"
            + " {\"dimension\": 3, \"capacity\": 1000, \"metric\": \"l2sq\"}",
        Neo4jIndex.generateCreateIndexCypher(update, MemgraphGraphDialect.INSTANCE));
    update.setVectorCapacity("50");
    assertTrue(
        Neo4jIndex.generateCreateIndexCypher(update, MemgraphGraphDialect.INSTANCE)
            .contains("\"capacity\": 50"));
    assertEquals(
        "DROP VECTOR INDEX `doc_vectors`",
        Neo4jIndex.generateDropIndexCypher(
            vector(UpdateType.DROP, ObjectType.NODE, "3"), MemgraphGraphDialect.INSTANCE));
    // A vector index on relationships is an edge index, dropped by name like the others
    assertEquals(
        "CREATE VECTOR EDGE INDEX `doc_vectors` ON :`Doc`(`embedding`) WITH CONFIG"
            + " {\"dimension\": 3, \"capacity\": 1000, \"metric\": \"cos\"}",
        Neo4jIndex.generateCreateIndexCypher(
            vector(UpdateType.CREATE, ObjectType.RELATIONSHIP, "3"),
            MemgraphGraphDialect.INSTANCE));
    assertEquals(
        "DROP VECTOR INDEX `doc_vectors`",
        Neo4jIndex.generateDropIndexCypher(
            vector(UpdateType.DROP, ObjectType.RELATIONSHIP, "3"), MemgraphGraphDialect.INSTANCE));
  }

  @Test
  void databasesWithoutVectorIndexesRefuse() {
    IGraphDialect plain = () -> "PLAIN";
    for (IGraphDialect dialect : new IGraphDialect[] {NeptuneGraphDialect.INSTANCE, plain}) {
      HopException e =
          assertThrows(
              HopException.class,
              () ->
                  Neo4jIndex.generateCreateIndexCypher(
                      vector(UpdateType.CREATE, ObjectType.NODE, "3"), dialect));
      assertTrue(e.getMessage().contains("not supported by " + dialect.getId()), e.getMessage());
    }
  }

  @Test
  void invalidSettingsAreReported() {
    assertThrows(
        HopException.class,
        () ->
            Neo4jIndex.generateCreateIndexCypher(
                vector(UpdateType.CREATE, ObjectType.NODE, ""), Neo4jGraphDialect.INSTANCE));
    assertThrows(
        HopException.class,
        () ->
            Neo4jIndex.generateCreateIndexCypher(
                vector(UpdateType.CREATE, ObjectType.NODE, "-1"), Neo4jGraphDialect.INSTANCE));
    IndexUpdate twoProperties =
        IndexUpdate.vector(
            UpdateType.CREATE,
            ObjectType.NODE,
            "i",
            "Doc",
            "a, b",
            "3",
            GraphVectorSimilarity.COSINE);
    assertThrows(
        HopException.class,
        () -> Neo4jIndex.generateCreateIndexCypher(twoProperties, Neo4jGraphDialect.INSTANCE));
  }

  /** Actions saved before vector indexes have no index type: they stay RANGE indexes. */
  @Test
  void vectorSettingsSurviveXml() throws Exception {
    Neo4jIndex action = new Neo4jIndex("index");
    action.getIndexUpdates().add(vector(UpdateType.CREATE, ObjectType.NODE, "${DIMENSIONS}"));
    action.getIndexUpdates().add(new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, "", "A", "b"));
    String xml = "<action>" + XmlMetadataUtil.serializeObjectToXml(action) + "</action>";
    Neo4jIndex copy = new Neo4jIndex();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, "action"), Neo4jIndex.class, copy, null);
    IndexUpdate vector = copy.getIndexUpdates().get(0);
    assertEquals(IndexType.VECTOR, vector.getIndexType());
    assertEquals("${DIMENSIONS}", vector.getVectorDimensions());
    assertEquals(GraphVectorSimilarity.COSINE, vector.getVectorSimilarity());
    assertTrue(!copy.getIndexUpdates().get(1).isVector());
  }
}
