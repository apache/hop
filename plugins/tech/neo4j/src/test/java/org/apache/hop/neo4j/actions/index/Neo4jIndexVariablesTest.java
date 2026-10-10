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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.neo4j.bolt.MemgraphGraphDialect;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.junit.jupiter.api.Test;

/** The variables in the index name, object name and properties are resolved, for every dialect. */
class Neo4jIndexVariablesTest {

  private static Neo4jIndex action() {
    Neo4jIndex action = new Neo4jIndex("index");
    action.setVariable("INDEX", "chunk_index");
    action.setVariable("LABEL", "Chunk");
    action.setVariable("PROPERTY", "embedding");
    action.setVariable("DIMENSIONS", "768");
    return action;
  }

  @Test
  void createRangeIndex() throws HopException {
    Neo4jIndex action = action();
    IndexUpdate update =
        action.resolved(
            new IndexUpdate(
                UpdateType.CREATE, ObjectType.NODE, "${INDEX}", "${LABEL}", "id, ${PROPERTY}"));
    assertEquals(
        "CREATE INDEX `chunk_index` IF NOT EXISTS FOR (n:`Chunk`) ON (n.`id`, n.`embedding`)",
        Neo4jIndex.generateCreateIndexCypher(update, Neo4jGraphDialect.INSTANCE));
  }

  @Test
  void dropRangeIndex() throws HopException {
    IndexUpdate update =
        action()
            .resolved(
                new IndexUpdate(UpdateType.DROP, ObjectType.NODE, "${INDEX}", "${LABEL}", "id"));
    assertEquals(
        "DROP INDEX `chunk_index` IF EXISTS",
        Neo4jIndex.generateDropIndexCypher(update, Neo4jGraphDialect.INSTANCE));
  }

  @Test
  void createAndDropVectorIndex() throws HopException {
    Neo4jIndex action = action();
    IndexUpdate create = vector(UpdateType.CREATE);
    IndexUpdate resolved = action.resolved(create);

    assertEquals("chunk_index", resolved.getIndexName());
    assertEquals("Chunk", resolved.getObjectName());
    assertEquals("embedding", resolved.getObjectProperties());
    assertEquals("768", resolved.getVectorDimensions());
    // The action itself keeps the variables
    assertEquals("${INDEX}", create.getIndexName());

    String memgraph = Neo4jIndex.generateCreateIndexCypher(resolved, MemgraphGraphDialect.INSTANCE);
    assertEquals(
        "CREATE VECTOR INDEX `chunk_index` ON :`Chunk`(`embedding`)",
        memgraph.substring(0, memgraph.indexOf(" WITH CONFIG")));
    assertEquals(
        "DROP VECTOR INDEX `chunk_index`",
        Neo4jIndex.generateDropIndexCypher(
            action.resolved(vector(UpdateType.DROP)), MemgraphGraphDialect.INSTANCE));
  }

  private static IndexUpdate vector(UpdateType type) {
    IndexUpdate update =
        new IndexUpdate(type, ObjectType.NODE, "${INDEX}", "${LABEL}", "${PROPERTY}");
    update.setIndexType(IndexType.VECTOR);
    update.setVectorDimensions("${DIMENSIONS}");
    update.setVectorSimilarity(GraphVectorSimilarity.COSINE);
    return update;
  }
}
