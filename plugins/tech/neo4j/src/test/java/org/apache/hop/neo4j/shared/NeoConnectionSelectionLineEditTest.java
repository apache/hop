/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.shared;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;

/** Which editor the connection line opens for a connection name. */
class NeoConnectionSelectionLineEditTest {

  @Test
  void testNeo4jConnectionWinsWhenNamesClash() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    NeoConnection neoConnection = new NeoConnection();
    neoConnection.setName("graph");
    metadataProvider.getSerializer(NeoConnection.class).save(neoConnection);
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("graph", null));

    // The runtime uses the Neo4j connection when both exist, so the line edits that one.
    assertTrue(NeoConnectionSelectionLine.isNeo4jConnection(metadataProvider, "graph"));
  }

  @Test
  void testGraphDatabaseConnection() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("graph", null));

    assertFalse(NeoConnectionSelectionLine.isNeo4jConnection(metadataProvider, "graph"));
  }
}
