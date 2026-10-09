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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.graph.GraphConnectionLookup;
import org.apache.hop.core.graph.GraphDatabaseCapabilities;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.graph.GraphDatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.neo4j.bolt.MemgraphGraphDatabase;
import org.apache.hop.neo4j.bolt.Neo4jGraphDatabase;
import org.apache.hop.neo4j.bolt.NeptuneGraphDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A Neo4j connection found by name through the graph SPI of Hop core, as hop-conf does. */
class NeoConnectionGraphLookupTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (Class<?> type :
        List.of(
            Neo4jGraphDatabase.class, MemgraphGraphDatabase.class, NeptuneGraphDatabase.class)) {
      registry.registerPluginClass(
          type.getName(), GraphDatabasePluginType.class, GraphDatabasePlugin.class);
    }
    registry.registerPluginClass(
        NeoConnection.class.getName(), MetadataPluginType.class, HopMetadata.class);
  }

  @Test
  void neo4jConnectionIsFoundAsGraphConnection() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    NeoConnection neo = new NeoConnection();
    neo.setName("legacy");
    neo.setServer("neo4j.example.com");
    neo.setPassword("secret");
    provider.getSerializer(NeoConnection.class).save(neo);

    GraphConnectionLookup.Found found = GraphConnectionLookup.find(provider, "legacy");

    assertNotNull(found);
    assertEquals("neo4j-connection", found.metadataKey());
    GraphDatabaseMeta meta = found.graphDatabaseMeta();
    assertEquals("legacy", meta.getName());
    assertEquals("NEO4J", meta.getPluginId());
    Neo4jGraphDatabase neo4j = (Neo4jGraphDatabase) meta.getGraphDatabase();
    assertEquals("neo4j.example.com", neo4j.getServer());
    // Nothing was saved
    assertFalse(provider.getSerializer(GraphDatabaseMeta.class).exists("legacy"));
  }

  @Test
  void capabilitiesOfTheBoltTypes() throws Exception {
    for (String id : List.of("NEO4J", "MEMGRAPH", "NEPTUNE")) {
      GraphDatabaseCapabilities capabilities = GraphDatabaseCapabilities.of(id);
      Map<String, Boolean> map = capabilities.getCapabilities();
      assertEquals(
          GraphDatabaseCapabilities.QUERY_LANGUAGE_CYPHER, capabilities.getQueryLanguage());
      assertTrue(map.get("cypher"));
      assertTrue(map.containsKey("supportingVectorIndexes"), map.toString());
    }
    assertTrue(
        GraphDatabaseCapabilities.of("NEO4J").getCapabilities().get("supportingVectorIndexes"));
    assertFalse(
        GraphDatabaseCapabilities.of("NEPTUNE").getCapabilities().get("supportingVectorIndexes"));
  }
}
