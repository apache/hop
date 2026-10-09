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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.graph.GraphDatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BoltGraphDatabaseTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
    for (Class<?> type :
        List.of(
            Neo4jGraphDatabase.class, MemgraphGraphDatabase.class, NeptuneGraphDatabase.class)) {
      PluginRegistry.getInstance()
          .registerPluginClass(
              type.getName(), GraphDatabasePluginType.class, GraphDatabasePlugin.class);
    }
  }

  /** Every setting of a Neo4j connection must survive the trip through a Bolt connection. */
  @Test
  void testNeoConnectionRoundTrip() throws Exception {
    NeoConnection source = new NeoConnection();
    source.setName("source");
    List<Field> fields = getNeoConnectionFields();
    for (Field field : fields) {
      if (field.getType() == String.class) {
        field.set(source, "value-of-" + field.getName());
      } else if (field.getType() == boolean.class) {
        field.set(source, !field.getBoolean(source));
      } else if (field.getType() == List.class) {
        field.set(source, new ArrayList<>(List.of("bolt://one:7687", "bolt://two:7687")));
      }
    }

    Neo4jGraphDatabase bolt = new Neo4jGraphDatabase();
    bolt.copyFrom(source);
    NeoConnection target = bolt.toNeoConnection("target");

    assertEquals("target", target.getName());
    for (Field field : fields) {
      assertEquals(field.get(source), field.get(target), "Field " + field.getName());
    }
  }

  /** A setting added to NeoConnection must be added to BoltGraphDatabase too. */
  @Test
  void testAllNeoConnectionFieldsAreBoltFields() throws Exception {
    for (Field field : getNeoConnectionFields()) {
      Field boltField = BoltGraphDatabase.class.getDeclaredField(field.getName());
      assertNotNull(boltField.getAnnotation(HopMetadataProperty.class), field.getName());
    }
  }

  @Test
  void testDefaults() {
    NeoConnection neoDefaults = new NeoConnection();
    NeoConnection neo4j = new Neo4jGraphDatabase().toNeoConnection("neo4j");
    assertEquals(neoDefaults.getBoltPort(), neo4j.getBoltPort());
    assertEquals(neoDefaults.getProtocol(), neo4j.getProtocol());
    assertEquals(neoDefaults.isAutomatic(), neo4j.isAutomatic());

    NeoConnection memgraph = new MemgraphGraphDatabase().toNeoConnection("memgraph");
    assertFalse(memgraph.isAutomatic());
    assertEquals("bolt", memgraph.getProtocol());
    assertEquals("7687", memgraph.getBoltPort());

    NeoConnection neptune = new NeptuneGraphDatabase().toNeoConnection("neptune");
    assertFalse(neptune.isAutomatic());
    assertTrue(neptune.isUsingEncryption());
    assertEquals("8182", neptune.getBoltPort());
  }

  @Test
  void testCloneIsDeep() {
    MemgraphGraphDatabase memgraph = new MemgraphGraphDatabase();
    memgraph.getManualUrls().add(new BoltManualUrl("bolt://a:7687"));
    BoltGraphDatabase clone = memgraph.clone();
    clone.getManualUrls().get(0).setUrl("bolt://b:7687");
    clone.setServer("other");
    assertEquals("bolt://a:7687", memgraph.getManualUrls().get(0).getUrl());
    assertNull(memgraph.getServer());
  }

  @Test
  void testJsonRoundTrip(@TempDir Path folder) throws Exception {
    IHopMetadataProvider provider = createProvider(folder);

    MemgraphGraphDatabase memgraph =
        (MemgraphGraphDatabase) GraphDatabaseMeta.createGraphDatabase("MEMGRAPH");
    memgraph.setServer("memgraph-host");
    memgraph.setPassword("secret");
    memgraph.getManualUrls().add(new BoltManualUrl("bolt://memgraph-host:7688"));
    provider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("memgraph", memgraph));

    Path file = folder.resolve("graph-database-connection").resolve("memgraph.json");
    assertTrue(Files.exists(file));
    String json = Files.readString(file);
    assertTrue(json.contains("MEMGRAPH"), json);
    assertFalse(json.contains("secret"), "The password is stored encrypted: " + json);

    GraphDatabaseMeta loaded = GraphDatabaseMeta.load(provider, "memgraph");
    assertNotNull(loaded);
    assertEquals("MEMGRAPH", loaded.getPluginId());
    MemgraphGraphDatabase loadedMemgraph = (MemgraphGraphDatabase) loaded.getGraphDatabase();
    assertEquals("memgraph-host", loadedMemgraph.getServer());
    assertEquals("secret", loadedMemgraph.getPassword());
    assertEquals("bolt", loadedMemgraph.getProtocol());
    assertFalse(loadedMemgraph.isAutomatic());
    assertEquals("bolt://memgraph-host:7688", loadedMemgraph.getManualUrls().get(0).getUrl());
  }

  /** A Neo4j connection wins over a graph database connection with the same name. */
  @Test
  void testLoadConnectionPrecedence(@TempDir Path folder) throws Exception {
    IHopMetadataProvider provider = createProvider(folder);

    NeoConnection neo = new NeoConnection();
    neo.setName("shared");
    neo.setServer("neo4j-connection-host");
    provider.getSerializer(NeoConnection.class).save(neo);

    for (String name : List.of("shared", "bolt-only")) {
      Neo4jGraphDatabase bolt = new Neo4jGraphDatabase();
      bolt.setPluginId("NEO4J");
      bolt.setServer("graph-connection-host");
      provider.getSerializer(GraphDatabaseMeta.class).save(new GraphDatabaseMeta(name, bolt));
    }

    NeoConnection shared = NeoConnectionUtils.loadConnection(provider, "shared");
    assertEquals("neo4j-connection-host", shared.getServer());

    NeoConnection boltOnly = NeoConnectionUtils.loadConnection(provider, "bolt-only");
    assertEquals("bolt-only", boltOnly.getName());
    assertEquals("graph-connection-host", boltOnly.getServer());

    assertNull(NeoConnectionUtils.loadConnection(provider, "missing"));
    assertNull(NeoConnectionUtils.loadConnection(provider, ""));

    assertEquals(List.of("bolt-only", "shared"), NeoConnectionUtils.getConnectionNames(provider));
  }

  @Test
  void testConvertToGraphConnection(@TempDir Path folder) throws Exception {
    IHopMetadataProvider provider = createProvider(folder);
    NeoConnection neo = new NeoConnection();
    neo.setName("legacy");
    neo.setServer("legacy-host");
    neo.setRouting(true);
    neo.setVirtualPath("/graphs");
    provider.getSerializer(NeoConnection.class).save(neo);

    GraphDatabaseMeta converted = NeoConnectionUtils.convertToGraphConnection(provider, "legacy");

    assertEquals("NEO4J", converted.getPluginId());
    assertFalse(provider.getSerializer(NeoConnection.class).exists("legacy"));
    GraphDatabaseMeta loaded = GraphDatabaseMeta.load(provider, "legacy");
    assertNotNull(loaded);
    Neo4jGraphDatabase neo4j = (Neo4jGraphDatabase) loaded.getGraphDatabase();
    assertEquals("legacy-host", neo4j.getServer());
    assertTrue(neo4j.isRouting());

    // Found by name as before, now through the graph database connection
    //
    assertEquals("legacy-host", NeoConnectionUtils.loadConnection(provider, "legacy").getServer());

    // Converting again fails: there is no Neo4j connection left
    //
    assertThrows(
        HopException.class, () -> NeoConnectionUtils.convertToGraphConnection(provider, "legacy"));
  }

  @Test
  void testConvertRefusesToOverwrite(@TempDir Path folder) throws Exception {
    IHopMetadataProvider provider = createProvider(folder);
    NeoConnection neo = new NeoConnection();
    neo.setName("both");
    provider.getSerializer(NeoConnection.class).save(neo);
    provider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("both", GraphDatabaseMeta.createGraphDatabase("NEO4J")));

    assertThrows(
        HopException.class, () -> NeoConnectionUtils.convertToGraphConnection(provider, "both"));
    assertTrue(provider.getSerializer(NeoConnection.class).exists("both"));
  }

  @Test
  void testConvertAll(@TempDir Path folder) throws Exception {
    IHopMetadataProvider provider = createProvider(folder);
    for (String name : List.of("one", "two", "taken")) {
      NeoConnection neo = new NeoConnection();
      neo.setName(name);
      neo.setServer(name + "-host");
      provider.getSerializer(NeoConnection.class).save(neo);
    }
    provider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("taken", GraphDatabaseMeta.createGraphDatabase("MEMGRAPH")));

    java.util.Map<String, String> skipped =
        NeoConnectionUtils.convertAllToGraphConnections(provider);

    assertEquals(List.of("taken"), new ArrayList<>(skipped.keySet()));
    assertEquals(List.of("taken"), provider.getSerializer(NeoConnection.class).listObjectNames());
    assertEquals(
        "two-host",
        ((Neo4jGraphDatabase) GraphDatabaseMeta.load(provider, "two").getGraphDatabase())
            .getServer());
    // The existing graph database connection is untouched
    assertEquals("MEMGRAPH", GraphDatabaseMeta.load(provider, "taken").getPluginId());
  }

  /**
   * A Neo4j connection in a parent project is converted in that project, not in the last provider
   * where new objects go.
   */
  @Test
  void testConvertKeepsTheMetadataProvider(@TempDir Path folder) throws Exception {
    JsonMetadataProvider parent = createProvider(folder.resolve("parent"), "parent");
    JsonMetadataProvider child = createProvider(folder.resolve("child"), "child");
    MultiMetadataProvider multi =
        new MultiMetadataProvider(Variables.getADefaultVariableSpace(), parent, child);

    NeoConnection neo = new NeoConnection();
    neo.setName("inherited");
    neo.setServer("parent-host");
    parent.getSerializer(NeoConnection.class).save(neo);

    NeoConnectionUtils.convertToGraphConnection(multi, "inherited");

    assertTrue(parent.getSerializer(GraphDatabaseMeta.class).exists("inherited"));
    assertFalse(parent.getSerializer(NeoConnection.class).exists("inherited"));
    assertTrue(child.getSerializer(GraphDatabaseMeta.class).listObjectNames().isEmpty());
    assertTrue(child.getSerializer(NeoConnection.class).listObjectNames().isEmpty());
  }

  /**
   * A child project's Neo4j connection overriding one of its parent project is not converted: after
   * deleting the child's, the parent's Neo4j connection would be found first, pointing pipelines
   * and workflows at the parent's server. Nothing is changed.
   */
  @Test
  void testConvertRefusesDuplicateInParentProject(@TempDir Path folder) throws Exception {
    JsonMetadataProvider parent = createProvider(folder.resolve("parent"), "parent");
    JsonMetadataProvider child = createProvider(folder.resolve("child"), "child");
    MultiMetadataProvider multi =
        new MultiMetadataProvider(Variables.getADefaultVariableSpace(), parent, child);
    saveNeoConnection(parent, "shared", "parent-host");
    saveNeoConnection(child, "shared", "child-host");
    assertEquals("child-host", NeoConnectionUtils.loadConnection(multi, "shared").getServer());

    HopException e =
        assertThrows(
            HopException.class, () -> NeoConnectionUtils.convertToGraphConnection(multi, "shared"));
    // Both locations, the parent project first
    assertTrue(e.getMessage().matches("(?s).*parent, .*child.*"), e.getMessage());

    assertTrue(parent.getSerializer(NeoConnection.class).exists("shared"));
    assertTrue(child.getSerializer(NeoConnection.class).exists("shared"));
    assertFalse(multi.getSerializer(GraphDatabaseMeta.class).exists("shared"));
    assertEquals("child-host", NeoConnectionUtils.loadConnection(multi, "shared").getServer());
  }

  /**
   * Convert all only converts the Neo4j connections of the active project, the last provider. Those
   * of a parent project and duplicates are reported as skipped and left alone.
   */
  @Test
  void testConvertAllOnlyConvertsActiveProject(@TempDir Path folder) throws Exception {
    JsonMetadataProvider parent = createProvider(folder.resolve("parent"), "parent");
    JsonMetadataProvider child = createProvider(folder.resolve("child"), "child");
    MultiMetadataProvider multi =
        new MultiMetadataProvider(Variables.getADefaultVariableSpace(), parent, child);
    saveNeoConnection(parent, "inherited", "parent-host");
    saveNeoConnection(parent, "shared", "parent-host");
    saveNeoConnection(child, "shared", "child-host");
    saveNeoConnection(child, "local", "child-host");

    assertEquals(
        List.of("local", "shared"), NeoConnectionUtils.getConvertibleConnectionNames(multi));

    java.util.Map<String, String> skipped = NeoConnectionUtils.convertAllToGraphConnections(multi);

    assertEquals(List.of("inherited", "shared"), new ArrayList<>(skipped.keySet()));
    assertTrue(skipped.get("inherited").contains("parent"), skipped.get("inherited"));
    assertTrue(skipped.get("shared").matches("(?s).*parent, .*child.*"), skipped.get("shared"));

    assertTrue(child.getSerializer(GraphDatabaseMeta.class).exists("local"));
    assertFalse(child.getSerializer(NeoConnection.class).exists("local"));
    assertTrue(parent.getSerializer(NeoConnection.class).exists("inherited"));
    assertTrue(parent.getSerializer(NeoConnection.class).exists("shared"));
    assertTrue(child.getSerializer(NeoConnection.class).exists("shared"));
    assertTrue(parent.getSerializer(GraphDatabaseMeta.class).listObjectNames().isEmpty());
    assertEquals(List.of("local"), child.getSerializer(GraphDatabaseMeta.class).listObjectNames());
  }

  private static void saveNeoConnection(IHopMetadataProvider provider, String name, String server)
      throws Exception {
    NeoConnection neo = new NeoConnection();
    neo.setName(name);
    neo.setServer(server);
    provider.getSerializer(NeoConnection.class).save(neo);
  }

  /**
   * Every setting of a Neo4j connection, the password included, survives a conversion saved to and
   * loaded from JSON. A field added to NeoConnection without being converted fails this test.
   */
  @Test
  void testConvertKeepsEverySetting(@TempDir Path folder) throws Exception {
    IHopMetadataProvider provider = createProvider(folder);
    NeoConnection source = new NeoConnection();
    source.setName("all-fields");
    List<Field> fields = getNeoConnectionFields();
    for (Field field : fields) {
      if (field.getType() == String.class) {
        field.set(source, "value-of-" + field.getName());
      } else if (field.getType() == boolean.class) {
        field.set(source, !field.getBoolean(source));
      } else if (field.getType() == List.class) {
        field.set(source, new ArrayList<>(List.of("bolt://one:7687", "bolt://two:7687")));
      } else {
        throw new AssertionError(
            "Unsupported type of field " + field.getName() + ": extend this test");
      }
    }
    provider.getSerializer(NeoConnection.class).save(source);

    NeoConnectionUtils.convertToGraphConnection(provider, "all-fields");
    NeoConnection target = NeoConnectionUtils.loadConnection(provider, "all-fields");

    assertNotNull(target);
    assertEquals("all-fields", target.getName());
    assertEquals("value-of-password", target.getPassword());
    for (Field field : fields) {
      assertEquals(field.get(source), field.get(target), "Field " + field.getName());
    }
  }

  private static JsonMetadataProvider createProvider(Path folder, String description)
      throws Exception {
    Files.createDirectories(folder);
    JsonMetadataProvider provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            folder.toString(),
            Variables.getADefaultVariableSpace());
    provider.setDescription(description);
    return provider;
  }

  private static IHopMetadataProvider createProvider(Path folder) {
    return new JsonMetadataProvider(
        new HopTwoWayPasswordEncoder(), folder.toString(), Variables.getADefaultVariableSpace());
  }

  private static List<Field> getNeoConnectionFields() {
    List<Field> fields = new ArrayList<>();
    for (Field field : NeoConnection.class.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) != null) {
        field.setAccessible(true);
        fields.add(field);
      }
    }
    return fields;
  }
}
