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

package org.apache.hop.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.BaseGraphDatabase;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.graph.GraphDatabasePluginType;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDatabaseMetaConvertible;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class GraphConnectionConfigOptionPluginTest {

  private static final String SECRET = "very-secret-password";

  /** A graph database which can be reached when its host is not "unreachable". */
  @GraphDatabasePlugin(id = "FAKE_CONF", name = "Fake conf", description = "Fake for hop-conf")
  @Getter
  @Setter
  public static class FakeGraphDatabase extends BaseGraphDatabase {
    @HopMetadataProperty private String host;

    @HopMetadataProperty(password = true)
    private String password;

    @Override
    public IGraphDialect getGraphDialect() {
      return new CypherGraphDialect("FAKE_CONF_DIALECT") {
        @Override
        public boolean isSupportingVectorIndexes() {
          return true;
        }
      };
    }

    @Override
    public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
        throws HopException {
      throw new HopException("Not connecting");
    }

    @Override
    public String test(IVariables variables, String connectionName) throws HopException {
      String realHost = variables.resolve(host);
      if ("unreachable".equals(realHost)) {
        throw new HopException("Unable to reach " + realHost);
      }
      return "fake://" + realHost;
    }
  }

  /** A connection type from before the graph database connection, like the Neo4j connection. */
  @HopMetadata(key = "fake-legacy-graph-connection", name = "Fake legacy graph connection")
  @Getter
  @Setter
  public static class FakeLegacyConnection extends HopMetadataBase
      implements IGraphDatabaseMetaConvertible {
    @HopMetadataProperty private String host;

    public FakeLegacyConnection() {}

    public FakeLegacyConnection(String name, String host) {
      this.name = name;
      this.host = host;
    }

    @Override
    public GraphDatabaseMeta toGraphDatabaseMeta() throws HopException {
      FakeGraphDatabase database =
          (FakeGraphDatabase) GraphDatabaseMeta.createGraphDatabase("FAKE_CONF");
      database.setHost(host);
      return new GraphDatabaseMeta(name, database);
    }
  }

  private MemoryMetadataProvider metadataProvider;
  private IVariables variables;
  private ByteArrayOutputStream output;

  @BeforeAll
  static void registerPlugins() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        FakeGraphDatabase.class.getName(),
        GraphDatabasePluginType.class,
        GraphDatabasePlugin.class);
    registry.registerPluginClass(
        FakeLegacyConnection.class.getName(), MetadataPluginType.class, HopMetadata.class);
  }

  @BeforeEach
  void createMetadata() throws Exception {
    metadataProvider = new MemoryMetadataProvider();
    variables = new Variables();
    variables.setVariable("FAKE_HOST", "graph.example.com");
    output = new ByteArrayOutputStream();

    saveGraphConnection("good", "${FAKE_HOST}");
    saveGraphConnection("bad", "unreachable");
    metadataProvider
        .getSerializer(FakeLegacyConnection.class)
        .save(new FakeLegacyConnection("legacy", "legacy.example.com"));
  }

  private void saveGraphConnection(String name, String host) throws HopException {
    FakeGraphDatabase database =
        (FakeGraphDatabase) GraphDatabaseMeta.createGraphDatabase("FAKE_CONF");
    database.setHost(host);
    database.setPassword(SECRET);
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta(name, database));
  }

  private boolean handle(String... args) throws HopException {
    GraphConnectionConfigOptionPlugin plugin = new GraphConnectionConfigOptionPlugin();
    plugin.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
    new CommandLine(plugin).parseArgs(args);
    return plugin.handleOption(new LogChannel("test"), hasMetadataProvider(), variables);
  }

  private IHasHopMetadataProvider hasMetadataProvider() {
    return new IHasHopMetadataProvider() {
      @Override
      public MultiMetadataProvider getMetadataProvider() {
        return new MultiMetadataProvider(
            metadataProvider.getTwoWayPasswordEncoder(),
            java.util.List.<IHopMetadataProvider>of(metadataProvider),
            variables);
      }

      @Override
      public void setMetadataProvider(MultiMetadataProvider provider) {
        // Not changed by this plugin
      }
    };
  }

  private String output() {
    return output.toString(StandardCharsets.UTF_8);
  }

  @Test
  void noOptionIsNoAction() throws Exception {
    assertFalse(handle());
    assertEquals("", output());
  }

  @Test
  void testSucceedsWithVariablesResolved() throws Exception {
    assertTrue(handle("--graph-connection-test=good"));
    assertTrue(output().contains("fake://graph.example.com"), output());
  }

  @Test
  void testFails() {
    HopException e =
        assertThrows(HopException.class, () -> handle("--graph-connection-test", "bad"));
    assertTrue(e.getMessage().contains("'bad'"), e.getMessage());
    assertTrue(
        org.apache.hop.core.Const.getStackTracker(e).contains("Unable to reach unreachable"));
  }

  @Test
  void testUnknownConnection() {
    HopException e =
        assertThrows(HopException.class, () -> handle("--graph-connection-test=missing"));
    assertTrue(e.getMessage().contains("missing"), e.getMessage());
  }

  @Test
  void testLegacyConnection() throws Exception {
    assertTrue(handle("--graph-connection-test=legacy"));
    assertTrue(output().contains("fake://legacy.example.com"), output());
  }

  @Test
  void graphDatabaseTypes() throws Exception {
    assertTrue(handle("--graph-database-types"));
    JsonNode types = new ObjectMapper().readTree(output());
    assertTrue(types.isArray());
    JsonNode fake = null;
    for (JsonNode type : types) {
      if ("FAKE_CONF".equals(type.get("pluginId").asText())) {
        fake = type;
      }
    }
    assertTrue(fake != null, output());
    assertEquals("Fake conf", fake.get("name").asText());
    assertEquals("FAKE_CONF_DIALECT", fake.get("dialectId").asText());
    assertEquals("Cypher", fake.get("queryLanguage").asText());
    assertTrue(fake.get("capabilities").get("supportingVectorIndexes").asBoolean());
    assertTrue(fake.get("capabilities").get("cypher").asBoolean());
  }

  @Test
  void connectionInfoWithoutPassword() throws Exception {
    assertTrue(handle("--graph-connection-info=good"));
    String json = output();
    assertFalse(json.contains(SECRET), json);
    assertFalse(json.contains("graph.example.com"), json);
    JsonNode info = new ObjectMapper().readTree(json);
    assertEquals("good", info.get("name").asText());
    assertEquals("graph-database-connection", info.get("metadataType").asText());
    assertEquals("FAKE_CONF", info.get("type").get("pluginId").asText());
    assertTrue(info.get("type").get("capabilities").get("supportingVectorIndexes").asBoolean());
  }

  @Test
  void legacyConnectionInfo() throws Exception {
    assertTrue(handle("--graph-connection-info=legacy"));
    JsonNode info = new ObjectMapper().readTree(output());
    assertEquals("fake-legacy-graph-connection", info.get("metadataType").asText());
    assertEquals("FAKE_CONF", info.get("type").get("pluginId").asText());
  }

  /** Through the hop conf command, the way the hop script runs it: exit code 0 or 1. */
  @Test
  void exitCodeOfTheConfCommand() throws Exception {
    assertEquals(0, runConfCommand("--graph-connection-test=good"));
    assertTrue(output().contains("fake://graph.example.com"), output());
    assertNotEquals(0, runConfCommand("--graph-connection-test=bad"));
  }

  private int runConfCommand(String... args) throws HopException {
    HopCommandConfig config = new HopCommandConfig();
    CommandLine cmd = new CommandLine(config);
    PrintStream errors = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
    cmd.setErr(new java.io.PrintWriter(errors, true));
    config.initialize(cmd, variables, hasMetadataProvider().getMetadataProvider());
    GraphConnectionConfigOptionPlugin plugin =
        (GraphConnectionConfigOptionPlugin)
            cmd.getMixins().get("GraphConnectionConfigOptionPlugin");
    if (plugin == null) {
      // Plugins in the engine jar aren't registered in a unit test
      plugin = new GraphConnectionConfigOptionPlugin();
      cmd.addMixin("GraphConnectionConfigOptionPlugin", plugin);
    }
    plugin.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
    return cmd.execute(args);
  }
}
