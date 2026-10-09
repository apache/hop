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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class GraphDatabaseCapabilitiesTest {

  /** A dialect with a capability the interface doesn't know, which has to show up anyway. */
  public static class FakeDialect extends CypherGraphDialect {
    public FakeDialect() {
      super("FAKE");
    }

    @Override
    public boolean isSupportingRelationshipIndexes() {
      return false;
    }

    public boolean isSupportingTimeTravel() {
      return true;
    }

    /** Not a capability: it takes an argument. */
    @Override
    public boolean isRequiringAutoCommit(String statement) {
      return true;
    }

    /** Not a capability: the name doesn't match. */
    public boolean isFast() {
      return true;
    }

    /** Not a capability: no boolean. */
    public String isSupportingNothing() {
      return "no";
    }
  }

  @GraphDatabasePlugin(
      id = "FAKE_CAPABILITIES",
      name = "Fake capabilities",
      description = "A fake graph database",
      documentationUrl = "/fake.html")
  public static class FakeGraphDatabase extends BaseGraphDatabase {
    @Override
    public IGraphDialect getGraphDialect() {
      return new FakeDialect();
    }

    @Override
    public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
        throws HopException {
      throw new HopException("Not connecting");
    }

    @Override
    public String test(IVariables variables, String connectionName) {
      return "fake://" + connectionName;
    }
  }

  @BeforeAll
  static void registerFakeType() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            FakeGraphDatabase.class.getName(),
            GraphDatabasePluginType.class,
            GraphDatabasePlugin.class);
  }

  @Test
  void capabilitiesOfTheDialectAreReadReflectively() {
    Map<String, Boolean> capabilities =
        GraphDatabaseCapabilities.getCapabilities(new FakeDialect());

    assertEquals(Boolean.TRUE, capabilities.get("cypher"));
    assertEquals(Boolean.TRUE, capabilities.get("supportingNodeIndexes"));
    assertEquals(Boolean.FALSE, capabilities.get("supportingRelationshipIndexes"));
    // Derived from the node indexes
    assertEquals(Boolean.TRUE, capabilities.get("supportingIndexes"));
    assertEquals(Boolean.TRUE, capabilities.get("supportingTimeTravel"));
    assertFalse(capabilities.containsKey("requiringAutoCommit"));
    assertFalse(capabilities.containsKey("fast"));
    assertFalse(capabilities.containsKey("supportingNothing"));
    // Sorted by name
    assertEquals("cypher", capabilities.keySet().iterator().next());
  }

  @Test
  void dialectOfAClassWhichIsNotPublic() {
    IGraphDialect dialect =
        new IGraphDialect() {
          @Override
          public String getId() {
            return "ANONYMOUS";
          }

          @Override
          public boolean isCypher() {
            return false;
          }
        };
    Map<String, Boolean> capabilities = GraphDatabaseCapabilities.getCapabilities(dialect);
    assertEquals(Boolean.FALSE, capabilities.get("cypher"));
    assertEquals(Boolean.FALSE, capabilities.get("supportingVectorIndexes"));
    assertEquals(
        GraphDatabaseCapabilities.QUERY_LANGUAGE_GREMLIN,
        GraphDatabaseCapabilities.getQueryLanguage(dialect));
  }

  @Test
  void capabilitiesOfAType() throws Exception {
    GraphDatabaseCapabilities capabilities = GraphDatabaseCapabilities.of("FAKE_CAPABILITIES");

    assertEquals("FAKE_CAPABILITIES", capabilities.getPluginId());
    assertEquals("Fake capabilities", capabilities.getName());
    assertEquals("A fake graph database", capabilities.getDescription());
    assertTrue(capabilities.getDocumentationUrl().endsWith("/fake.html"));
    assertEquals("FAKE", capabilities.getDialectId());
    assertEquals(GraphDatabaseCapabilities.QUERY_LANGUAGE_CYPHER, capabilities.getQueryLanguage());
    assertTrue(capabilities.getCapabilities().get("supportingTimeTravel"));

    List<GraphDatabaseCapabilities> all = GraphDatabaseCapabilities.getAll();
    assertTrue(all.stream().anyMatch(c -> "FAKE_CAPABILITIES".equals(c.getPluginId())));

    assertThrows(HopException.class, () -> GraphDatabaseCapabilities.of("NO_SUCH_TYPE"));
  }

  @Test
  void json() throws Exception {
    String json =
        GraphDatabaseCapabilities.toJson(GraphDatabaseCapabilities.of("FAKE_CAPABILITIES").toMap());

    assertTrue(json.contains("\"pluginId\" : \"FAKE_CAPABILITIES\""), json);
    assertTrue(json.contains("\"queryLanguage\" : \"Cypher\""), json);
    assertTrue(json.contains("\"supportingTimeTravel\" : true"), json);
    assertTrue(
        json.indexOf("\"pluginId\"") < json.indexOf("\"capabilities\""),
        "pluginId comes first: " + json);
  }

  @Test
  void names() {
    assertEquals(
        "supportingVectorIndexes",
        GraphDatabaseCapabilities.getCapabilityName("isSupportingVectorIndexes"));
    assertEquals("cypher", GraphDatabaseCapabilities.getCapabilityName("isCypher"));
  }

  @Test
  void labels() {
    assertEquals(
        "Vector indexes", GraphDatabaseCapabilities.getCapabilityLabel("supportingVectorIndexes"));
    // Without a label in the messages, derived from the name
    assertEquals(
        "Time travel", GraphDatabaseCapabilities.getCapabilityLabel("supportingTimeTravel"));
    assertEquals(
        "Requiring something", GraphDatabaseCapabilities.getCapabilityLabel("requiringSomething"));
  }

  @Test
  void graphDatabaseWithoutPlugin() {
    GraphDatabaseCapabilities capabilities = GraphDatabaseCapabilities.of(new FakeGraphDatabase());
    assertNull(capabilities.getPluginId());
    assertNotNull(capabilities.getCapabilities());
    assertEquals("FAKE", capabilities.getDialectId());
  }
}
