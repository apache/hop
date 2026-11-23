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

package org.apache.hop.core.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.TestUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.server.HopServerMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class SerializableMetadataProviderTest {

  @BeforeEach
  void before() throws Exception {
    TestUtil.registerTestPluginTypes();
  }

  @Test
  void testRoundTrip() throws Exception {
    MemoryMetadataProvider source = new MemoryMetadataProvider();
    IHopMetadataSerializer<HopServerMeta> sourceSerializer =
        source.getSerializer(HopServerMeta.class);
    HopServerMeta sourceServer1 =
        new HopServerMeta(
            "server1", "hostname1", "8181", "username1", "password1", null, null, null, false);
    sourceSerializer.save(sourceServer1);
    HopServerMeta sourceServer2 =
        new HopServerMeta(
            "server2", "hostname2", "8282", "username2", "password2", null, null, null, true);
    sourceSerializer.save(sourceServer2);

    SerializableMetadataProvider serializableMetadataProvider =
        new SerializableMetadataProvider(source);
    String json = serializableMetadataProvider.toJson();
    SerializableMetadataProvider target = new SerializableMetadataProvider(json);
    IHopMetadataSerializer<HopServerMeta> targetSerializer =
        target.getSerializer(HopServerMeta.class);
    HopServerMeta targetServer1 = targetSerializer.load("server1");
    assertNotNull(targetServer1);
    assertEquals(sourceServer1.getName(), targetServer1.getName());
    assertEquals(sourceServer1.getHostname(), targetServer1.getHostname());
    assertEquals(sourceServer1.getPort(), targetServer1.getPort());
    assertEquals(sourceServer1.getUsername(), targetServer1.getUsername());
    assertEquals(sourceServer1.getPassword(), targetServer1.getPassword());

    HopServerMeta targetServer2 = targetSerializer.load("server2");
    assertNotNull(targetServer2);
    assertEquals(sourceServer2.getName(), targetServer2.getName());
    assertEquals(sourceServer2.getHostname(), targetServer2.getHostname());
    assertEquals(sourceServer2.getPort(), targetServer2.getPort());
    assertEquals(sourceServer2.getUsername(), targetServer2.getUsername());
    assertEquals(sourceServer2.getPassword(), targetServer2.getPassword());
  }

  /**
   * An object which can't be loaded, for example a pipeline run configuration referencing an engine
   * plugin which isn't installed, shouldn't keep the other objects from being copied. Otherwise a
   * pipeline or workflow which doesn't use that object at all can't even be prepared for execution.
   */
  @Test
  void testUnloadableObjectIsSkipped() throws Exception {
    MemoryMetadataProvider source =
        new MemoryMetadataProvider() {
          @Override
          public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer(
              Class<T> managedClass) throws HopException {
            IHopMetadataSerializer<T> serializer = super.getSerializer(managedClass);
            if (HopServerMeta.class.equals(managedClass)) {
              return new FailingOnLoadSerializer<>(serializer, "broken");
            }
            return serializer;
          }
        };
    IHopMetadataSerializer<HopServerMeta> sourceSerializer =
        source.getSerializer(HopServerMeta.class);
    sourceSerializer.save(
        new HopServerMeta(
            "server1", "hostname1", "8181", "username1", "password1", null, null, null, false));
    sourceSerializer.save(
        new HopServerMeta(
            "broken", "hostname2", "8282", "username2", "password2", null, null, null, false));

    SerializableMetadataProvider target = new SerializableMetadataProvider(source);
    IHopMetadataSerializer<HopServerMeta> targetSerializer =
        target.getSerializer(HopServerMeta.class);

    assertNotNull(targetSerializer.load("server1"));
    assertNull(targetSerializer.load("broken"));

    // The same needs to hold for the JSON round trip used to ship metadata to an engine.
    //
    SerializableMetadataProvider roundTripped = new SerializableMetadataProvider(target.toJson());
    assertNotNull(roundTripped.getSerializer(HopServerMeta.class).load("server1"));
  }

  @Test
  void testEmptyUnknownTypesDoNotRequirePlugins() throws Exception {
    final var json =
        """
        {
          "salesforceconnection": [],
          "server": [{"name": "server1", "hostname": "localhost"}],
          "neo4j-connection": []
        }
        """;
    final var provider =
        new SerializableMetadataProvider(json) {
          @Override
          public <T extends IHopMetadata> Class<T> getMetadataClassForKey(final String key)
              throws HopException {
            assertEquals("server", key, "Empty types must not require a registered plugin");
            return super.getMetadataClassForKey(key);
          }
        };

    final var server = provider.getSerializer(HopServerMeta.class).load("server1");
    assertNotNull(server);
    assertEquals("localhost", server.getHostname());
  }

  @Test
  void testUnavailableNeo4jConnectionWithManualUrlsIsSkipped() throws Exception {
    // Shape from integration-tests/neo4j/metadata/neo4j-connection/Test.json.
    final var json =
        """
        {
          "neo4j-connection": [
            {
              "server": "${NEO_HOSTNAME}",
              "browserPort": "7474",
              "databaseName": "",
              "manualUrls": [],
              "automaticVariable": "",
              "usingEncryption": false,
              "routing": true,
              "name": "Test",
              "username": "${NEO_USERNAME}"
            }
          ],
          "server": [{"name": "server1", "hostname": "localhost"}]
        }
        """;

    assertServerAfterUnavailableConnection(json, "neo4j-connection");
  }

  @Test
  void testUnavailableGraphDatabaseConnectionWithManualUrlsIsSkipped() throws Exception {
    // Shape from plugins/tech/neo4j/src/main/samples/metadata/
    // graph-database-connection/demo-neo4j.json.
    final var json =
        """
        {
          "graph-database-connection": [
            {
              "graphDatabase": {
                "NEO4J": {
                  "databaseName": "",
                  "manualUrls": [],
                  "automaticVariable": "",
                  "server": "${NEO4J_HOSTNAME}",
                  "boltPort": "${NEO4J_PORT}",
                  "automatic": true,
                  "protocol": "neo4j",
                  "routing": false,
                  "usingEncryption": false
                }
              },
              "name": "demo-neo4j"
            }
          ],
          "server": [{"name": "server1", "hostname": "localhost"}]
        }
        """;

    assertServerAfterUnavailableConnection(json, "graph-database-connection");
  }

  /**
   * An object of an installed type that refers to a plugin this Hop doesn't have is left out; the
   * objects around it still load.
   */
  @Test
  void testObjectThatCannotBeBuiltIsSkipped() throws Exception {
    final var json =
        """
        {
          "pipeline-run-configuration": [
            {"name": "before", "engineRunConfiguration": {"Local": {}}},
            {"name": "unavailable", "engineRunConfiguration": {"NotInstalledEngine": {}}},
            {"name": "after", "engineRunConfiguration": {"Local": {}}}
          ],
          "server": [{"name": "server1", "hostname": "localhost"}]
        }
        """;

    final var provider = new SerializableMetadataProvider(json);

    final var runConfigurations = provider.getSerializer(PipelineRunConfiguration.class);
    assertEquals(
        List.of("after", "before"), runConfigurations.listObjectNames().stream().sorted().toList());
    assertNotNull(runConfigurations.load("before").getEngineRunConfiguration());
    assertNotNull(provider.getSerializer(HopServerMeta.class).load("server1"));
  }

  /** A type no plugin provides is skipped without looking up its class, which logs an error. */
  @Test
  void testUnknownTypeWithObjectsIsSkippedWithoutLookup() throws Exception {
    final var json =
        """
        {
          "not-installed-type": [{"name": "x", "nested": [1, 2]}],
          "server": [{"name": "server1", "hostname": "localhost"}]
        }
        """;
    final var provider =
        new SerializableMetadataProvider(json) {
          @Override
          public <T extends IHopMetadata> Class<T> getMetadataClassForKey(final String key)
              throws HopException {
            assertEquals("server", key, "A type no plugin provides must not be looked up");
            return super.getMetadataClassForKey(key);
          }
        };

    assertNotNull(provider.getSerializer(HopServerMeta.class).load("server1"));
  }

  private void assertServerAfterUnavailableConnection(
      final String json, final String unavailableKey) throws Exception {
    final var provider =
        new SerializableMetadataProvider(json) {
          @Override
          public <T extends IHopMetadata> Class<T> getMetadataClassForKey(final String key)
              throws HopException {
            if (unavailableKey.equals(key)) {
              throw new HopException("Metadata plugin is not installed: " + key);
            }
            return super.getMetadataClassForKey(key);
          }
        };

    final var server = provider.getSerializer(HopServerMeta.class).load("server1");
    assertNotNull(server);
    assertEquals("localhost", server.getHostname());
  }

  /** Delegates everything but throws when one specific object is loaded. */
  private static class FailingOnLoadSerializer<T extends IHopMetadata>
      implements IHopMetadataSerializer<T> {
    private final IHopMetadataSerializer<T> delegate;
    private final String failingName;

    FailingOnLoadSerializer(IHopMetadataSerializer<T> delegate, String failingName) {
      this.delegate = delegate;
      this.failingName = failingName;
    }

    @Override
    public T load(String objectName) throws HopException {
      if (failingName.equals(objectName)) {
        throw new HopException("Unable to find the plugin for object '" + objectName + "'");
      }
      return delegate.load(objectName);
    }

    @Override
    public String getDescription() {
      return delegate.getDescription();
    }

    @Override
    public void save(T object) throws HopException {
      delegate.save(object);
    }

    @Override
    public T delete(String name) throws HopException {
      return delegate.delete(name);
    }

    @Override
    public List<String> listObjectNames() throws HopException {
      return delegate.listObjectNames();
    }

    @Override
    public List<T> loadAll() throws HopException {
      return delegate.loadAll();
    }

    @Override
    public boolean exists(String name) throws HopException {
      return delegate.exists(name);
    }

    @Override
    public Class<T> getManagedClass() {
      return delegate.getManagedClass();
    }

    @Override
    public IHopMetadataProvider getMetadataProvider() {
      return delegate.getMetadataProvider();
    }
  }
}
