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

package org.apache.hop.vfs.minio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.vfs.minio.metadata.MinioMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class MinioVfsPluginTest {

  private MinioVfsPlugin plugin;

  @BeforeEach
  void setUp() {
    plugin = new MinioVfsPlugin();
  }

  @Test
  void testGetUrlSchemes() {
    String[] schemes = plugin.getUrlSchemes();
    assertNotNull(schemes, "URL schemes should not be null");
    assertEquals(
        0, schemes.length, "MinIO derives schemes from metadata, so array should be empty");
  }

  @Test
  void testGetProvider() {
    // There are no fixed schemes, so there is nothing to register a provider under. Handing one
    // over anyway leaves it unreachable and unclosed on the file system manager :
    // "DefaultFilesystemManager.close: not all components are closed".
    assertNull(plugin.getProvider(), "MinIO has no fixed scheme, so it has no fixed provider");
  }

  @Test
  void testGetProvidersWithNullVariables() {
    Map<String, FileProvider> providers = plugin.getProviders(null);
    assertNotNull(providers, "Providers map should not be null");
    // Should return empty map when variables is null (metadata provider will fail gracefully)
    assertTrue(providers.isEmpty(), "Should return empty map when variables is null");
  }

  @Test
  void testGetProvidersWithVariables() {
    IVariables variables = new Variables();
    Map<String, FileProvider> providers = plugin.getProviders(variables);

    assertNotNull(providers, "Providers map should not be null");
    // Without actual metadata configured, should return empty map
    // The method catches exceptions and returns empty map
  }

  @Test
  void testGetProvidersWithEmptyVariables() {
    IVariables variables = Variables.getADefaultVariableSpace();
    Map<String, FileProvider> providers = plugin.getProviders(variables);

    assertNotNull(providers, "Providers map should not be null");
  }

  @Test
  void testPluginAnnotation() {
    // Verify the plugin has the VfsPlugin annotation by checking class annotations
    assertTrue(
        plugin.getClass().isAnnotationPresent(org.apache.hop.core.vfs.plugin.VfsPlugin.class),
        "Plugin should have VfsPlugin annotation");
  }

  @Test
  void testPluginType() {
    org.apache.hop.core.vfs.plugin.VfsPlugin annotation =
        plugin.getClass().getAnnotation(org.apache.hop.core.vfs.plugin.VfsPlugin.class);

    assertNotNull(annotation, "VfsPlugin annotation should not be null");
    assertEquals("minio", annotation.type(), "Plugin type should be 'minio'");
    assertEquals("Minio VFS plugin", annotation.typeDescription(), "Type description should match");
  }

  @Test
  void connectionMetadataSharesTheClassLoaderGroupOfThisPlugin() {
    // The plugin reads MinioMeta objects it did not deserialize itself when the connections come
    // from a resource export.
    String pluginGroup =
        plugin
            .getClass()
            .getAnnotation(org.apache.hop.core.vfs.plugin.VfsPlugin.class)
            .classLoaderGroup();
    String metadataGroup =
        MinioMeta.class
            .getAnnotation(org.apache.hop.metadata.api.HopMetadata.class)
            .classLoaderGroup();

    assertEquals(
        pluginGroup,
        metadataGroup,
        "MinioMeta and the Minio VFS plugin must share a class loader group");
  }

  @Test
  void testGetProvidersErrorHandling() {
    // Test that getProviders handles errors gracefully
    IVariables variables = new Variables();
    variables.setVariable("SOME_VAR", "value");

    Map<String, FileProvider> providers = plugin.getProviders(variables);

    // Should not throw exception, even if metadata provider fails
    assertNotNull(providers, "Should handle errors and return empty map");
  }

  @Test
  void testGetProvidersReturnsMap() {
    IVariables variables = new Variables();
    Map<String, FileProvider> providers = plugin.getProviders(variables);

    assertNotNull(providers, "Providers should be a map");
    assertTrue(providers instanceof Map, "Should be a Map instance");
  }
}
