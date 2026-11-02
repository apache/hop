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

package org.apache.hop.core.compress;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.compress.hadoopsnappy.HadoopSnappyCompressionProvider;
import org.apache.hop.core.compress.snappy.SnappyCompressionProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class CompressionProviderFactoryTest {

  CompressionProviderFactory factory = null;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    // The commons-compress registry is used directly; the plugin registry is not required
    // anymore for the built-in formats. Keep the plugin type registered for the legacy formats
    // (Snappy, Hadoop-snappy) which are still plugin based.
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() {
    factory = CompressionProviderFactory.getInstance();
  }

  @Test
  void testGetInstance() {
    assertNotNull(factory);
  }

  @Test
  void testCreateCoreProviders() {
    // The commons-compress backed formats resolve through their provider classes
    ICompressionProvider provider = factory.createCompressionProviderInstance("None");
    assertNotNull(provider);
    assertEquals("None", provider.getName());
    assertEquals("No compression", provider.getDescription());

    provider = factory.createCompressionProviderInstance("Zip");
    assertNotNull(provider);
    assertEquals("Zip", provider.getName());
    assertEquals("ZIP compression", provider.getDescription());

    provider = factory.createCompressionProviderInstance("GZip");
    assertNotNull(provider);
    assertEquals("GZip", provider.getName());
    assertEquals("GZIP compression", provider.getDescription());

    // Zstd is now a built-in format
    provider = factory.createCompressionProviderInstance("Zstd");
    assertNotNull(provider);
    assertEquals("Zstd", provider.getName());
    assertEquals("zst", provider.getDefaultExtension());

    // The xerial snappy and hadoop-snappy remain legacy plugin providers (wire compatibility)
    provider = factory.createCompressionProviderInstance("Snappy");
    assertNotNull(provider);
    assertTrue(provider.getClass().isAssignableFrom(SnappyCompressionProvider.class));
    assertEquals("Snappy", provider.getName());
    assertEquals("Snappy compression", provider.getDescription());

    provider = factory.createCompressionProviderInstance("Hadoop-snappy");
    assertNotNull(provider);
    assertTrue(provider.getClass().isAssignableFrom(HadoopSnappyCompressionProvider.class));
    assertEquals("Hadoop-snappy", provider.getName());
    assertEquals("Hadoop Snappy compression", provider.getDescription());
  }

  /** All historic names plus the new formats are exposed by the names listing. */
  @Test
  void getCoreProviderNames() {
    final HashMap<String, Boolean> foundProvider =
        new HashMap<>() {
          {
            put("None", false);
            put("Zip", false);
            put("GZip", false);
            put("Zstd", false);
            put("BZip2", false);
            put("XZ", false);
            put("Snappy", false);
            put("Hadoop-snappy", false);
          }
        };

    String[] providers = factory.getCompressionProviderNames();
    assertNotNull(providers);
    for (String provider : providers) {
      Boolean known = foundProvider.get(provider);
      if (known != null) {
        foundProvider.put(provider, true);
      }
    }

    boolean foundAllProviders = true;
    for (Boolean b : foundProvider.values()) {
      foundAllProviders = foundAllProviders && b;
    }

    assertTrue(foundAllProviders, () -> "Missing providers: " + foundProvider);
  }

  /** The providers listing exposes all formats, including the new ones. */
  @Test
  void getCoreProviders() {
    final HashMap<String, Boolean> foundProvider =
        new HashMap<>() {
          {
            put("None", false);
            put("Zip", false);
            put("GZip", false);
            put("Zstd", false);
            put("BZip2", false);
            put("XZ", false);
            put("LZ4", false);
            put("Snappy", false);
            put("Hadoop-snappy", false);
          }
        };

    Collection<ICompressionProvider> providers = factory.getCompressionProviders();
    assertNotNull(providers);
    for (ICompressionProvider provider : providers) {
      Boolean known = foundProvider.get(provider.getName());
      if (known != null) {
        foundProvider.put(provider.getName(), true);
      }
    }

    boolean foundAllProviders = true;
    for (Boolean b : foundProvider.values()) {
      foundAllProviders = foundAllProviders && b;
    }

    assertTrue(foundAllProviders, () -> "Missing providers: " + foundProvider);
  }

  @Test
  void namesAndProvidersAreAligned() {
    String[] names = factory.getCompressionProviderNames();
    Collection<ICompressionProvider> providers = factory.getCompressionProviders();
    assertEquals(providers.size(), names.length);
    for (ICompressionProvider provider : providers) {
      boolean found = false;
      for (String name : names) {
        if (provider.getName().equals(name)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "name missing for provider " + provider.getName());
    }
    // legacy wire-incompatible providers still resolvable by name
    assertNotNull(factory.getCompressionProviderByName("Snappy"));
    assertNotNull(factory.getCompressionProviderByName("Hadoop-snappy"));
  }

  @Test
  void getNonExistentProvider() {
    ICompressionProvider provider = factory.createCompressionProviderInstance("Fake");
    assertNull(provider);
    provider = factory.getCompressionProviderByName(null);
    assertNull(provider);
    provider = factory.getCompressionProviderByName("Invalid");
    assertNull(provider);
  }

  @Test
  void getFormatsExposesNewFormats() {
    List<CompressionFormat> formats = factory.getFormats();
    assertNotNull(formats);
    // "None" stays the first entry so empty compression settings keep their historical default
    assertEquals("None", formats.get(0).name());
    CompressionFormat zstd = factory.getFormat("Zstd");
    assertNotNull(zstd);
    assertEquals("zstd", zstd.id());
    assertEquals("zst", zstd.extension());
    assertTrue(zstd.supportsInput());
    assertTrue(zstd.supportsOutput());

    CompressionFormat gzip = factory.getFormat("GZip");
    assertNotNull(gzip);
    assertEquals("GZip", gzip.name());
    assertEquals("gz", gzip.extension());

    CompressionFormat zip = factory.getFormat("Zip");
    assertNotNull(zip);
    assertEquals("zip", zip.id());
    assertEquals("zip", zip.extension());
  }

  @Test
  void legacyFormatsAreAvailableInAllLists() {
    // Snappy and Hadoop-snappy have no commons-compress codec; they must remain available
    // (their codec is classpath-loaded) and appear in the format lists and dialogs
    CompressionFormat snappy = factory.getFormat("Snappy");
    assertNotNull(snappy);
    assertTrue(snappy.supportsInput());
    assertTrue(snappy.supportsOutput());

    List<CompressionFormat> formats = factory.getFormats();
    assertTrue(formats.stream().anyMatch(f -> "Snappy".equals(f.name())));
    assertTrue(formats.stream().anyMatch(f -> "Hadoop-snappy".equals(f.name())));

    List<CompressionFormat> inputFormats = factory.getInputFormats();
    assertTrue(inputFormats.stream().anyMatch(f -> "Snappy".equals(f.name())));
    assertTrue(inputFormats.stream().anyMatch(f -> "Hadoop-snappy".equals(f.name())));

    List<CompressionFormat> outputFormats = factory.getOutputFormats();
    assertTrue(outputFormats.stream().anyMatch(f -> "Snappy".equals(f.name())));
    assertTrue(outputFormats.stream().anyMatch(f -> "Hadoop-snappy".equals(f.name())));
  }

  @Test
  void registryOnlyCodecResolvesAndRoundTrips() throws Exception {
    // a codec contributed to HopCompressionRegistry without an annotation class must be
    // resolvable and usable through the factory (getFormat + create*Stream)
    HopCompressionRegistry registry = HopCompressionRegistry.getInstance();
    var extension = new HopCompressionRegistryTest.RegistryOnlyStreamProvider();
    registry.register(extension);
    try {
      factory.invalidateProviderCache();
      CompressionFormat format = factory.getFormat("hop-registry-only");
      assertNotNull(format);
      assertTrue(format.supportsInput());
      assertTrue(format.supportsOutput());

      byte[] payload = "registry only payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (CompressionOutputStream out =
          factory.createCompressionOutputStream("hop-registry-only", bos)) {
        out.write(payload);
      }
      try (CompressionInputStream in =
          factory.createCompressionInputStream(
              "hop-registry-only", new ByteArrayInputStream(bos.toByteArray()))) {
        assertArrayEquals(payload, in.readAllBytes());
      }
    } finally {
      registry.unregister(extension);
      factory.invalidateProviderCache();
    }
  }

  @Test
  void inputOnlyFormatsAreFilteredFromOutputFormats() {
    CompressionFormat brotli = factory.getFormat("Brotli");
    assertNotNull(brotli);
    assertTrue(brotli.supportsInput());
    assertFalse(brotli.supportsOutput());

    List<CompressionFormat> outputFormats = factory.getOutputFormats();
    assertTrue(outputFormats.stream().noneMatch(f -> "Brotli".equals(f.name())));

    List<CompressionFormat> inputFormats = factory.getInputFormats();
    assertTrue(inputFormats.stream().anyMatch(f -> "Brotli".equals(f.name())));
  }
}
