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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies the class-loader resilience of {@link HopCompressionRegistry} and that an extension
 * plugin contributes its format through the {@link CompressionPlugin} annotation alone (no {@code
 * META-INF/services} file required), without being registered twice when it also ships a
 * commons-compress SPI file.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class HopCompressionRegistryTest {

  static final String EXTENSION_ID = "hop-test-extension";
  static final String EXTENSION_NAME = "HopTestExtension";
  static final String REGISTRY_ONLY_ID = "hop-registry-only";

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @Test
  void noneAndZipAlwaysAvailable() {
    HopCompressionRegistry registry = HopCompressionRegistry.getInstance();
    assertTrue(registry.getCompressorInputStreamNames().contains("NONE"));
    assertTrue(registry.getCompressorOutputStreamNames().contains("NONE"));
    assertTrue(registry.getCompressorInputStreamNames().contains("ZIP"));
    assertTrue(registry.getCompressorOutputStreamNames().contains("ZIP"));
  }

  @Test
  void registerUnregisterProvider() {
    HopCompressionRegistry registry = HopCompressionRegistry.getInstance();
    CompressorStreamProvider extension = new TestExtensionStreamProvider();
    registry.register(extension);
    try {
      assertTrue(registry.getCompressorInputStreamNames().contains("HOP-TEST-EXTENSION"));
      assertTrue(registry.getCompressorOutputStreamNames().contains("HOP-TEST-EXTENSION"));
      // registering twice must not duplicate
      registry.register(extension);
      assertEquals(1, countProvider(registry, extension));
    } finally {
      registry.unregister(extension);
    }
    assertFalse(registry.getCompressorInputStreamNames().contains("HOP-TEST-EXTENSION"));
  }

  @Test
  void extensionDiscoveredThroughAnnotationOnly() throws Exception {
    CompressionProviderFactory factory = CompressionProviderFactory.getInstance();
    // the metadata class is registered through @CompressionPlugin discovery, no SPI file needed;
    // the codec comes from a registered commons-compress provider (extension plugin)
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTestExtensionProvider.class.getName(),
            CompressionPluginType.class,
            CompressionPlugin.class);
    factory.invalidateProviderCache();
    HopCompressionRegistry.getInstance().register(new TestExtensionStreamProvider());
    try {
      CompressionFormat format = factory.getFormat(EXTENSION_ID);
      assertNotNull(format, "extension format must be discoverable via annotation");
      assertEquals(EXTENSION_NAME, format.name());

      byte[] payload = "extension payload".getBytes(StandardCharsets.UTF_8);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (CompressionOutputStream out = factory.createCompressionOutputStream(EXTENSION_ID, bos)) {
        out.write(payload);
      }
      try (CompressionInputStream in =
          factory.createCompressionInputStream(
              EXTENSION_ID, new ByteArrayInputStream(bos.toByteArray()))) {
        assertArrayEquals(payload, in.readAllBytes());
      }
    } finally {
      HopCompressionRegistry.getInstance().unregister(new TestExtensionStreamProvider());
    }
  }

  @Test
  void registryOnlyCodecIsDerived() {
    HopCompressionRegistry registry = HopCompressionRegistry.getInstance();
    CompressorStreamProvider extension = new RegistryOnlyStreamProvider();
    registry.register(extension);
    try {
      CompressionFormat format =
          CompressionProviderFactory.getInstance().getFormat(REGISTRY_ONLY_ID);
      assertNotNull(format, "codec without annotation class must be derived from the registry");
      assertTrue(format.supportsInput());
    } finally {
      registry.unregister(extension);
    }
  }

  private static int countProvider(
      HopCompressionRegistry registry, CompressorStreamProvider extension) {
    int count = 0;
    for (CompressorStreamProvider provider : registry.getRegisteredProviders()) {
      if (provider == extension) {
        count++;
      }
    }
    return count;
  }

  /** A codec contributed to the registry without an annotation class. */
  static final class RegistryOnlyStreamProvider implements CompressorStreamProvider {

    @Override
    public CompressorInputStream createCompressorInputStream(
        String name, InputStream in, boolean decompressUntilEof) throws CompressorException {
      return new CompressorInputStream() {
        @Override
        public int read() throws IOException {
          return in.read();
        }

        @Override
        public void close() throws IOException {
          in.close();
        }
      };
    }

    @Override
    public <T extends OutputStream> CompressorOutputStream<T> createCompressorOutputStream(
        String name, T out) throws CompressorException {
      return new CompressorOutputStream<>(out) {
        @Override
        public void write(int b) throws IOException {
          out.write(b);
        }

        @Override
        public void close() throws IOException {
          out.close();
        }
      };
    }

    @Override
    public Set<String> getInputStreamCompressorNames() {
      return Set.of(REGISTRY_ONLY_ID);
    }

    @Override
    public Set<String> getOutputStreamCompressorNames() {
      return Set.of(REGISTRY_ONLY_ID);
    }
  }

  /** A trivial extension codec contributed by an external plugin. */
  static final class TestExtensionStreamProvider implements CompressorStreamProvider {

    @Override
    public CompressorInputStream createCompressorInputStream(
        String name, InputStream in, boolean decompressUntilEof) throws CompressorException {
      return new CompressorInputStream() {
        @Override
        public int read() throws IOException {
          return in.read();
        }

        @Override
        public void close() throws IOException {
          in.close();
        }
      };
    }

    @Override
    public <T extends OutputStream> CompressorOutputStream<T> createCompressorOutputStream(
        String name, T out) throws CompressorException {
      return new CompressorOutputStream<>(out) {
        @Override
        public void write(int b) throws IOException {
          out.write(b);
        }

        @Override
        public void close() throws IOException {
          out.close();
        }
      };
    }

    @Override
    public Set<String> getInputStreamCompressorNames() {
      return Set.of(EXTENSION_ID);
    }

    @Override
    public Set<String> getOutputStreamCompressorNames() {
      return Set.of(EXTENSION_ID);
    }
  }
}
