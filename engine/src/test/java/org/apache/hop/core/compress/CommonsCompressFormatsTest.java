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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamProvider;
import org.apache.hop.core.compress.spi.HopCompressorStreamProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Verifies the commons-compress backed formats and Hop's SPI contribution to that registry. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class CommonsCompressFormatsTest {

  private static final String PAYLOAD = "Apache Hop compression test payload\nsecond line\n";

  private final CompressionProviderFactory factory = CompressionProviderFactory.getInstance();

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    // the provider classes are discovered through the plugin machinery
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @Test
  void spiRegistersHopProvider() {
    List<CompressorStreamProvider> providers =
        ServiceLoader.load(CompressorStreamProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .toList();
    assertTrue(
        providers.stream().anyMatch(p -> p instanceof HopCompressorStreamProvider),
        "HopCompressorStreamProvider must be registered via META-INF/services");
  }

  @Test
  void ccSingletonIncludesNoneAndZip() {
    var singleton = CompressorStreamFactory.getSingleton();
    assertTrue(singleton.getCompressorInputStreamProviders().containsKey("NONE"));
    assertTrue(singleton.getCompressorOutputStreamProviders().containsKey("NONE"));
    assertTrue(singleton.getCompressorInputStreamProviders().containsKey("ZIP"));
    assertTrue(singleton.getCompressorOutputStreamProviders().containsKey("ZIP"));
  }

  @Test
  void roundTripForAllOutputFormats() throws IOException {
    for (CompressionFormat format : factory.getOutputFormats()) {
      if ("none".equals(format.id())
          || "zip".equals(format.id())
          || "pack200".equals(format.id())) {
        continue; // none/zip covered by dedicated tests; pack200 is a jar-oriented codec
      }
      if (!isCommonsCompressBacked(format)) {
        continue; // legacy wire-incompatible providers (Snappy, Hadoop-snappy) are covered
        // elsewhere
      }
      byte[] payload = PAYLOAD.getBytes(StandardCharsets.UTF_8);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (CompressionOutputStream out = factory.createCompressionOutputStream(format.id(), bos)) {
        out.write(payload);
      }
      byte[] compressed = bos.toByteArray();
      assertTrue(compressed.length > 0, "Compressed output must not be empty: " + format.id());

      try (CompressionInputStream in =
          factory.createCompressionInputStream(format.id(), new ByteArrayInputStream(compressed))) {
        byte[] roundTripped = in.readAllBytes();
        assertArrayEquals(payload, roundTripped, "Round trip failed for format " + format.id());
      }
    }
  }

  /** The legacy providers are not backed by a commons-compress codec. */
  private static boolean isCommonsCompressBacked(CompressionFormat format) {
    return format.id() != null
        && !"SNAPPY".equalsIgnoreCase(format.id())
        && !"HADOOPSNAPPY".equalsIgnoreCase(format.id());
  }

  @Test
  void inputOnlyFormatsRejectOutput() {
    for (CompressionFormat format : factory.getInputFormats()) {
      if (format.supportsOutput()) {
        continue;
      }
      ioThrows(
          () -> factory.createCompressionOutputStream(format.id(), new ByteArrayOutputStream()));
    }
  }

  @Test
  void noneRoundTrip() throws IOException {
    byte[] payload = PAYLOAD.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("none", bos)) {
      out.write(payload);
    }
    assertArrayEquals(payload, bos.toByteArray(), "none must be a passthrough");

    try (CompressionInputStream in =
        factory.createCompressionInputStream("none", new ByteArrayInputStream(bos.toByteArray()))) {
      assertArrayEquals(payload, in.readAllBytes());
    }
  }

  @Test
  void zipEntryRoundTrip() throws IOException {
    byte[] payload = PAYLOAD.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("zip", bos)) {
      out.addEntry("/some/folder/data.txt", "txt");
      out.write(payload);
    }
    byte[] zipBytes = bos.toByteArray();
    // ZIP local file header magic
    assertEquals(0x50, zipBytes[0] & 0xff);
    assertEquals(0x4b, zipBytes[1] & 0xff);

    try (CompressionInputStream in =
        factory.createCompressionInputStream("zip", new ByteArrayInputStream(zipBytes))) {
      Object entry = in.nextEntry();
      assertNotNull(entry, "zip must expose an entry");
      // historical Hop behavior: folder stripped, then the given extension appended
      assertEquals("data.txt.txt", entry.toString());
      assertArrayEquals(payload, in.readAllBytes());
    }
  }

  @Test
  void magicBytes() throws IOException {
    byte[] payload = PAYLOAD.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("zstd", bos)) {
      out.write(payload);
    }
    byte[] bytes = bos.toByteArray();
    assertArrayEquals(
        new byte[] {0x28, (byte) 0xb5, 0x2f, (byte) 0xfd}, Arrays.copyOf(bytes, 4), "zstd magic");

    bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("gzip", bos)) {
      out.write(payload);
    }
    bytes = bos.toByteArray();
    assertArrayEquals(new byte[] {0x1f, (byte) 0x8b}, Arrays.copyOf(bytes, 2), "gzip magic");

    bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("bzip2", bos)) {
      out.write(payload);
    }
    bytes = bos.toByteArray();
    assertArrayEquals(new byte[] {0x42, 0x5a, 0x68}, Arrays.copyOf(bytes, 3), "bzip2 magic");

    bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("xz", bos)) {
      out.write(payload);
    }
    bytes = bos.toByteArray();
    assertArrayEquals(
        new byte[] {(byte) 0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00},
        Arrays.copyOf(bytes, 6),
        "xz magic");
  }

  @Test
  void detectCompressionFormat() throws IOException {
    byte[] payload = PAYLOAD.getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("gzip", bos)) {
      out.write(payload);
    }
    assertEquals(
        "gz",
        factory.detectCompressionFormat(new ByteArrayInputStream(bos.toByteArray())),
        "detect gzip");

    bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = factory.createCompressionOutputStream("zstd", bos)) {
      out.write(payload);
    }
    assertEquals(
        "zstd",
        factory.detectCompressionFormat(new ByteArrayInputStream(bos.toByteArray())),
        "detect zstd");

    assertNull(
        factory.detectCompressionFormat(new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5})),
        "unknown bytes are not detected");
  }

  private interface IoCallable {
    void call() throws IOException;
  }

  private void ioThrows(IoCallable callable) {
    try {
      callable.call();
      throw new AssertionError("Expected IOException to be thrown");
    } catch (IOException expected) {
      // ok
    }
  }
}
