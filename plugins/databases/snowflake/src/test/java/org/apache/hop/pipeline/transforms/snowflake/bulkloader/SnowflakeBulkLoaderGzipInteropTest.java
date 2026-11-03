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

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.compress.ICompressionProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies that the gzip stream produced through the compression factory (commons-compress backed
 * since the compression rework) is a standard gzip stream, as expected by Snowflake's file format
 * detection.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class SnowflakeBulkLoaderGzipInteropTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @Test
  void gzipProviderProducesStandardGzip() throws IOException {
    ICompressionProvider provider =
        CompressionProviderFactory.getInstance().getCompressionProviderByName("GZip");
    assertNotNull(provider, "GZip provider must be resolvable by name");
    assertTrue(provider.supportsOutput());

    byte[] payload = "Snowflake bulk load payload\n".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (CompressionOutputStream out = provider.createOutputStream(bos)) {
      out.write(payload);
    }
    byte[] compressed = bos.toByteArray();

    // standard gzip magic
    assertTrue(compressed.length > 2, "gzip output must not be empty");
    assertArrayEquals(new byte[] {0x1f, (byte) 0x8b}, new byte[] {compressed[0], compressed[1]});

    // readable by the JDK gzip decoder, as Snowflake's sniffer would
    try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
      assertArrayEquals(payload, gz.readAllBytes());
    }
  }
}
