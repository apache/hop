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

package org.apache.hop.core.compress.gzip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.compress.CompressionInputStream;
import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.compress.ICompressionProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies that the "GZip" name resolves to the commons-compress backed provider through the
 * factory, keeping the historical provider attributes.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GzipCompressionProviderTest {
  public static final String PROVIDER_NAME = "GZip";

  public CompressionProviderFactory factory = null;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() {
    factory = CompressionProviderFactory.getInstance();
  }

  @Test
  void testCtor() {
    GzipCompressionProvider ncp = new GzipCompressionProvider();
    assertNotNull(ncp);
  }

  @Test
  void testGetName() {
    ICompressionProvider provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    assertNotNull(provider);
    assertEquals(PROVIDER_NAME, provider.getName());
  }

  @Test
  void testGetProviderAttributes() {
    ICompressionProvider provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    assertEquals("GZIP compression", provider.getDescription());
    assertTrue(provider.supportsInput());
    assertTrue(provider.supportsOutput());
    assertEquals("gz", provider.getDefaultExtension());
  }

  @Test
  void testCreateInputStream() throws IOException {
    ICompressionProvider provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    assertNotNull(provider);

    // Create an in-memory GZIP output stream for use by the input stream (to avoid exceptions)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream(baos);
    byte[] testBytes = "Test".getBytes();
    gos.write(testBytes);
    gos.close();

    CompressionInputStream ncis =
        provider.createInputStream(new ByteArrayInputStream(baos.toByteArray()));
    assertNotNull(ncis);
    byte[] decoded = ncis.readAllBytes();
    assertEquals(new String(testBytes), new String(decoded));
  }

  @Test
  void testCreateOutputStream() throws IOException {
    ICompressionProvider provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    assertNotNull(provider);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream(out);
    CompressionOutputStream outStream = new GzipCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    out = new ByteArrayOutputStream();
    CompressionOutputStream ncis = provider.createOutputStream(out);
    assertNotNull(ncis);
    CompressionOutputStream ncis2 = provider.createOutputStream(gos);
    assertNotNull(ncis2);
  }
}
