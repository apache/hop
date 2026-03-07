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

package org.apache.hop.core.compress.snappy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class SnappyCompressionProviderTest {
  public static final String PROVIDER_NAME = "Snappy";

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
    SnappyCompressionProvider ncp = new SnappyCompressionProvider();
    assertNotNull(ncp);
  }

  @Test
  void testGetName() {
    SnappyCompressionProvider provider =
        (SnappyCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    assertNotNull(provider);
    assertEquals(PROVIDER_NAME, provider.getName());
  }

  @Test
  void testGetProviderAttributes() {
    SnappyCompressionProvider provider =
        (SnappyCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    assertEquals("Snappy compression", provider.getDescription());
    assertTrue(provider.supportsInput());
    assertTrue(provider.supportsOutput());
    assertNull(provider.getDefaultExtension());
  }

  @Test
  void testCreateInputStream() throws IOException {
    SnappyCompressionProvider provider =
        (SnappyCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    SnappyInputStream in = createSnappyInputStream();
    SnappyCompressionInputStream inStream = new SnappyCompressionInputStream(in, provider);
    assertNotNull(inStream);
    SnappyCompressionInputStream ncis = provider.createInputStream(in);
    assertNotNull(ncis);
  }

  @Test
  void testCreateOutputStream() throws IOException {
    SnappyCompressionProvider provider =
        (SnappyCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SnappyCompressionOutputStream outStream = new SnappyCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    SnappyCompressionOutputStream ncis = provider.createOutputStream(out);
    assertNotNull(ncis);
  }

  private SnappyInputStream createSnappyInputStream() throws IOException {
    // Create an in-memory ZIP output stream for use by the input stream (to avoid exceptions)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    SnappyOutputStream sos = new SnappyOutputStream(baos);
    byte[] testBytes = "Test".getBytes();
    sos.write(testBytes);
    sos.close();
    ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());

    return new SnappyInputStream(in);
  }
}
