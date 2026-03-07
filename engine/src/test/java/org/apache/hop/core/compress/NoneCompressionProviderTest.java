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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hop.core.compress.NoneCompressionProvider.NoneCompressionInputStream;
import org.apache.hop.core.compress.NoneCompressionProvider.NoneCompressionOutputStream;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class NoneCompressionProviderTest {
  static final String PROVIDER_NAME = "None";

  CompressionProviderFactory factory = null;

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
    NoneCompressionProvider ncp = new NoneCompressionProvider();
    assertNotNull(ncp);
  }

  @Test
  void testGetName() {
    NoneCompressionProvider provider =
        (NoneCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    assertNotNull(provider);
    assertEquals(PROVIDER_NAME, provider.getName());
  }

  @Test
  void testGetProviderAttributes() {
    NoneCompressionProvider provider =
        (NoneCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    assertEquals("No compression", provider.getDescription());
    assertTrue(provider.supportsInput());
    assertTrue(provider.supportsOutput());
    assertNull(provider.getDefaultExtension());
  }

  @Test
  void testCreateInputStream() throws IOException {
    NoneCompressionProvider provider =
        (NoneCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    ByteArrayInputStream in = new ByteArrayInputStream("Test".getBytes());
    NoneCompressionInputStream inStream = new NoneCompressionInputStream(in, provider);
    assertNotNull(inStream);
    NoneCompressionInputStream ncis = (NoneCompressionInputStream) provider.createInputStream(in);
    assertNotNull(ncis);
  }

  @Test
  void testCreateOutputStream() throws IOException {
    NoneCompressionProvider provider =
        (NoneCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    NoneCompressionOutputStream outStream = new NoneCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    NoneCompressionOutputStream ncis =
        (NoneCompressionOutputStream) provider.createOutputStream(out);
    assertNotNull(ncis);
  }
}
