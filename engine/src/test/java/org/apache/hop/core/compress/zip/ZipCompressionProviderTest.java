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

package org.apache.hop.core.compress.zip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class ZipCompressionProviderTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public static final String PROVIDER_NAME = "Zip";

  public CompressionProviderFactory factory = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @Before
  public void setUp() {
    factory = CompressionProviderFactory.getInstance();
  }

  @Test
  public void testCtor() {
    ZipCompressionProvider ncp = new ZipCompressionProvider();
    assertNotNull(ncp);
  }

  @Test
  public void testGetName() {
    ZipCompressionProvider provider =
        (ZipCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    assertNotNull(provider);
    assertEquals(PROVIDER_NAME, provider.getName());
  }

  @Test
  public void testGetProviderAttributes() {
    ZipCompressionProvider provider =
        (ZipCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    assertEquals("ZIP compression", provider.getDescription());
    assertTrue(provider.supportsInput());
    assertTrue(provider.supportsOutput());
    assertEquals("zip", provider.getDefaultExtension());
  }

  @Test
  public void testCreateInputStream() throws IOException {
    ZipCompressionProvider provider =
        (ZipCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    ByteArrayInputStream in = new ByteArrayInputStream("Test".getBytes());
    ZipInputStream zis = new ZipInputStream(in);
    ZipCompressionInputStream inStream = new ZipCompressionInputStream(in, provider);
    assertNotNull(inStream);
    ZipCompressionInputStream ncis = provider.createInputStream(in);
    assertNotNull(ncis);
    ZipCompressionInputStream ncis2 = provider.createInputStream(zis);
    assertNotNull(ncis2);
  }

  @Test
  public void testCreateOutputStream() throws IOException {
    ZipCompressionProvider provider =
        (ZipCompressionProvider) factory.getCompressionProviderByName(PROVIDER_NAME);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ZipOutputStream zos = new ZipOutputStream(out);
    ZipCompressionOutputStream outStream = new ZipCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    ZipCompressionOutputStream ncis = provider.createOutputStream(out);
    assertNotNull(ncis);
    ZipCompressionOutputStream ncis2 = provider.createOutputStream(zos);
    assertNotNull(ncis2);
  }
}
