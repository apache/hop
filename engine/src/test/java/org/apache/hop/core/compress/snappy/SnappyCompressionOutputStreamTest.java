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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.compress.ICompressionProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class SnappyCompressionOutputStreamTest {
  public static final String PROVIDER_NAME = "Snappy";

  public CompressionProviderFactory factory = null;
  public SnappyCompressionOutputStream outStream = null;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    factory = CompressionProviderFactory.getInstance();
    ICompressionProvider provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    ByteArrayOutputStream in = new ByteArrayOutputStream();
    outStream = new SnappyCompressionOutputStream(in, provider);
  }

  @Test
  void testCtor() {
    assertNotNull(outStream);
  }

  @Test
  void getCompressionProvider() {
    ICompressionProvider provider = outStream.getCompressionProvider();
    assertEquals(PROVIDER_NAME, provider.getName());
  }

  @Test
  void testClose() throws IOException {
    ICompressionProvider provider = outStream.getCompressionProvider();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    outStream = new SnappyCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    outStream.close();
  }

  @Test
  void testWrite() throws IOException {
    ICompressionProvider provider = outStream.getCompressionProvider();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    outStream = new SnappyCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    outStream.write("Test".getBytes());
  }

  @Test
  void testAddEntry() throws IOException {
    ICompressionProvider provider = outStream.getCompressionProvider();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    outStream = new SnappyCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    outStream.addEntry(null, null);
  }
}
