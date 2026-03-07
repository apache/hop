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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;
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
class GzipCompressionInputStreamTest {
  public static final String PROVIDER_NAME = "GZip";

  protected CompressionProviderFactory factory = null;
  protected GzipCompressionInputStream inStream = null;
  protected ICompressionProvider provider = null;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    factory = CompressionProviderFactory.getInstance();
    provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    inStream = new GzipCompressionInputStream(createGZIPInputStream(), provider) {};
  }

  @Test
  void testCtor() {
    assertNotNull(inStream);
  }

  @Test
  void getZIPCompressionProvider() {
    ICompressionProvider p = inStream.getCompressionProvider();
    assertEquals(PROVIDER_NAME, p.getName());
  }

  @Test
  void testNextEntry() throws IOException {
    assertNull(inStream.nextEntry());
  }

  @Test
  void testClose() throws IOException {
    inStream = new GzipCompressionInputStream(createGZIPInputStream(), provider) {};
    assertNotNull(inStream);
    inStream.close();
  }

  @Test
  void testRead() throws IOException {
    inStream = new GzipCompressionInputStream(createGZIPInputStream(), provider) {};

    int read = inStream.read(new byte[100], 0, inStream.available());
    assertEquals(0, read);
  }

  protected InputStream createGZIPInputStream() throws IOException {
    // Create an in-memory GZIP output stream for use by the input stream (to avoid exceptions)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream(baos);
    byte[] testBytes = "Test".getBytes();
    gos.write(testBytes);
    return new ByteArrayInputStream(baos.toByteArray());
  }
}
