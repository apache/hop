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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
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
class ZipCompressionInputStreamTest {
  public static final String PROVIDER_NAME = "Zip";

  public CompressionProviderFactory factory = null;
  public ZipCompressionInputStream inStream = null;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    factory = CompressionProviderFactory.getInstance();
    ICompressionProvider provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    ByteArrayInputStream in = new ByteArrayInputStream("Test".getBytes());
    inStream = new ZipCompressionInputStream(in, provider) {};
  }

  @Test
  void testCtor() {
    assertNotNull(inStream);
  }

  @Test
  void getZIPCompressionProvider() {
    ICompressionProvider provider = inStream.getCompressionProvider();
    assertEquals(PROVIDER_NAME, provider.getName());
  }

  @Test
  void testNextEntry() throws IOException {
    ZipInputStream zipInputStream = createZIPInputStream();
    assertNotNull(zipInputStream);
    assertNotNull(zipInputStream.getNextEntry());
  }

  @Test
  void testClose() throws IOException {
    createZIPInputStream().close();
  }

  @Test
  void testRead() throws IOException {
    ICompressionProvider provider = inStream.getCompressionProvider();
    ByteArrayInputStream in = new ByteArrayInputStream("Test".getBytes());
    inStream = new ZipCompressionInputStream(in, provider) {};
    assertNotNull(inStream);
    inStream.read(new byte[100], 0, inStream.available());
  }

  private ZipInputStream createZIPInputStream() throws IOException {
    // Create an in-memory ZIP output stream for use by the input stream (to avoid exceptions)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ZipOutputStream gos = new ZipOutputStream(baos);
    gos.putNextEntry(new ZipEntry("./test.txt"));
    byte[] testBytes = "Test".getBytes();
    gos.write(testBytes);
    ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());

    assertNotNull(in);
    return new ZipInputStream(in);
  }
}
