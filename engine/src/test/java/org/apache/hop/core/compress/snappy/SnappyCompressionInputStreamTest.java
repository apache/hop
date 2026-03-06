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

import java.io.ByteArrayInputStream;
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
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class SnappyCompressionInputStreamTest {
  public static final String PROVIDER_NAME = "Snappy";

  protected CompressionProviderFactory factory = null;
  protected SnappyCompressionInputStream inStream = null;
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
    inStream = new SnappyCompressionInputStream(createSnappyInputStream(), provider) {};
  }

  @Test
  void testCtor() {
    assertNotNull(inStream);
  }

  @Test
  void getCompressionProvider() {
    assertEquals(PROVIDER_NAME, provider.getName());
  }

  @Test
  void testNextEntry() throws IOException {
    assertNull(inStream.nextEntry());
  }

  @Test
  void testClose() throws IOException {
    inStream = new SnappyCompressionInputStream(createSnappyInputStream(), provider);
    assertNotNull(inStream);
    inStream.close();
  }

  @Test
  void testRead() throws IOException {
    assertEquals(inStream.available(), inStream.read(new byte[100], 0, inStream.available()));
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
