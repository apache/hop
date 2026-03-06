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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.compress.ICompressionProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ZipCompressionOutputStreamTest {
  static final String PROVIDER_NAME = "Zip";

  CompressionProviderFactory factory = null;
  ZipCompressionOutputStream outStream = null;

  private ByteArrayOutputStream internalStream;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() {
    factory = CompressionProviderFactory.getInstance();
    ICompressionProvider provider = factory.getCompressionProviderByName(PROVIDER_NAME);
    internalStream = new ByteArrayOutputStream();
    outStream = new ZipCompressionOutputStream(internalStream, provider);
  }

  @AfterEach
  void tearDown() {
    internalStream = null;
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
    outStream = new ZipCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    outStream.close();
  }

  @Test
  void testAddEntryAndWrite() throws IOException {
    ICompressionProvider provider = outStream.getCompressionProvider();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    outStream = new ZipCompressionOutputStream(out, provider);
    assertNotNull(outStream);
    outStream.addEntry("./test.zip", null);
    outStream.write("Test".getBytes());
  }

  @Test
  void directoriesHierarchyIsIgnored() throws Exception {
    outStream.addEntry(createFilePath("1", "~", "hop", "dir"), "txt");
    outStream.close();

    Map<String, String> map = readArchive(internalStream.toByteArray());
    assertNotNull(map);
    assertEquals(1, map.size());
    assertEquals("1.txt", map.keySet().iterator().next());
  }

  @Test
  void extraZipExtensionIsIgnored() throws Exception {
    outStream.addEntry(createFilePath("1.zip", "~", "hop", "dir"), "txt");
    outStream.close();

    Map<String, String> map = readArchive(internalStream.toByteArray());
    assertEquals(1, map.size());
    assertEquals("1.txt", map.keySet().iterator().next());
  }

  @Test
  void absentExtensionIsOk() throws Exception {
    outStream.addEntry(createFilePath("1", "~", "hop", "dir"), null);
    outStream.close();

    Map<String, String> map = readArchive(internalStream.toByteArray());
    assertEquals(1, map.size());
    assertEquals("1", map.keySet().iterator().next());
  }

  @Test
  void createsWellFormedArchive() throws Exception {
    outStream.addEntry("1", "txt");
    outStream.write("1.txt".getBytes());
    outStream.addEntry("2", "txt");
    outStream.write("2.txt".getBytes());
    outStream.close();

    Map<String, String> map = readArchive(internalStream.toByteArray());
    assertEquals("1.txt", map.remove("1.txt"));
    assertEquals("2.txt", map.remove("2.txt"));
    assertTrue(map.isEmpty());
  }

  private static String createFilePath(String file, String... directories) {
    StringBuilder sb = new StringBuilder();
    for (String dir : directories) {
      sb.append(dir).append(Const.FILE_SEPARATOR);
    }
    return sb.append(file).toString();
  }

  private static Map<String, String> readArchive(byte[] bytes) throws Exception {
    Map<String, String> result = new HashMap<>();

    ZipInputStream stream = new ZipInputStream(new ByteArrayInputStream(bytes));
    byte[] buf = new byte[256];
    ZipEntry entry;
    while ((entry = stream.getNextEntry()) != null) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      int read;
      while ((read = stream.read(buf)) > 0) {
        os.write(buf, 0, read);
      }
      result.put(entry.getName(), os.toString());
    }

    return result;
  }
}
