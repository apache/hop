/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.imports.kettle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

class KettleImportTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    // Registers the DatabasePluginType so the "GENERIC" connection type can be resolved.
    HopClientEnvironment.init();
  }

  /**
   * Kettle/PDI stores the JDBC connection string of a generic database connection in a CUSTOM_URL
   * attribute, while Hop's GenericDatabaseMeta exposes it through the manualUrl field. Verify the
   * importer bridges the two so the URL (and driver class) survive the import (#5383).
   */
  @Test
  void testGenericConnectionKeepsJdbcUrlAndDriver() throws Exception {
    String xml =
        "<transformation>"
            + "<connection>"
            + "<name>My Generic DB</name>"
            + "<server/>"
            + "<type>GENERIC</type>"
            + "<access>Native</access>"
            + "<database/>"
            + "<port/>"
            + "<username>myuser</username>"
            + "<password>mypass</password>"
            + "<attributes>"
            + "<attribute><code>CUSTOM_URL</code>"
            + "<attribute>jdbc:generic://myhost:1234/mydb</attribute></attribute>"
            + "<attribute><code>CUSTOM_DRIVER_CLASS</code>"
            + "<attribute>com.example.MyDriver</attribute></attribute>"
            + "</attributes>"
            + "</connection>"
            + "</transformation>";

    KettleImport kettleImport = new KettleImport();
    invokeImportDbConnections(kettleImport, parse(xml));

    List<DatabaseMeta> connections = kettleImport.getConnectionsList();
    assertEquals(1, connections.size());

    DatabaseMeta databaseMeta = connections.get(0);
    assertEquals("My Generic DB", databaseMeta.getName());
    // The URL from the CUSTOM_URL attribute must be carried over to manualUrl...
    assertEquals("jdbc:generic://myhost:1234/mydb", databaseMeta.getIDatabase().getManualUrl());
    // ...and therefore be returned by getURL() for a generic connection.
    assertEquals("jdbc:generic://myhost:1234/mydb", databaseMeta.getIDatabase().getURL("", "", ""));
    // The driver class rides along unchanged because Kettle and Hop share the attribute name.
    assertEquals(
        "com.example.MyDriver",
        databaseMeta.getIDatabase().getAttributes().get("CUSTOM_DRIVER_CLASS"));
  }

  /** A generic connection without a CUSTOM_URL attribute must not gain a manual URL. */
  @Test
  void testGenericConnectionWithoutCustomUrlHasNoManualUrl() throws Exception {
    String xml =
        "<transformation>"
            + "<connection>"
            + "<name>No URL DB</name>"
            + "<type>GENERIC</type>"
            + "<access>Native</access>"
            + "<attributes>"
            + "<attribute><code>CUSTOM_DRIVER_CLASS</code>"
            + "<attribute>com.example.MyDriver</attribute></attribute>"
            + "</attributes>"
            + "</connection>"
            + "</transformation>";

    KettleImport kettleImport = new KettleImport();
    invokeImportDbConnections(kettleImport, parse(xml));

    DatabaseMeta databaseMeta = kettleImport.getConnectionsList().get(0);
    assertNull(databaseMeta.getIDatabase().getManualUrl());
  }

  private static Document parse(String xml) throws Exception {
    try (ByteArrayInputStream in = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))) {
      return XmlParserFactoryProducer.createSecureDocBuilderFactory()
          .newDocumentBuilder()
          .parse(in);
    }
  }

  private static void invokeImportDbConnections(KettleImport kettleImport, Document doc)
      throws Exception {
    FileObject kettleFile = mock(FileObject.class);
    FileName fileName = mock(FileName.class);
    when(kettleFile.getName()).thenReturn(fileName);
    when(fileName.getURI()).thenReturn("file:///tmp/test.ktr");

    Method method =
        KettleImport.class.getDeclaredMethod(
            "importDbConnections", Document.class, FileObject.class);
    method.setAccessible(true);
    method.invoke(kettleImport, doc, kettleFile);
  }
}
