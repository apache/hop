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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * The Kettle SFTPPut step carries its own server settings. On import those move into an SFTP
 * connection in the metadata and the step is pointed at it by name.
 */
class KettleImportSftpPutTest {

  private KettleImport kettleImport;
  private MemoryMetadataProvider memoryMetadataProvider;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
    // The client environment doesn't load metadata plugins, and that's what the importer looks the
    // SFTP connection up in.
    PluginRegistry.getInstance().registerType(MetadataPluginType.getInstance());
  }

  @BeforeEach
  void setUp() {
    memoryMetadataProvider = new MemoryMetadataProvider();
    kettleImport = new KettleImport();
    kettleImport.setMetadataProvider(
        new MultiMetadataProvider(
            null, Collections.singletonList(memoryMetadataProvider), kettleImport.getVariables()));
  }

  @Test
  void testServerSettingsMoveIntoAnSftpConnection() throws Exception {
    Document doc = parse(sftpPutStep("sftp.example.com", "customers"));
    processNode(doc);

    Node step = XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");

    // The step points at the connection...
    assertEquals("sftp-sftp-example-com", XmlHandler.getTagValue(step, "connection"));

    // ...and no longer holds anything about the server itself.
    for (String tag :
        new String[] {
          "servername",
          "serverport",
          "username",
          "password",
          "usekeyfilename",
          "keyfilename",
          "keyfilepass",
          "compression",
          "proxyType",
          "proxyHost",
          "proxyPort",
          "proxyUsername",
          "proxyPassword"
        }) {
      assertNull(XmlHandler.getTagValue(step, tag), tag + " should have moved to the connection");
    }

    // What the step does with the file stays where it is.
    assertEquals("filename", XmlHandler.getTagValue(step, "sourceFileFieldName"));
    assertEquals("folder", XmlHandler.getTagValue(step, "remoteDirectoryFieldName"));
    assertEquals("Y", XmlHandler.getTagValue(step, "createRemoteFolder"));

    // Kettle's typo is fixed on the way in.
    assertNull(XmlHandler.getTagValue(step, "addFilenameResut"));
    assertEquals("Y", XmlHandler.getTagValue(step, "addFilenameToResult"));

    SftpConnection connection =
        memoryMetadataProvider.getSerializer(SftpConnection.class).load("sftp-sftp-example-com");
    assertNotNull(connection);
    assertEquals("sftp.example.com", connection.getServerName());
    assertEquals("2222", connection.getServerPort());
    assertEquals("hop", connection.getUsername());
    assertEquals("secret", connection.getPassword());
    assertTrue(connection.isUseKeyFile());
    assertEquals("/home/hop/.ssh/id_rsa", connection.getKeyFilename());
    assertEquals("phrase", connection.getKeyPassphrase());
    assertEquals("zlib", connection.getCompression());
    assertEquals("HTTP", connection.getProxyType());
    assertEquals("proxy.example.com", connection.getProxyHost());
    assertEquals("8080", connection.getProxyPort());
    assertEquals("proxy-user", connection.getProxyUsername());
    assertEquals("proxy-secret", connection.getProxyPassword());
  }

  /** Steps talking to the same server share one connection. */
  @Test
  void testIdenticalStepsShareOneConnection() throws Exception {
    Document doc =
        parse(
            "<transformation>"
                + sftpPutStepBody("sftp.example.com", "customers")
                + sftpPutStepBody("sftp.example.com", "orders")
                + "</transformation>");
    processNode(doc);

    Node pipeline = XmlHandler.getSubNode(doc, "pipeline");
    assertEquals(
        "sftp-sftp-example-com",
        XmlHandler.getTagValue(XmlHandler.getSubNodeByNr(pipeline, "transform", 0), "connection"));
    assertEquals(
        "sftp-sftp-example-com",
        XmlHandler.getTagValue(XmlHandler.getSubNodeByNr(pipeline, "transform", 1), "connection"));
    assertEquals(
        1, memoryMetadataProvider.getSerializer(SftpConnection.class).listObjectNames().size());
  }

  /** Two servers, two connections, both with a name a VFS scheme can live with. */
  @Test
  void testDifferentServersGetDifferentConnections() throws Exception {
    Document doc =
        parse(
            "<transformation>"
                + sftpPutStepBody("sftp.example.com", "customers")
                + sftpPutStepBody("files.example.org", "orders")
                + "</transformation>");
    processNode(doc);

    assertEquals(
        2, memoryMetadataProvider.getSerializer(SftpConnection.class).listObjectNames().size());
    assertNotNull(
        memoryMetadataProvider.getSerializer(SftpConnection.class).load("sftp-files-example-org"));
  }

  /** Very old files delete the source file through a separate {@code remove} element. */
  @Test
  void testLegacyRemoveFlagBecomesDelete() throws Exception {
    String xml =
        sftpPutStep("sftp.example.com", "customers")
            .replace("<aftersftpput>move</aftersftpput>", "<remove>Y</remove>");
    Document doc = parse(xml);
    processNode(doc);

    Node step = XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");
    assertEquals("delete", XmlHandler.getTagValue(step, "aftersftpput"));
    assertNull(XmlHandler.getTagValue(step, "remove"));
  }

  private String sftpPutStep(String serverName, String stepName) {
    return "<transformation>" + sftpPutStepBody(serverName, stepName) + "</transformation>";
  }

  private String sftpPutStepBody(String serverName, String stepName) {
    return "<step>"
        + "<name>"
        + stepName
        + "</name>"
        + "<type>SFTPPut</type>"
        + "<servername>"
        + serverName
        + "</servername>"
        + "<serverport>2222</serverport>"
        + "<username>hop</username>"
        + "<password>secret</password>"
        + "<usekeyfilename>Y</usekeyfilename>"
        + "<keyfilename>/home/hop/.ssh/id_rsa</keyfilename>"
        + "<keyfilepass>phrase</keyfilepass>"
        + "<compression>zlib</compression>"
        + "<proxyType>HTTP</proxyType>"
        + "<proxyHost>proxy.example.com</proxyHost>"
        + "<proxyPort>8080</proxyPort>"
        + "<proxyUsername>proxy-user</proxyUsername>"
        + "<proxyPassword>proxy-secret</proxyPassword>"
        + "<sourceFileFieldName>filename</sourceFileFieldName>"
        + "<remoteDirectoryFieldName>folder</remoteDirectoryFieldName>"
        + "<remoteFilenameFieldName>target</remoteFilenameFieldName>"
        + "<createRemoteFolder>Y</createRemoteFolder>"
        + "<addFilenameResut>Y</addFilenameResut>"
        + "<aftersftpput>move</aftersftpput>"
        + "<destinationfolderFieldName>archive</destinationfolderFieldName>"
        + "<createdestinationfolder>Y</createdestinationfolder>"
        + "</step>";
  }

  private void processNode(Document doc) throws Exception {
    Method method =
        KettleImport.class.getDeclaredMethod(
            "processNode", Document.class, Node.class, Class.forName(EntryType()), int.class);
    method.setAccessible(true);
    method.invoke(kettleImport, doc, doc, otherEntryType(), 0);
  }

  private static String EntryType() {
    return "org.apache.hop.imports.kettle.KettleImport$EntryType";
  }

  private Object otherEntryType() throws Exception {
    Class<?> entryTypeClass = Class.forName(EntryType());
    for (Object constant : entryTypeClass.getEnumConstants()) {
      if ("OTHER".equals(constant.toString())) {
        return constant;
      }
    }
    throw new IllegalStateException("No OTHER entry type");
  }

  private static Document parse(String xml) throws Exception {
    return XmlParserFactoryProducer.createSecureDocBuilderFactory()
        .newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }
}
