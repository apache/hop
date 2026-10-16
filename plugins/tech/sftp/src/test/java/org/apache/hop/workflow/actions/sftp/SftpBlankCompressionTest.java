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

package org.apache.hop.workflow.actions.sftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.InetAddress;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.vfs.sftp.client.SftpClient;
import org.apache.hop.workflow.actions.sftpput.ActionSftpPut;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * A blank Compression on the SFTP get / put actions means no compression settings for JSch. Up to
 * 2.17 a saved {@code <compression/>} loaded as null; since 2.18 it loads as an empty string, which
 * must not be handed to JSch as an (empty) compression algorithm list.
 */
class SftpBlankCompressionTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankCompressionOnSftpGetSetsNoCompression() throws Exception {
    ActionSftp action = new ActionSftp();
    action.loadXml(actionNode("<compression/>"), new MemoryMetadataProvider(), new Variables());

    assertNull(clientCompression(action.getCompression()));
  }

  @Test
  void blankCompressionOnSftpPutSetsNoCompression() throws Exception {
    ActionSftpPut action = new ActionSftpPut();
    action.loadXml(actionNode("<compression/>"), new MemoryMetadataProvider(), new Variables());

    assertNull(clientCompression(action.getCompression()));
  }

  @Test
  void zlibCompressionIsPassedOn() throws Exception {
    ActionSftpPut action = new ActionSftpPut();
    action.loadXml(
        actionNode("<compression>zlib</compression>"),
        new MemoryMetadataProvider(),
        new Variables());

    assertEquals("zlib@openssh.com,zlib", clientCompression(action.getCompression()));
  }

  /** The value SftpClient.login() puts into the JSch compression.s2c / compression.c2s config. */
  private static String clientCompression(String actionCompression) throws Exception {
    SftpClient client = new SftpClient(InetAddress.getLoopbackAddress(), 22, "user");
    client.setCompression(actionCompression);
    return client.getCompression();
  }

  private static Node actionNode(String compressionTag) throws Exception {
    String xml =
        "<action><name>sftp</name>"
            + "<servername>localhost</servername><serverport>22</serverport>"
            + "<username>user</username>"
            + compressionTag
            + "</action>";
    return XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "action");
  }
}
