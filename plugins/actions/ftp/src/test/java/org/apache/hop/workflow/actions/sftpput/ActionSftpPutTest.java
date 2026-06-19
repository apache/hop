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
 *
 */

package org.apache.hop.workflow.actions.sftpput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class ActionSftpPutTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionSftpPut action =
        ActionSerializationTestUtil.testSerialization("/action-sftp-put.xml", ActionSftpPut.class);

    assertEquals("server", action.getServerName());
    assertEquals("22", action.getServerPort());
    assertEquals("username", action.getUserName());
    assertEquals("password", action.getPassword());
    assertEquals("remote-folder", action.getRemoteDirectory());
    assertEquals("local-dir", action.getLocalDirectory());
    assertEquals("wildcard", action.getWildcard());
    assertTrue(action.isCopyingPrevious());
    assertTrue(action.isCopyingPreviousFiles());
    assertTrue(action.isAddFilenameResut());
    assertTrue(action.isUseKeyFilename());
    assertEquals("keyfile", action.getKeyFilename());
    assertEquals("keypass", action.getKeyFilePassword());
    assertEquals("zlib", action.getCompression());
    assertEquals("HTTP", action.getProxyType());
    assertEquals("proxy-host", action.getProxyHost());
    assertEquals("80", action.getProxyPort());
    assertEquals("proxy-user", action.getProxyUsername());
    assertEquals("proxy-pass", action.getProxyPassword());
    assertTrue(action.isCreateRemoteFolder());
    assertEquals(ActionSftpPut.AfterFtpAction.MOVE, action.getAfterSftpAction());
    assertEquals("move-to-folder", action.getDestinationFolder());
    assertTrue(action.isCreateDestinationFolder());
    assertTrue(action.isPreserveTargetFileTimestamp());
    assertTrue(action.isSuccessWhenNoFile());
  }

  @Test
  void testLegacyRemoveTagPromotedToDelete() throws Exception {
    // Pre-2.18 files stored "delete after put" as <remove>Y</remove>. After the
    // @HopMetadataProperty
    // migration that tag is no longer mapped, so it must be promoted to AfterFtpAction.DELETE on
    // load, otherwise those workflows silently stop deleting the source files.
    ActionSftpPut action = loadFromXml("<action><remove>Y</remove></action>");
    assertEquals(ActionSftpPut.AfterFtpAction.DELETE, action.getAfterSftpAction());
  }

  @Test
  void testMissingAfterActionDefaultsToNothingNotNull() throws Exception {
    // A legacy file without <aftersftpput> must not leave the enum null (it would NPE the dialog
    // and
    // the execute() switch).
    ActionSftpPut action = loadFromXml("<action></action>");
    assertEquals(ActionSftpPut.AfterFtpAction.NOTHING, action.getAfterSftpAction());
  }

  private static ActionSftpPut loadFromXml(String xml) throws Exception {
    Node node = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "action");
    ActionSftpPut action = new ActionSftpPut();
    action.loadXml(node, new MemoryMetadataProvider(), new Variables());
    return action;
  }
}
