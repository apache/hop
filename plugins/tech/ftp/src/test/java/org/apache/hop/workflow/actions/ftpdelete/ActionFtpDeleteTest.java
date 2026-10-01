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

package org.apache.hop.workflow.actions.ftpdelete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ActionFtpDeleteTest {
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
    ActionFtpDelete action =
        ActionSerializationTestUtil.testSerialization(
            "/action-ftp-delete.xml", ActionFtpDelete.class);

    assertEquals("FTP", action.getProtocol());
    assertEquals("prod-ftp", action.getConnection());
    assertEquals("prod-sftp", action.getSftpConnection());
    assertTrue(action.isUsingConnection());
    assertFalse(action.isUsingSftp());
    assertEquals("server", action.getServerName());
    assertEquals("21", action.getServerPort());
    assertEquals("user", action.getUserName());
    assertEquals("pass", action.getPassword());
    assertEquals("remote-folder", action.getRemoteDirectory());
    assertEquals("wildcard", action.getWildcard());
    assertEquals(999, action.getTimeout());
    assertTrue(action.isActiveConnection());
    assertTrue(action.isUseProxy());
    assertEquals("proxy-host", action.getProxyHost());
    assertEquals("proxy-port", action.getProxyPort());
    assertEquals("proxy-user", action.getProxyUsername());
    assertEquals("proxy-pass", action.getProxyPassword());
    assertTrue(action.isUsingPublicKey());
    assertEquals("keyfile", action.getKeyFilename());
    assertEquals("keypass", action.getKeyFilePass());
    assertEquals("10", action.getNrLimitSuccess());
    assertEquals("success_when_at_least", action.getSuccessCondition());
    assertTrue(action.isCopyPrevious());
    assertEquals("socks-host", action.getSocksProxyHost());
    assertEquals("1080", action.getSocksProxyPort());
    assertEquals("socks-user", action.getSocksProxyUsername());
    assertEquals("socks-pass", action.getSocksProxyPassword());
  }

  @Test
  @DisplayName("A copy carries every setting, including which connection it points at")
  void copyCarriesEverything() throws Exception {
    ActionFtpDelete action =
        ActionSerializationTestUtil.testSerialization(
            "/action-ftp-delete.xml", ActionFtpDelete.class);
    action.setName("delete them");
    action.setPluginId("FTP_DELETE");

    ActionFtpDelete copy = (ActionFtpDelete) action.clone();

    assertNotSame(action, copy);
    assertEquals("delete them", copy.getName());
    assertEquals("FTP_DELETE", copy.getPluginId());
    assertEquals(action.getConnection(), copy.getConnection());
    assertEquals(action.getSftpConnection(), copy.getSftpConnection());
    assertEquals(action.getProtocol(), copy.getProtocol());
    assertEquals(action.getServerName(), copy.getServerName());
    assertEquals(action.getServerPort(), copy.getServerPort());
    assertEquals(action.getUserName(), copy.getUserName());
    assertEquals(action.getPassword(), copy.getPassword());
    assertEquals(action.getRemoteDirectory(), copy.getRemoteDirectory());
    assertEquals(action.getWildcard(), copy.getWildcard());
    assertEquals(action.getTimeout(), copy.getTimeout());
    assertEquals(action.isUsingPublicKey(), copy.isUsingPublicKey());
    assertEquals(action.getKeyFilename(), copy.getKeyFilename());
    assertEquals(action.getKeyFilePass(), copy.getKeyFilePass());
    assertEquals(action.isUseProxy(), copy.isUseProxy());
    assertEquals(action.getProxyHost(), copy.getProxyHost());
    assertEquals(action.getSocksProxyHost(), copy.getSocksProxyHost());
    assertEquals(action.getNrLimitSuccess(), copy.getNrLimitSuccess());
    assertEquals(action.getSuccessCondition(), copy.getSuccessCondition());
    assertEquals(action.isCopyPrevious(), copy.isCopyPrevious());
  }

  @Test
  @DisplayName("A fresh action is a plain FTP one which evaluates its result")
  void defaults() {
    ActionFtpDelete action = new ActionFtpDelete();

    assertEquals(ActionFtpDelete.PROTOCOL_FTP, action.getProtocol());
    assertEquals("21", action.getServerPort());
    assertEquals(ActionFtpDelete.SUCCESS_IF_ALL_FILES_DOWNLOADED, action.getSuccessCondition());
    assertFalse(action.isUsingSftp());
    assertFalse(action.isUsingConnection());
    assertTrue(action.isEvaluation());
    assertEquals("FTP Delete", new ActionFtpDelete().getFtpConnectionName());
  }

  @Test
  @DisplayName("The timeout of the action is handed to the connection as milliseconds")
  void theTimeoutBecomesAConnectTimeout() {
    ActionFtpDelete action = new ActionFtpDelete();

    assertNull(action.getConnectTimeout(), "no timeout means the default of the library");

    action.setTimeout(5000);
    assertEquals("5000", action.getConnectTimeout());
  }

  @Test
  @DisplayName("An action without a server is reported by check()")
  void checkReportsTheMissingServer() {
    List<ICheckResult> remarks = new ArrayList<>();

    new ActionFtpDelete("delete")
        .check(remarks, mock(WorkflowMeta.class), new Variables(), new MemoryMetadataProvider());

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  @DisplayName("check() leaves the server settings alone when a connection brings them")
  void checkSkipsTheServerSettingsOfAConnection() {
    ActionFtpDelete action = new ActionFtpDelete("delete");
    action.setConnection("prod");
    List<ICheckResult> remarks = new ArrayList<>();

    action.check(remarks, mock(WorkflowMeta.class), new Variables(), new MemoryMetadataProvider());

    assertTrue(
        remarks.stream().noneMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "" + remarks);
  }

  @Test
  @DisplayName("The server of the action shows up as a resource it depends on")
  void theServerIsAResourceDependency() {
    ActionFtpDelete action = new ActionFtpDelete("delete");
    action.setServerName("ftp.example.com");

    List<ResourceReference> references =
        action.getResourceDependencies(new Variables(), mock(WorkflowMeta.class));

    assertEquals(1, references.size());
    assertEquals("ftp.example.com", references.get(0).getEntries().get(0).getResource());
  }
}
