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

package org.apache.hop.workflow.actions.ftpdelete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.core.CoreModuleProperties;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.AcceptAllPasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The deprecated SFTP protocol of the FTP delete action, against an embedded SSH server.
 *
 * <p>Deprecated is not the same as unsupported: the workflows which use it have to keep working
 * until it is actually removed, and the key file settings it always carried around were dead code
 * until recently, so this is where that is held down.
 */
class ActionFtpDeleteSftpTest {

  private static final String USER = "alice";
  private static final String PASSWORD = "secret";
  private static final String HOST = "127.0.0.1";

  @TempDir private static Path serverRoot;

  /** Outside the served directory: a host key in there would show up in every listing. */
  @TempDir private static Path serverPrivate;

  private static SshServer sshServer;

  @BeforeAll
  static void startServer() throws Exception {
    HopEnvironment.init();
    sshServer = SshServer.setUpDefaultServer();
    sshServer.setHost(HOST);
    sshServer.setPort(0);
    sshServer.setKeyPairProvider(
        new SimpleGeneratorHostKeyProvider(serverPrivate.resolve("hostkey.ser")));
    sshServer.setPasswordAuthenticator(AcceptAllPasswordAuthenticator.INSTANCE);
    sshServer.setFileSystemFactory(new VirtualFileSystemFactory(serverRoot));
    sshServer.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
    CoreModuleProperties.IDLE_TIMEOUT.set(sshServer, Duration.ofSeconds(10));
    sshServer.start();
  }

  @AfterAll
  static void stopServer() throws IOException {
    if (sshServer != null) {
      sshServer.stop();
    }
  }

  @BeforeEach
  void emptyTheServer() throws Exception {
    try (var files = Files.list(serverRoot)) {
      for (Path file : files.toList()) {
        Files.deleteIfExists(file);
      }
    }
  }

  @Test
  @DisplayName("Files matching the wildcard are deleted over SFTP")
  void deletesOverSftp() throws Exception {
    Files.writeString(serverRoot.resolve("report.txt"), "gone");
    Files.writeString(serverRoot.resolve("keep.csv"), "stays");

    ActionFtpDelete action = configure();
    action.setWildcard(".*\\.txt");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrFilesRetrieved());
    assertFalse(Files.exists(serverRoot.resolve("report.txt")));
    assertTrue(Files.exists(serverRoot.resolve("keep.csv")));
  }

  @Test
  @DisplayName("A named SFTP connection replaces the server settings on the action")
  void deletesThroughANamedSftpConnection() throws Exception {
    Files.writeString(serverRoot.resolve("report.txt"), "gone");

    SftpConnection stored = new SftpConnection();
    stored.setName("prod-sftp");
    stored.setServerName(HOST);
    stored.setServerPort(Integer.toString(sshServer.getPort()));
    stored.setUsername(USER);
    stored.setPassword(PASSWORD);
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(SftpConnection.class).save(stored);

    ActionFtpDelete action = configure();
    action.setMetadataProvider(metadataProvider);
    action.setSftpConnection("prod-sftp");
    action.setWildcard(".*");
    // Nonsense inline settings: picking those up instead has to fail the test.
    action.setServerName("nowhere.invalid");
    action.setServerPort("1");

    Result result = action.execute(new Result(), 0);

    assertTrue(action.isUsingConnection());
    assertTrue(result.isResult());
    assertFalse(Files.exists(serverRoot.resolve("report.txt")));
  }

  @Test
  @DisplayName("Deleting in a subdirectory of the server")
  void deletesInARemoteDirectory() throws Exception {
    Files.createDirectories(serverRoot.resolve("inbox"));
    Files.writeString(serverRoot.resolve("inbox/report.txt"), "gone");
    Files.writeString(serverRoot.resolve("report.txt"), "stays");

    ActionFtpDelete action = configure();
    action.setRemoteDirectory("/inbox");
    action.setWildcard(".*\\.txt");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertFalse(Files.exists(serverRoot.resolve("inbox/report.txt")));
    assertTrue(
        Files.exists(serverRoot.resolve("report.txt")), "only the subdirectory was asked for");
  }

  @Test
  @DisplayName("Set to use a private key but given none: an error, not a password login")
  void usingAPublicKeyWithoutAKeyFileIsAnError() throws Exception {
    Files.writeString(serverRoot.resolve("report.txt"), "stays");

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setUsingPublicKey(true);
    action.setKeyFilename("");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
    assertTrue(Files.exists(serverRoot.resolve("report.txt")));
  }

  @Test
  @DisplayName("A private key file which isn't there is an error naming the file")
  void aMissingKeyFileIsAnError(@TempDir Path elsewhere) throws Exception {
    Files.writeString(serverRoot.resolve("report.txt"), "stays");

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setUsingPublicKey(true);
    action.setKeyFilename(elsewhere.resolve("no-such-key").toString());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(Files.exists(serverRoot.resolve("report.txt")));
  }

  @Test
  @DisplayName("An unreachable SFTP server is an error, not a silent success")
  void anUnreachableServerIsAnError() {
    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setServerName("nowhere.invalid");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
  }

  @Test
  @DisplayName("An empty directory over SFTP is a success")
  void anEmptyDirectoryIsFine() {
    ActionFtpDelete action = configure();
    action.setWildcard(".*");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  @DisplayName("The action says it is on SFTP, which is what drives the deprecation warning")
  void theActionKnowsItIsOnSftp() {
    assertTrue(configure().isUsingSftp());
    ActionFtpDelete ftp = configure();
    ftp.setProtocol(ActionFtpDelete.PROTOCOL_FTP);
    assertFalse(ftp.isUsingSftp());
    assertNotNull(ActionFtpDelete.PROTOCOL_SFTP);
    assertNull(configure().getControlEncoding());
    assertTrue(configure().isBinaryMode());
  }

  @Test
  @DisplayName("A proxy which isn't there fails the connect rather than being ignored")
  void anUnreachableProxyIsAnError() throws Exception {
    Files.writeString(serverRoot.resolve("report.txt"), "stays");

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setUseProxy(true);
    action.setProxyHost("proxy.invalid");
    action.setProxyPort("3128");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(Files.exists(serverRoot.resolve("report.txt")));
  }

  @SuppressWarnings("unchecked")
  private ActionFtpDelete configure() {
    ActionFtpDelete action = new ActionFtpDelete("SFTP delete");
    action.setProtocol(ActionFtpDelete.PROTOCOL_SFTP);
    action.setServerName(HOST);
    action.setServerPort(Integer.toString(sshServer.getPort()));
    action.setUserName(USER);
    action.setPassword(PASSWORD);

    IWorkflowEngine<WorkflowMeta> workflowEngine = mock(IWorkflowEngine.class);
    when(workflowEngine.isStopped()).thenReturn(false);
    // Debug: every log line in the action runs, so a bad message key fails a test.
    when(workflowEngine.getLogLevel()).thenReturn(LogLevel.DEBUG);
    when(workflowEngine.getContainerId()).thenReturn("test-container");
    action.setParentWorkflow(workflowEngine);
    return action;
  }
}
