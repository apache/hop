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

package org.apache.hop.vfs.ftp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.actions.ftp.ActionFtp;
import org.apache.hop.workflow.actions.ftpdelete.ActionFtpDelete;
import org.apache.hop.workflow.actions.ftpput.ActionFtpPut;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Nothing an FTP action writes to the log may contain a password.
 *
 * <p>Logs get pasted into issues, shipped to log aggregators and kept for years, so a password in
 * one is a password disclosed. These run the actions at the most verbose level there is - every log
 * statement in them executes - and then read the log back looking for the secrets that went in.
 *
 * <p>The failure paths matter at least as much as the happy ones: an error message assembled from
 * "everything I was configured with" is the usual way a credential ends up in a log.
 */
class CredentialsAreNotLeakedTest {

  /** Distinctive enough that finding it in the log can't be a coincidence. */
  private static final String PASSWORD = "s3cr3t-Passw0rd-Do-Not-Log";

  private static final String PROXY_PASSWORD = "pr0xy-Passw0rd-Do-Not-Log";
  private static final String SOCKS_PASSWORD = "s0cks-Passw0rd-Do-Not-Log";
  private static final String KEYSTORE_PASSWORD = "keyst0re-Passw0rd-Do-Not-Log";

  @TempDir private static Path serverRoot;

  private static FtpTestServer server;

  private final IVariables variables = new Variables();

  @BeforeAll
  static void startServer() throws Exception {
    HopEnvironment.init();
    server = FtpTestServer.start(serverRoot, FtpSecurityMode.FTP);
    server.writeFile("greeting.txt", "hello");
  }

  @AfterAll
  static void stopServer() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  @Test
  @DisplayName("The get action logs no password, whether it succeeds or fails")
  void theGetActionKeepsQuiet(@TempDir Path targetDir) throws Exception {
    ActionFtp action = new ActionFtp("get");
    action.setServerName(FtpTestServer.HOST);
    action.setServerPort(Integer.toString(server.getPort()));
    action.setUserName(FtpTestServer.USER);
    action.setTargetDirectory(targetDir.toString());
    action.setAddResult(true);

    assertNothingLeaks(action, FtpTestServer.PASSWORD);
    assertNothingLeaks(action, PASSWORD);
  }

  @Test
  @DisplayName("The put action logs no password, whether it succeeds or fails")
  void thePutActionKeepsQuiet(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("payload.txt"), "up it goes");

    ActionFtpPut action = new ActionFtpPut("put");
    action.setServerName(FtpTestServer.HOST);
    action.setServerPort(Integer.toString(server.getPort()));
    action.setUserName(FtpTestServer.USER);
    action.setLocalDirectory(localDir.toString());

    assertNothingLeaks(action, FtpTestServer.PASSWORD);
    assertNothingLeaks(action, PASSWORD);
  }

  @Test
  @DisplayName("The delete action logs no password, whether it succeeds or fails")
  void theDeleteActionKeepsQuiet() throws Exception {
    ActionFtpDelete action = new ActionFtpDelete("delete");
    action.setProtocol(ActionFtpDelete.PROTOCOL_FTP);
    action.setServerName(FtpTestServer.HOST);
    action.setServerPort(Integer.toString(server.getPort()));
    action.setUserName(FtpTestServer.USER);
    action.setWildcard("nothing-matches-this");

    assertNothingLeaks(action, FtpTestServer.PASSWORD);
    assertNothingLeaks(action, PASSWORD);
  }

  @Test
  @DisplayName("Neither do the proxy and keystore passwords, on the path where they are used")
  void theOtherSecretsKeepQuietToo(@TempDir Path targetDir) throws Exception {
    ActionFtp action = new ActionFtp("get through everything");
    action.setServerName(FtpTestServer.HOST);
    action.setServerPort(Integer.toString(server.getPort()));
    action.setUserName(FtpTestServer.USER);
    action.setPassword(PASSWORD);
    action.setTargetDirectory(targetDir.toString());
    // An FTP proxy and a SOCKS proxy which don't answer: the failure message is written from
    // whatever the action knows, which is exactly when a secret slips out.
    action.setProxyHost("proxy.invalid");
    action.setProxyPort("8021");
    action.setProxyUsername("proxy-user");
    action.setProxyPassword(PROXY_PASSWORD);
    action.setSocksProxyHost("socks.invalid");
    action.setSocksProxyPort("1080");
    action.setSocksProxyUsername("socks-user");
    action.setSocksProxyPassword(SOCKS_PASSWORD);

    String log = runAndCaptureLog(action);

    assertNoSecretsIn(log, PASSWORD, PROXY_PASSWORD, SOCKS_PASSWORD);
  }

  @Test
  @DisplayName("A failing FTPS client certificate does not put the keystore password in the log")
  void theKeystorePasswordKeepsQuiet(@TempDir Path root) throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setName("secured");
    connection.setSecurityMode(FtpSecurityMode.FTPS_EXPLICIT);
    connection.setServerName(FtpTestServer.HOST);
    connection.setServerPort(Integer.toString(server.getPort()));
    connection.setUserName(FtpTestServer.USER);
    connection.setPassword(PASSWORD);
    connection.setClientCertificateFile(root.resolve("not-a-keystore.p12").toString());
    connection.setClientCertificatePassword(KEYSTORE_PASSWORD);

    String message = "";
    try {
      FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    } catch (Exception e) {
      message = messageChain(e);
    }

    assertFalse(message.isEmpty(), "this was supposed to fail");
    assertNoSecretsIn(message, PASSWORD, KEYSTORE_PASSWORD);
  }

  /**
   * The URI of a file behind a named connection is what turns up in the log of every transform
   * which touches that file, so it may never carry the credentials of the connection - not in any
   * of the four ways Commons VFS can be asked for it.
   */
  @Test
  @DisplayName("No form of the URI of a named connection carries its credentials")
  void theUriNeverCarriesTheCredentials() throws Exception {
    FtpConnection connection = new FtpConnection();
    connection.setName("prod");
    connection.setServerName("ftp.example.com");
    connection.setServerPort("2121");
    connection.setUserName("hop");
    connection.setPassword(PASSWORD);

    GenericFileName name =
        (GenericFileName)
            new FtpConnectionFileNameParser(variables, connection)
                .parseUri(vfsContext(), null, "prod:///inbox/customers.csv");

    assertNoSecretsIn(name.getURI(), PASSWORD);
    assertNoSecretsIn(name.getFriendlyURI(), PASSWORD);
    assertNoSecretsIn(name.getRootURI(), PASSWORD);
    assertNoSecretsIn(name.toString(), PASSWORD);
    assertNoSecretsIn(name.createName("/other.csv", name.getType()).getURI(), PASSWORD);

    // The provider still has to be able to log in, so the name does hold the credentials - it just
    // never hands them out in a URI.
    assertTrue(PASSWORD.equals(name.getPassword()), "the provider needs the password to connect");
  }

  @Test
  @DisplayName("The log says which server the action is talking to, resolved, not the variable")
  void theLogNamesTheServer(@TempDir Path targetDir) throws Exception {
    ActionFtp action = new ActionFtp("get");
    action.setVariable("FTP_HOST", FtpTestServer.HOST);
    action.setServerName("${FTP_HOST}");
    action.setServerPort(Integer.toString(server.getPort()));
    action.setUserName(FtpTestServer.USER);
    action.setPassword(FtpTestServer.PASSWORD);
    action.setTargetDirectory(targetDir.toString());

    String log = runAndCaptureLog(action);

    assertTrue(log.contains(FtpTestServer.HOST), "the log should name the host it reached");
    assertFalse(log.contains("${FTP_HOST}"), "the log should not show the unresolved variable");
  }

  @Test
  @DisplayName("With a named connection the log says which connection, not an empty server")
  void theLogNamesTheConnection(@TempDir Path targetDir) throws Exception {
    FtpConnection stored = new FtpConnection();
    stored.setName("the-named-one");
    stored.setServerName(FtpTestServer.HOST);
    stored.setServerPort(Integer.toString(server.getPort()));
    stored.setUserName(FtpTestServer.USER);
    stored.setPassword(PASSWORD);
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(FtpConnection.class).save(stored);

    ActionFtp action = new ActionFtp("get");
    action.setMetadataProvider(metadataProvider);
    action.setConnection("the-named-one");
    action.setTargetDirectory(targetDir.toString());

    String log = runAndCaptureLog(action);

    assertTrue(log.contains("the-named-one"), "the log should name the connection in use");
    assertNoSecretsIn(log, PASSWORD);
  }

  // --- helpers ------------------------------------------------------------------------------

  /** Runs the action with the given password and checks the log, on success and on failure. */
  private void assertNothingLeaks(ActionBase action, String password) throws Exception {
    setPassword(action, password);
    assertNoSecretsIn(runAndCaptureLog(action), password);
  }

  private static void setPassword(ActionBase action, String password) {
    if (action instanceof ActionFtp ftp) {
      ftp.setPassword(password);
    } else if (action instanceof ActionFtpPut put) {
      put.setPassword(password);
    } else if (action instanceof ActionFtpDelete delete) {
      delete.setPassword(password);
    }
  }

  /** Everything the action wrote to the log while it ran, at the most verbose level. */
  @SuppressWarnings("unchecked")
  private static String runAndCaptureLog(ActionBase action) throws Exception {
    IWorkflowEngine<WorkflowMeta> workflowEngine = mock(IWorkflowEngine.class);
    when(workflowEngine.isStopped()).thenReturn(false);
    when(workflowEngine.getLogLevel()).thenReturn(LogLevel.ROWLEVEL);
    when(workflowEngine.getContainerId()).thenReturn("credential-test");
    when(workflowEngine.getWorkflowName()).thenReturn("credential-test");
    action.setParentWorkflow(workflowEngine);
    action.setLogLevel(LogLevel.ROWLEVEL);

    String logChannelId = action.getLogChannel().getLogChannelId();
    action.execute(new Result(), 0);

    return HopLogStore.getAppender().getBuffer(logChannelId, true).toString();
  }

  private static void assertNoSecretsIn(String text, String... secrets) {
    for (String secret : secrets) {
      assertFalse(
          text.contains(secret),
          "a secret reached the log or the message:\n"
              + text.replace(secret, ">>> THE SECRET <<<"));
    }
  }

  /** The message of an exception and of everything that caused it. */
  private static String messageChain(Throwable throwable) {
    StringBuilder messages = new StringBuilder();
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    return messages.toString();
  }

  /** The parser only asks the context for the known schemes. */
  private static VfsComponentContext vfsContext() {
    FileSystemManager manager = mock(FileSystemManager.class);
    when(manager.getSchemes()).thenReturn(new String[] {"prod", "file", "ftp"});
    VfsComponentContext context = mock(VfsComponentContext.class);
    when(context.getFileSystemManager()).thenReturn(manager);
    return context;
  }
}
