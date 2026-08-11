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

package org.apache.hop.workflow.actions.ftpput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.vfs.ftp.FtpSecurityMode;
import org.apache.hop.vfs.ftp.FtpTestServer;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The FTP put action against an embedded FTP server, so that what the action reports and what
 * actually landed on the server are checked against each other rather than against a mock which
 * agrees with whatever the action does.
 */
class ActionFtpPutExecutionTest {

  @TempDir private static Path serverRoot;

  private static FtpTestServer server;

  private ActionFtpPut action;

  @BeforeAll
  static void startServer() throws Exception {
    HopEnvironment.init();
    server = FtpTestServer.start(serverRoot, FtpSecurityMode.FTP);
  }

  @AfterAll
  static void stopServer() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  @BeforeEach
  void emptyTheServer() throws Exception {
    try (var entries = Files.list(serverRoot)) {
      for (Path entry : entries.toList()) {
        try (var tree = Files.walk(entry)) {
          for (Path path : tree.sorted(Comparator.reverseOrder()).toList()) {
            Files.deleteIfExists(path);
          }
        }
      }
    }
  }

  @BeforeEach
  void setUp() throws HopException {
    action = new ActionFtpPut("Test Put a file with FTP");
    action.setUserName("user");
    action.setPassword("password");
    action.setServerName("127.0.0.1");
    action.setName("Test name");
    action.setRemoteDirectory("/home/user");
    action.setLocalDirectory("/tmp");

    HopEnvironment.init();
  }

  @Test
  void testEmptyActionFtpPut() {
    assertEquals("", action.getDescription());

    ActionFtpPut ftpPut = new ActionFtpPut();
    assertTrue(ftpPut.getDescription().isBlank());
  }

  @Test
  void testClone() {
    Object cloned = action.clone();
    assertNotSame(cloned, action);
  }

  @Test
  void testIsEvaluation() {
    assertTrue(action.isEvaluation());
  }

  @Test
  void testGetResourceDependencies() {
    IVariables variables = mock(IVariables.class);
    WorkflowMeta meta = mock(WorkflowMeta.class);

    // 127.0.0.1 server
    List<ResourceReference> references = action.getResourceDependencies(variables, meta);
    assertNotNull(references);
    assertEquals(1, references.size());

    // null server
    action.setServerName(null);
    references = action.getResourceDependencies(variables, meta);
    assertNotNull(references);
    assertTrue(references.isEmpty());
  }

  @Test
  void testCheck() {
    List<ICheckResult> remarks = new ArrayList<>();
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    IVariables variables = mock(IVariables.class);
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);

    // server is null
    action.setServerName(Const.EMPTY_STRING);
    action.check(remarks, workflowMeta, variables, provider);

    boolean hasError =
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR);
    assertTrue(hasError);
  }

  @Test
  @DisplayName("A successful upload reports success and leaves the file on the server")
  void executeUploadsTheLocalFiles(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("payload.txt"), "put-me");

    Result result = uploadFrom(localDir, false);

    assertTrue(result.isResult(), "the upload succeeded, so the action should report success");
    assertEquals(0, result.getNrErrors());
    assertEquals("put-me", server.readFile("payload.txt"));
  }

  @Test
  @DisplayName("A login the server refuses is an error, not a silent success")
  void executeReportsFailureWhenTheLoginIsRefused(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("payload.txt"), "never-arrives");

    ActionFtpPut put = configure(localDir, false);
    put.setPassword("wrong-password");
    Result result = put.execute(new Result(), 0);

    assertFalse(result.isResult(), "a refused login must not report success");
    assertTrue(result.getNrErrors() > 0);
  }

  @Test
  @DisplayName("\"Only put new files\" leaves a file which is already on the server alone")
  void onlyPuttingNewFilesDoesNotOverwrite(@TempDir Path localDir) throws Exception {
    server.writeFile("existing.txt", "the-server-copy");
    Files.writeString(localDir.resolve("existing.txt"), "the-local-copy");

    Result result = uploadFrom(localDir, true);

    assertTrue(result.isResult());
    assertEquals(
        "the-server-copy",
        server.readFile("existing.txt"),
        "only new files means the file on the server is left as it is");
  }

  @Test
  @DisplayName("Without \"only put new files\" an existing file is replaced")
  void withoutOnlyNewFilesTheFileIsReplaced(@TempDir Path localDir) throws Exception {
    server.writeFile("replaced.txt", "the-server-copy");
    Files.writeString(localDir.resolve("replaced.txt"), "the-local-copy");

    Result result = uploadFrom(localDir, false);

    assertTrue(result.isResult());
    assertEquals("the-local-copy", server.readFile("replaced.txt"));
  }

  @Test
  @DisplayName("Only the files matching the wildcard go up")
  void theWildcardPicksTheFiles(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("wanted.txt"), "yes");
    Files.writeString(localDir.resolve("ignored.csv"), "no");

    ActionFtpPut put = configure(localDir, false);
    put.setWildcard(".*\\.txt");
    Result result = put.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("yes", server.readFile("wanted.txt"));
    assertNull(server.readFile("ignored.csv"));
  }

  @Test
  @DisplayName("Subdirectories of the source folder are left where they are")
  void subdirectoriesAreNotUploaded(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("top.txt"), "up");
    Files.createDirectories(localDir.resolve("nested"));
    Files.writeString(localDir.resolve("nested/deep.txt"), "stays");

    Result result = uploadFrom(localDir, false);

    assertTrue(result.isResult());
    assertEquals("up", server.readFile("top.txt"));
    assertNull(server.readFile("deep.txt"));
  }

  @Test
  @DisplayName("\"Remove\" deletes the source file once it is up")
  void removeDeletesTheSourceFile(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("moved.txt"), "gone from here");

    ActionFtpPut put = configure(localDir, false);
    put.setRemove(true);
    Result result = put.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("gone from here", server.readFile("moved.txt"));
    assertFalse(Files.exists(localDir.resolve("moved.txt")));
  }

  @Test
  @DisplayName("Uploading into a subdirectory of the server")
  void uploadsIntoARemoteDirectory(@TempDir Path localDir) throws Exception {
    Files.createDirectories(serverRoot.resolve("inbox"));
    Files.writeString(localDir.resolve("payload.txt"), "in the inbox");

    ActionFtpPut put = configure(localDir, false);
    put.setRemoteDirectory("/inbox");
    Result result = put.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("in the inbox", server.readFile("inbox/payload.txt"));
  }

  @Test
  @DisplayName("A remote directory which isn't there is an error, not an upload somewhere else")
  void aMissingRemoteDirectoryIsAnError(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("payload.txt"), "nowhere to go");

    ActionFtpPut put = configure(localDir, false);
    put.setRemoteDirectory("/not-there");
    Result result = put.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
    assertNull(server.readFile("payload.txt"), "it must not land in the login directory instead");
  }

  @Test
  @DisplayName("A source directory which isn't there is an error")
  void aMissingSourceDirectoryIsAnError(@TempDir Path localDir) throws Exception {
    ActionFtpPut put = configure(localDir.resolve("not-there"), false);

    Result result = put.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
  }

  @Test
  @DisplayName("An empty source directory is a success with nothing uploaded")
  void anEmptySourceDirectoryIsFine(@TempDir Path localDir) throws Exception {
    Result result = uploadFrom(localDir, false);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  @DisplayName("A named FTP connection replaces the server settings on the action")
  void aNamedConnectionIsUsed(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("payload.txt"), "through-a-connection");

    FtpConnection stored = new FtpConnection();
    stored.setName("prod");
    stored.setServerName(FtpTestServer.HOST);
    stored.setServerPort(Integer.toString(server.getPort()));
    stored.setUserName(FtpTestServer.USER);
    stored.setPassword(FtpTestServer.PASSWORD);
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(FtpConnection.class).save(stored);

    ActionFtpPut put = configure(localDir, false);
    put.setMetadataProvider(metadataProvider);
    put.setConnection("prod");
    put.setServerName("nowhere.invalid");
    put.setServerPort("1");
    put.setUserName("nobody");
    put.setPassword("wrong");

    Result result = put.execute(new Result(), 0);

    assertTrue(put.isUsingConnection());
    assertTrue(result.isResult());
    assertEquals("through-a-connection", server.readFile("payload.txt"));
  }

  @Test
  @DisplayName("The timeout of the action is handed to the connection as milliseconds")
  void theTimeoutBecomesAConnectTimeout() {
    ActionFtpPut put = new ActionFtpPut();

    assertNull(put.getConnectTimeout());

    put.setTimeout(7000);
    assertEquals("7000", put.getConnectTimeout());
    assertEquals("FTP Put", new ActionFtpPut().getFtpConnectionName());
  }

  @Test
  @DisplayName("An upload the server refuses is reported as a failure")
  void aRefusedUploadIsAFailure(@TempDir Path localDir) throws Exception {
    Files.writeString(localDir.resolve("payload.txt"), "not allowed");

    ActionFtpPut put = configure(localDir, false);
    put.setUserName(FtpTestServer.READ_ONLY_USER);
    put.setPassword(FtpTestServer.READ_ONLY_PASSWORD);

    Result result = put.execute(new Result(), 0);

    assertFalse(result.isResult(), "the server refused the file, so this is not a success");
    assertTrue(result.getNrErrors() > 0);
    assertNull(server.readFile("payload.txt"));
  }

  @Test
  @DisplayName("A source directory which is really a file is an error")
  void aSourceWhichIsAFileIsAnError(@TempDir Path localDir) throws Exception {
    Path notADirectory = Files.writeString(localDir.resolve("a-file.txt"), "not a directory");

    ActionFtpPut put = configure(notADirectory, false);
    Result result = put.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
  }

  @Test
  @DisplayName("An empty source directory setting is an error, not the working directory")
  void anEmptySourceDirectoryIsAnError() throws Exception {
    ActionFtpPut put = configure(Path.of("."), false);
    put.setLocalDirectory("");

    Result result = put.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
  }

  private Result uploadFrom(Path localDir, boolean onlyNew) throws Exception {
    return configure(localDir, onlyNew).execute(new Result(), 0);
  }

  @SuppressWarnings("unchecked")
  private ActionFtpPut configure(Path localDir, boolean onlyNew) {
    ActionFtpPut put = new ActionFtpPut("FTP put");
    put.setServerName(FtpTestServer.HOST);
    put.setServerPort(Integer.toString(server.getPort()));
    put.setUserName(FtpTestServer.USER);
    put.setPassword(FtpTestServer.PASSWORD);
    put.setLocalDirectory(localDir.toString());
    put.setBinaryMode(true);
    put.setOnlyPuttingNewFiles(onlyNew);

    IWorkflowEngine<WorkflowMeta> workflowEngine = mock(IWorkflowEngine.class);
    when(workflowEngine.isStopped()).thenReturn(false);
    // Debug: every log line in the action runs, so a bad message key fails a test.
    when(workflowEngine.getLogLevel()).thenReturn(LogLevel.DEBUG);
    when(workflowEngine.getContainerId()).thenReturn("test-container");
    put.setParentWorkflow(workflowEngine);
    return put;
  }
}
