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

package org.apache.hop.workflow.actions.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
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

/** The FTP get action against an embedded FTP server. */
class ActionFtpExecutionTest {

  @TempDir private static Path serverRoot;

  private static FtpTestServer server;

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
    // Directories and all: the tests move files into folders they create on the server.
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

  @Test
  @DisplayName("The files matching the wildcard are downloaded")
  void downloadsTheMatchingFiles(@TempDir Path targetDir) throws Exception {
    server.writeFile("wanted.txt", "the-payload");
    server.writeFile("ignored.csv", "not-this-one");

    ActionFtp action = configure(targetDir);
    action.setWildcard(".*\\.txt");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getNrFilesRetrieved());
    assertEquals("the-payload", Files.readString(targetDir.resolve("wanted.txt")));
    assertFalse(Files.exists(targetDir.resolve("ignored.csv")));
  }

  /**
   * A wildcard which matches exactly one file used to be treated as the "no files found" message
   * some ancient servers reply with, and the action failed on it.
   */
  @Test
  @DisplayName("A wildcard matching a single file downloads that file")
  void aSingleMatchIsNotMistakenForAnError(@TempDir Path targetDir) throws Exception {
    server.writeFile("test.txt", "only-one");

    ActionFtp action = configure(targetDir);
    action.setWildcard("test.*");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "a single matching file is a normal download, not a failure");
    assertEquals(1, result.getNrFilesRetrieved());
    assertEquals("only-one", Files.readString(targetDir.resolve("test.txt")));
  }

  @Test
  @DisplayName("Without a wildcard every file on the server is downloaded")
  void withoutAWildcardEverythingComesDown(@TempDir Path targetDir) throws Exception {
    server.writeFile("one.txt", "1");
    server.writeFile("two.txt", "2");

    Result result = configure(targetDir).execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(2, result.getNrFilesRetrieved());
  }

  @Test
  @DisplayName("\"Only get new files\" skips a file which is already in the target directory")
  void onlyNewFilesSkipsWhatIsAlreadyThere(@TempDir Path targetDir) throws Exception {
    server.writeFile("existing.txt", "the-server-copy");
    Files.writeString(targetDir.resolve("existing.txt"), "the-local-copy");

    ActionFtp action = configure(targetDir);
    action.setOnlyGettingNewFiles(true);
    action.setIfFileExistsOperation(ActionFtp.IfFileExistsOperation.SKIP);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrFilesRetrieved());
    assertEquals("the-local-copy", Files.readString(targetDir.resolve("existing.txt")));
  }

  @Test
  @DisplayName("A remote directory which isn't there is an error, not a download of the wrong one")
  void aMissingRemoteDirectoryIsAnError(@TempDir Path targetDir) throws Exception {
    server.writeFile("wrong-place.txt", "should stay put");

    ActionFtp action = configure(targetDir);
    action.setRemoteDirectory("/not-there");
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(0, result.getNrFilesRetrieved());
    assertFalse(Files.exists(targetDir.resolve("wrong-place.txt")));
  }

  @Test
  @DisplayName("A login the server refuses is an error, not a silent success")
  void refusedLoginIsAnError(@TempDir Path targetDir) throws Exception {
    server.writeFile("wanted.txt", "the-payload");

    ActionFtp action = configure(targetDir);
    action.setPassword("wrong-password");
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
  }

  @Test
  @DisplayName("Downloaded files land in the result when \"add to result\" is on")
  void downloadedFilesAreAddedToTheResult(@TempDir Path targetDir) throws Exception {
    server.writeFile("wanted.txt", "in-the-result");

    ActionFtp action = configure(targetDir);
    action.setAddResult(true);
    Result result = action.execute(new Result(), 0);

    assertEquals(1, result.getResultFiles().size());
    assertTrue(
        result.getResultFiles().keySet().iterator().next().endsWith("wanted.txt"),
        "the result should carry the file we just downloaded");
  }

  @Test
  @DisplayName("\"Remove\" deletes the file from the server once it is down")
  void removeDeletesTheRemoteFile(@TempDir Path targetDir) throws Exception {
    server.writeFile("transient.txt", "read once");

    ActionFtp action = configure(targetDir);
    action.setRemove(true);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("read once", Files.readString(targetDir.resolve("transient.txt")));
    assertNull(server.readFile("transient.txt"), "the file should be gone from the server");
  }

  @Test
  @DisplayName("\"Move\" puts the file in another folder on the server instead of deleting it")
  void moveRelocatesTheRemoteFile(@TempDir Path targetDir) throws Exception {
    Files.createDirectories(serverRoot.resolve("done"));
    server.writeFile("moved.txt", "move me");

    ActionFtp action = configure(targetDir);
    action.setMoveFiles(true);
    action.setMoveToDirectory("/done");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertNull(server.readFile("moved.txt"));
    assertEquals("move me", server.readFile("done/moved.txt"));
  }

  @Test
  @DisplayName("The move-to folder is created when the action is set up to create it")
  void theMoveToFolderIsCreatedOnDemand(@TempDir Path targetDir) throws Exception {
    server.writeFile("moved.txt", "move me");

    ActionFtp action = configure(targetDir);
    action.setMoveFiles(true);
    action.setMoveToDirectory("/created-on-demand");
    action.setCreateMoveFolder(true);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("move me", server.readFile("created-on-demand/moved.txt"));
  }

  @Test
  @DisplayName("A move-to folder which isn't there and may not be created stops the action")
  void aMissingMoveToFolderStopsTheAction(@TempDir Path targetDir) throws Exception {
    server.writeFile("stays.txt", "not moved");

    ActionFtp action = configure(targetDir);
    action.setMoveFiles(true);
    action.setMoveToDirectory("/not-there");
    action.setCreateMoveFolder(false);
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(0, result.getNrFilesRetrieved(), "nothing may be downloaded either");
    assertNotNull(server.readFile("stays.txt"));
  }

  @Test
  @DisplayName("Moving without saying where to is refused before anything is connected")
  void movingWithoutATargetFolderIsRefused(@TempDir Path targetDir) throws Exception {
    ActionFtp action = configure(targetDir);
    action.setMoveFiles(true);
    action.setMoveToDirectory("");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
  }

  @Test
  @DisplayName("\"Fail\" on an existing target file counts an error and leaves the file alone")
  void ifFileExistsFail(@TempDir Path targetDir) throws Exception {
    server.writeFile("existing.txt", "the-server-copy");
    Files.writeString(targetDir.resolve("existing.txt"), "the-local-copy");

    ActionFtp action = configure(targetDir);
    action.setOnlyGettingNewFiles(true);
    action.setIfFileExistsOperation(ActionFtp.IfFileExistsOperation.FAIL);
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    assertEquals("the-local-copy", Files.readString(targetDir.resolve("existing.txt")));
  }

  @Test
  @DisplayName("\"Give a unique name\" downloads next to the file which is already there")
  void ifFileExistsCreateUnique(@TempDir Path targetDir) throws Exception {
    server.writeFile("existing.txt", "the-server-copy");
    Files.writeString(targetDir.resolve("existing.txt"), "the-local-copy");

    ActionFtp action = configure(targetDir);
    action.setOnlyGettingNewFiles(true);
    action.setIfFileExistsOperation(ActionFtp.IfFileExistsOperation.CREATE_UNIQUE);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrFilesRetrieved());
    assertEquals(
        "the-local-copy",
        Files.readString(targetDir.resolve("existing.txt")),
        "the file which was already there must not be touched");
    try (var files = Files.list(targetDir)) {
      assertEquals(2, files.count(), "the download should sit next to it under another name");
    }
  }

  @Test
  @DisplayName("\"At least X files\" fails the action when fewer came down")
  void successWhenAtLeastXFiles(@TempDir Path targetDir) throws Exception {
    server.writeFile("one.txt", "1");

    ActionFtp action = configure(targetDir);
    action.setSuccessCondition(ActionFtp.SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED);
    action.setNrLimit("2");

    assertFalse(action.execute(new Result(), 0).isResult());

    server.writeFile("two.txt", "2");
    ActionFtp second = configure(targetDir);
    second.setSuccessCondition(ActionFtp.SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED);
    second.setNrLimit("2");

    assertTrue(second.execute(new Result(), 0).isResult());
  }

  @Test
  @DisplayName("\"Fewer errors than\" tolerates a failure the default condition would not")
  void successWhenFewerErrorsThan(@TempDir Path targetDir) throws Exception {
    server.writeFile("existing.txt", "the-server-copy");
    Files.writeString(targetDir.resolve("existing.txt"), "the-local-copy");

    ActionFtp action = configure(targetDir);
    action.setOnlyGettingNewFiles(true);
    action.setIfFileExistsOperation(ActionFtp.IfFileExistsOperation.FAIL);
    action.setSuccessCondition(ActionFtp.SUCCESS_IF_ERRORS_LESS);
    action.setNrLimit("5");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "one error is fewer than the five allowed");
    assertEquals(1, result.getNrErrors());
  }

  @Test
  @DisplayName("A named FTP connection replaces the server settings on the action")
  void aNamedConnectionIsUsed(@TempDir Path targetDir) throws Exception {
    server.writeFile("wanted.txt", "through-a-connection");

    FtpConnection stored = new FtpConnection();
    stored.setName("prod");
    stored.setServerName(FtpTestServer.HOST);
    stored.setServerPort(Integer.toString(server.getPort()));
    stored.setUserName(FtpTestServer.USER);
    stored.setPassword(FtpTestServer.PASSWORD);
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(FtpConnection.class).save(stored);

    ActionFtp action = configure(targetDir);
    action.setMetadataProvider(metadataProvider);
    action.setConnection("prod");
    // Nonsense inline settings: picking them up instead of the connection has to fail the test.
    action.setServerName("nowhere.invalid");
    action.setServerPort("1");
    action.setUserName("nobody");
    action.setPassword("wrong");

    Result result = action.execute(new Result(), 0);

    assertTrue(action.isUsingConnection());
    assertTrue(result.isResult());
    assertEquals("through-a-connection", Files.readString(targetDir.resolve("wanted.txt")));
  }

  @Test
  @DisplayName("A connection name which isn't in the metadata is an error")
  void anUnknownConnectionIsAnError(@TempDir Path targetDir) throws Exception {
    ActionFtp action = configure(targetDir);
    action.setMetadataProvider(new MemoryMetadataProvider());
    action.setConnection("nowhere");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
  }

  @Test
  @DisplayName("Directories on the server are skipped, only files come down")
  void directoriesAreSkipped(@TempDir Path targetDir) throws Exception {
    Files.createDirectories(serverRoot.resolve("a-folder"));
    server.writeFile("a-file.txt", "just me");

    Result result = configure(targetDir).execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrFilesRetrieved());
    assertFalse(Files.exists(targetDir.resolve("a-folder")));
  }

  @Test
  @DisplayName("A workflow which is stopped does not carry on downloading")
  void aStoppedWorkflowStops(@TempDir Path targetDir) throws Exception {
    server.writeFile("one.txt", "1");
    server.writeFile("two.txt", "2");

    ActionFtp action = configure(targetDir);
    action.setParentWorkflow(workflowEngine(true));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(0, result.getNrFilesRetrieved());
  }

  @Test
  @DisplayName("Downloading into a directory which isn't there is an error, not a lost file")
  void aMissingTargetDirectoryIsAnError(@TempDir Path targetDir) throws Exception {
    server.writeFile("wanted.txt", "nowhere to land");

    ActionFtp action = configure(targetDir.resolve("no").resolve("such").resolve("dir"));
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
  }

  @Test
  @DisplayName("Reading from a subdirectory of the server")
  void downloadsFromARemoteDirectory(@TempDir Path targetDir) throws Exception {
    Files.createDirectories(serverRoot.resolve("outbox"));
    server.writeFile("outbox/wanted.txt", "from the outbox");
    server.writeFile("ignored.txt", "not this one");

    ActionFtp action = configure(targetDir);
    action.setRemoteDirectory("/outbox");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrFilesRetrieved());
    assertEquals("from the outbox", Files.readString(targetDir.resolve("wanted.txt")));
  }

  @Test
  @DisplayName("A unique name is found for a file which has no extension either")
  void createUniqueForAFileWithoutAnExtension(@TempDir Path targetDir) throws Exception {
    server.writeFile("README", "the-server-copy");
    Files.writeString(targetDir.resolve("README"), "the-local-copy");

    ActionFtp action = configure(targetDir);
    action.setOnlyGettingNewFiles(true);
    action.setIfFileExistsOperation(ActionFtp.IfFileExistsOperation.CREATE_UNIQUE);
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("the-local-copy", Files.readString(targetDir.resolve("README")));
    try (var files = Files.list(targetDir)) {
      assertEquals(2, files.count());
    }
  }

  @SuppressWarnings("unchecked")
  private static IWorkflowEngine<WorkflowMeta> workflowEngine(boolean stopped) {
    IWorkflowEngine<WorkflowMeta> workflowEngine = mock(IWorkflowEngine.class);
    when(workflowEngine.isStopped()).thenReturn(stopped);
    when(workflowEngine.getLogLevel()).thenReturn(LogLevel.DEBUG);
    when(workflowEngine.getContainerId()).thenReturn("test-container");
    return workflowEngine;
  }

  private ActionFtp configure(Path targetDir) {
    ActionFtp action = new ActionFtp("FTP get");
    action.setServerName(FtpTestServer.HOST);
    action.setServerPort(Integer.toString(server.getPort()));
    action.setUserName(FtpTestServer.USER);
    action.setPassword(FtpTestServer.PASSWORD);
    action.setTargetDirectory(targetDir.toString());
    action.setBinaryMode(true);
    action.setAddResult(false);
    action.setSuccessCondition(ActionFtp.SUCCESS_IF_NO_ERRORS);
    action.setParentWorkflow(workflowEngine(false));
    return action;
  }
}
