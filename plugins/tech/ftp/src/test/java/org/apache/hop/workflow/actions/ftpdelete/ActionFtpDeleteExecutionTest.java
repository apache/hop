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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.vfs.ftp.FtpSecurityMode;
import org.apache.hop.vfs.ftp.FtpTestServer;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The FTP delete action against an embedded FTP server. Every one of these covers a case where the
 * action used to report something other than what happened on the server.
 */
class ActionFtpDeleteExecutionTest {

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
  @DisplayName("Files matching the wildcard are deleted and the others are left alone")
  void deletesTheMatchingFiles() throws Exception {
    server.writeFile("report.txt", "gone");
    server.writeFile("keep.csv", "stays");

    ActionFtpDelete action = configure();
    action.setWildcard(".*\\.txt");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getNrFilesRetrieved());
    assertNull(server.readFile("report.txt"));
    assertNotNull(server.readFile("keep.csv"));
  }

  @Test
  @DisplayName("Without a wildcard the action fails instead of quietly deleting nothing")
  void withoutAWildcardItFails() throws Exception {
    server.writeFile("report.txt", "stays");

    ActionFtpDelete action = configure();
    action.setWildcard("");
    Result result = action.execute(new Result(), 0);

    assertFalse(
        result.isResult(),
        "no wildcard and no incoming rows means the action can't know what to delete");
    assertTrue(result.getNrErrors() > 0);
    assertNotNull(server.readFile("report.txt"), "nothing may be deleted on that error");
  }

  @Test
  @DisplayName("An empty directory is a success, not a failure")
  void anEmptyDirectoryIsFine() throws Exception {
    ActionFtpDelete action = configure();
    action.setWildcard(".*");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  @DisplayName("With \"get file names from the previous action\" only the named files go")
  void copyPreviousDeletesOnlyTheNamedFiles() throws Exception {
    server.writeFile("named.txt", "gone");
    server.writeFile("other.txt", "stays");

    ActionFtpDelete action = configure();
    action.setCopyPrevious(true);

    Result previous = new Result();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("filename"));
    previous.setRows(List.of(new RowMetaAndData(rowMeta, "named.txt")));

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrFilesRetrieved());
    assertNull(server.readFile("named.txt"));
    assertNotNull(server.readFile("other.txt"));
  }

  @Test
  @DisplayName("A login the server refuses is an error, not a silent success")
  void refusedLoginIsAnError() throws Exception {
    server.writeFile("report.txt", "stays");

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setPassword("wrong-password");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() > 0);
    assertNotNull(server.readFile("report.txt"));
  }

  @Test
  @DisplayName("Something which cannot be deleted is counted as an error")
  void whatCannotBeDeletedIsAnError() throws Exception {
    // A directory: the delete command of FTP is for files, so the server refuses it.
    Files.createDirectories(serverRoot.resolve("a-folder"));

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "a refused delete is not a success");
    assertTrue(result.getNrErrors() > 0);
    assertTrue(Files.exists(serverRoot.resolve("a-folder")));
  }

  @Test
  @DisplayName("\"Fewer errors than\" tolerates a refused delete the default condition would not")
  void successWhenFewerErrorsThan() throws Exception {
    Files.createDirectories(serverRoot.resolve("a-folder"));

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setSuccessCondition(ActionFtpDelete.SUCCESS_IF_ERRORS_LESS);
    action.setNrLimitSuccess("5");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "one error is fewer than the five allowed");
  }

  @Test
  @DisplayName("\"At least X files\" fails when fewer than that were deleted")
  void successWhenAtLeastXFiles() throws Exception {
    server.writeFile("one.txt", "1");

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setSuccessCondition(ActionFtpDelete.SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED);
    action.setNrLimitSuccess("2");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrFilesRetrieved());
  }

  @Test
  @DisplayName("A workflow which is stopped does not carry on deleting")
  void aStoppedWorkflowStops() throws Exception {
    server.writeFile("one.txt", "1");

    ActionFtpDelete action = configure();
    action.setWildcard(".*");
    action.setParentWorkflow(workflowEngine(true));

    action.execute(new Result(), 0);

    assertNotNull(server.readFile("one.txt"), "nothing may be deleted after a stop");
  }

  @Test
  @DisplayName("Deleting in a subdirectory of the server")
  void deletesInARemoteDirectory() throws Exception {
    Files.createDirectories(serverRoot.resolve("inbox"));
    server.writeFile("inbox/gone.txt", "gone");
    server.writeFile("stays.txt", "stays");

    ActionFtpDelete action = configure();
    action.setRemoteDirectory("/inbox");
    action.setWildcard(".*");
    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertNull(server.readFile("inbox/gone.txt"));
    assertNotNull(server.readFile("stays.txt"));
  }

  @Test
  @DisplayName("A remote directory which isn't there is an error, not a delete somewhere else")
  void aMissingRemoteDirectoryIsAnError() throws Exception {
    server.writeFile("stays.txt", "stays");

    ActionFtpDelete action = configure();
    action.setRemoteDirectory("/not-there");
    action.setWildcard(".*");
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertNotNull(server.readFile("stays.txt"));
  }

  @Test
  @DisplayName("\"Get file names from the previous action\" with no rows does nothing at all")
  void copyPreviousWithoutRowsIsANoOp() throws Exception {
    server.writeFile("stays.txt", "stays");

    ActionFtpDelete action = configure();
    action.setCopyPrevious(true);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "nothing to do is not a failure");
    assertNotNull(server.readFile("stays.txt"));
  }

  @SuppressWarnings("unchecked")
  private static IWorkflowEngine<WorkflowMeta> workflowEngine(boolean stopped) {
    IWorkflowEngine<WorkflowMeta> workflowEngine = mock(IWorkflowEngine.class);
    when(workflowEngine.isStopped()).thenReturn(stopped);
    when(workflowEngine.getLogLevel()).thenReturn(LogLevel.DEBUG);
    when(workflowEngine.getContainerId()).thenReturn("test-container");
    return workflowEngine;
  }

  private ActionFtpDelete configure() {
    ActionFtpDelete action = new ActionFtpDelete("FTP delete");
    action.setProtocol(ActionFtpDelete.PROTOCOL_FTP);
    action.setServerName(FtpTestServer.HOST);
    action.setServerPort(Integer.toString(server.getPort()));
    action.setUserName(FtpTestServer.USER);
    action.setPassword(FtpTestServer.PASSWORD);
    action.setParentWorkflow(workflowEngine(false));
    return action;
  }
}
