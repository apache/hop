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

package org.apache.hop.workflow.actions.addresultfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * The "include subfolders" option has to control folder traversal as well, otherwise a sub-folder
 * the user has no access to breaks a run that was never meant to look inside it. See
 * https://github.com/apache/hop/issues/7952
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class ActionAddResultFilenamesSubfolderTest {

  @TempDir Path folder;

  private ActionAddResultFilenames action;
  private Path unreadableFolder;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionAddResultFilenames();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);
    action.setArgFromPrevious(false);
    action.setArguments(List.of(new Argument(folder.toString(), null)));
  }

  @AfterEach
  void restorePermissions() throws IOException {
    if (unreadableFolder != null) {
      Files.setPosixFilePermissions(unreadableFolder, PosixFilePermissions.fromString("rwx------"));
    }
  }

  @Test
  void unreadableSubfolderIsNotTraversedWhenSubfoldersAreExcluded() throws Exception {
    Files.createFile(folder.resolve("report.csv"));
    unreadableFolder = createUnreadableFolder();
    action.setIncludeSubFolders(false);

    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors(), "an excluded sub-folder must not cause an error");
    assertTrue(result.isResult());
    assertEquals(1, result.getResultFiles().size(), "only the base folder file is expected");
    assertTrue(
        result.getResultFiles().keySet().stream().anyMatch(k -> k.endsWith("report.csv")),
        "the base folder file must be added to the result files");
  }

  @Test
  void subfoldersAreTraversedWhenIncluded() throws Exception {
    Files.createFile(folder.resolve("report.csv"));
    Path subFolder = Files.createDirectory(folder.resolve("archive"));
    Files.createFile(subFolder.resolve("nested.csv"));

    action.setIncludeSubFolders(false);
    Result result = action.execute(new Result(), 0);
    assertEquals(1, result.getResultFiles().size(), "the nested file must be skipped");

    action.setIncludeSubFolders(true);
    result = action.execute(new Result(), 0);
    assertEquals(2, result.getResultFiles().size(), "the nested file must be picked up");
  }

  private Path createUnreadableFolder() throws IOException {
    Path unreadable = Files.createDirectory(folder.resolve("no-access"));
    Files.createFile(unreadable.resolve("hidden.csv"));
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));
    assumeTrue(
        unreadable.toFile().list() == null,
        "folder is still readable, the test cannot run as this user (root?)");
    return unreadable;
  }
}
