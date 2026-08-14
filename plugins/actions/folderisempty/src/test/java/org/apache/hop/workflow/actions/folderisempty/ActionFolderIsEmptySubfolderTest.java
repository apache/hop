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

package org.apache.hop.workflow.actions.folderisempty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
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
 * the user has no access to breaks a check that was never meant to look inside it. See
 * https://github.com/apache/hop/issues/7952
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class ActionFolderIsEmptySubfolderTest {

  @TempDir Path folder;

  private ActionFolderIsEmpty action;
  private Path unreadableFolder;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionFolderIsEmpty();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);
    action.setFolderName(folder.toString());
  }

  @AfterEach
  void restorePermissions() throws IOException {
    if (unreadableFolder != null) {
      Files.setPosixFilePermissions(unreadableFolder, PosixFilePermissions.fromString("rwx------"));
    }
  }

  @Test
  void unreadableSubfolderIsNotTraversedWhenSubfoldersAreExcluded() throws Exception {
    unreadableFolder = createUnreadableFolder("no-access");
    action.setIncludeSubFolders(false);

    Result result = action.execute(new Result(), 0);

    assertEquals(0, result.getNrErrors(), "an excluded sub-folder must not cause an error");
    assertTrue(result.isResult(), "the folder holds no files of its own, so it is empty");
  }

  @Test
  void unreadableSubfolderIsStillReportedWhenSubfoldersAreIncluded() throws Exception {
    unreadableFolder = createUnreadableFolder("no-access");
    action.setIncludeSubFolders(true);

    Result result = action.execute(new Result(), 0);

    assertEquals(1, result.getNrErrors(), "an included sub-folder that cannot be read is an error");
  }

  @Test
  void subfoldersAreTraversedWhenIncluded() throws Exception {
    Path subFolder = Files.createDirectory(folder.resolve("archive"));
    Files.createFile(subFolder.resolve("nested.txt"));

    action.setIncludeSubFolders(false);
    assertTrue(
        action.execute(new Result(), 0).isResult(),
        "only the base folder counts, so the folder is empty");

    action.setIncludeSubFolders(true);
    assertFalse(
        action.execute(new Result(), 0).isResult(),
        "the nested file must be seen when subfolders are included");
  }

  private Path createUnreadableFolder(String name) throws IOException {
    Path unreadable = Files.createDirectory(folder.resolve(name));
    Files.createFile(unreadable.resolve("hidden.txt"));
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));
    assumeTrue(
        unreadable.toFile().list() == null,
        "folder is still readable, the test cannot run as this user (root?)");
    return unreadable;
  }
}
