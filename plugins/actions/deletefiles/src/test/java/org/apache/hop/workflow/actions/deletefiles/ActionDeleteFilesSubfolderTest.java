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

package org.apache.hop.workflow.actions.deletefiles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/** Verifies that the "include subfolders" option also controls folder traversal. */
class ActionDeleteFilesSubfolderTest {

  private static final String CSV_MASK = ".*\\.csv";

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopLogStore.init();
    HopEnvironment.init();
  }

  private ActionDeleteFiles createAction(boolean includeSubfolders) {
    ActionDeleteFiles action = new ActionDeleteFiles();
    IWorkflowEngine<WorkflowMeta> parentWorkflow = mock(Workflow.class);
    doReturn(false).when(parentWorkflow).isStopped();
    doReturn(LogLevel.BASIC).when(parentWorkflow).getLogLevel();
    action.setParentWorkflow(parentWorkflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setIncludeSubfolders(includeSubfolders);
    return action;
  }

  /** An inaccessible subfolder must not break a delete that was not asked to recurse. */
  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void inaccessibleSubfolderIsNotTraversedWhenSubfoldersAreExcluded(@TempDir Path folder)
      throws Exception {
    Path matching = Files.createFile(folder.resolve("cams_20260814.csv"));
    Path notMatching = Files.createFile(folder.resolve("readme.txt"));
    Path unreadable = Files.createDirectory(folder.resolve("mcbp-full"));
    Files.createFile(unreadable.resolve("nested.csv"));
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));

    ActionDeleteFiles action = createAction(false);
    try {
      assertTrue(action.processFile(folder.toString(), CSV_MASK, action.getParentWorkflow()));

      assertFalse(Files.exists(matching), "matching file in the base folder was not deleted");
      assertTrue(Files.exists(notMatching), "non-matching file was deleted");
    } finally {
      Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("rwx------"));
      deleteRecursively(unreadable);
    }
  }

  /** Recursion still happens when subfolders are included. */
  @Test
  void subfoldersAreTraversedWhenIncluded(@TempDir Path folder) throws Exception {
    Path matching = Files.createFile(folder.resolve("cams_20260814.csv"));
    Path subFolder = Files.createDirectory(folder.resolve("archive"));
    Path nestedMatching = Files.createFile(subFolder.resolve("nested.csv"));
    Path nestedNotMatching = Files.createFile(subFolder.resolve("nested.txt"));

    ActionDeleteFiles action = createAction(true);

    assertTrue(action.processFile(folder.toString(), CSV_MASK, action.getParentWorkflow()));

    assertFalse(Files.exists(matching), "matching file in the base folder was not deleted");
    assertFalse(Files.exists(nestedMatching), "matching file in the subfolder was not deleted");
    assertTrue(Files.exists(nestedNotMatching), "non-matching file was deleted");
  }

  private void deleteRecursively(Path path) throws IOException {
    try (Stream<Path> paths = Files.walk(path)) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
    }
  }
}
