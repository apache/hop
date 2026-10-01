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

package org.apache.hop.workflow.actions.folderscompare;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
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
 * the user has no access to breaks a comparison that was never meant to look inside it. See
 * https://github.com/apache/hop/issues/7952
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class ActionFoldersCompareSubfolderTest {

  @TempDir Path testFolder;

  private ActionFoldersCompare action;
  private Path folder1;
  private Path folder2;
  private final List<Path> unreadableFolders = new ArrayList<>();

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws IOException {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionFoldersCompare();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);

    folder1 = Files.createDirectories(testFolder.resolve("left"));
    folder2 = Files.createDirectories(testFolder.resolve("right"));
    action.setFilename1(folder1.toString());
    action.setFilename2(folder2.toString());
    action.setCompareOnly("all");
  }

  @AfterEach
  void restorePermissions() throws IOException {
    for (Path unreadable : unreadableFolders) {
      Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("rwx------"));
    }
  }

  @Test
  void unreadableSubfolderIsNotTraversedWhenSubfoldersAreExcluded() throws Exception {
    Files.writeString(folder1.resolve("report.csv"), "same", StandardCharsets.UTF_8);
    Files.writeString(folder2.resolve("report.csv"), "same", StandardCharsets.UTF_8);
    createUnreadableFolder(folder1);
    createUnreadableFolder(folder2);
    action.setIncludeSubFolders(false);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "identical base folders must compare as equal");
  }

  @Test
  void subfoldersAreTraversedWhenIncluded() throws Exception {
    Files.writeString(folder1.resolve("report.csv"), "same", StandardCharsets.UTF_8);
    Files.writeString(folder2.resolve("report.csv"), "same", StandardCharsets.UTF_8);
    // only the left side has a nested file, so the folders differ below the base level
    Path subFolder1 = Files.createDirectory(folder1.resolve("archive"));
    Files.writeString(subFolder1.resolve("nested.csv"), "left", StandardCharsets.UTF_8);
    Files.createDirectory(folder2.resolve("archive"));

    action.setIncludeSubFolders(false);
    assertTrue(
        action.execute(new Result(), 0).isResult(),
        "the nested difference must be ignored when subfolders are excluded");

    action.setIncludeSubFolders(true);
    assertFalse(
        action.execute(new Result(), 0).isResult(),
        "the nested difference must be seen when subfolders are included");
  }

  private void createUnreadableFolder(Path parent) throws IOException {
    Path unreadable = Files.createDirectory(parent.resolve("no-access"));
    Files.createFile(unreadable.resolve("hidden.csv"));
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));
    unreadableFolders.add(unreadable);
    assumeTrue(
        unreadable.toFile().list() == null,
        "folder is still readable, the test cannot run as this user (root?)");
  }
}
