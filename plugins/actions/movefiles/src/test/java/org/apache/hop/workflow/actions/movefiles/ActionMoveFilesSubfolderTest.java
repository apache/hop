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

package org.apache.hop.workflow.actions.movefiles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * The "include subfolders" option has to control folder traversal as well, otherwise a sub-folder
 * the user has no access to breaks a move that was never meant to look inside it. See
 * https://github.com/apache/hop/issues/7952
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class ActionMoveFilesSubfolderTest {

  @TempDir Path testFolder;

  private ActionMoveFiles action;
  private Path sourceFolder;
  private Path destinationFolder;
  private Path unreadableFolder;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws IOException {
    action = MoveFilesActionHelper.defaultAction();
    sourceFolder = Files.createDirectories(testFolder.resolve("source"));
    destinationFolder = Files.createDirectories(testFolder.resolve("destination"));

    ActionMoveFiles.FileToMove fileToMove = new ActionMoveFiles.FileToMove();
    fileToMove.setSourceFileFolder(sourceFolder.toString());
    fileToMove.setDestinationFileFolder(destinationFolder.toString());
    action.getFilesToMove().add(fileToMove);
    action.setDestinationIsAFile(false);
  }

  @AfterEach
  void restorePermissions() throws IOException {
    if (unreadableFolder != null) {
      Files.setPosixFilePermissions(unreadableFolder, PosixFilePermissions.fromString("rwx------"));
    }
  }

  @Test
  void unreadableSubfolderIsNotTraversedWhenSubfoldersAreExcluded() throws Exception {
    Files.createFile(sourceFolder.resolve("report.csv"));
    unreadableFolder = createUnreadableFolder();
    action.setIncludeSubfolders(false);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "an excluded sub-folder must not fail the move");
    assertTrue(
        Files.exists(destinationFolder.resolve("report.csv")),
        "the base folder file must still be moved");
  }

  @Test
  void subfoldersAreTraversedWhenIncluded() throws Exception {
    Files.createFile(sourceFolder.resolve("report.csv"));
    Path subFolder = Files.createDirectory(sourceFolder.resolve("archive"));
    Files.createFile(subFolder.resolve("nested.csv"));

    action.setIncludeSubfolders(false);
    assertTrue(action.execute(new Result(), 0).isResult());
    assertTrue(Files.exists(destinationFolder.resolve("report.csv")), "report.csv must be moved");
    assertTrue(Files.exists(subFolder.resolve("nested.csv")), "the nested file must be left alone");

    action.setIncludeSubfolders(true);
    assertTrue(action.execute(new Result(), 0).isResult());
    assertFalse(
        Files.exists(subFolder.resolve("nested.csv")),
        "the nested file must be moved when subfolders are included");
    assertTrue(
        Files.exists(destinationFolder.resolve("archive").resolve("nested.csv")),
        "the nested file must arrive under its sub-folder");
  }

  private Path createUnreadableFolder() throws IOException {
    Path unreadable = Files.createDirectory(sourceFolder.resolve("no-access"));
    Files.createFile(unreadable.resolve("hidden.csv"));
    Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));
    assumeTrue(
        unreadable.toFile().list() == null,
        "folder is still readable, the test cannot run as this user (root?)");
    return unreadable;
  }
}
