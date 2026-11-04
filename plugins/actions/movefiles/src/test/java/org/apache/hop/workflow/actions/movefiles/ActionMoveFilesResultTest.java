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

package org.apache.hop.workflow.actions.movefiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Result / nrErrors behaviour for Move files (issue #9175): success-condition boundary and
 * single-counting when destination exists with Fail.
 */
class ActionMoveFilesResultTest {

  @TempDir Path testFolder;

  private ActionMoveFiles action;
  private Path sourceFolder;
  private Path destinationFolder;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    action = MoveFilesActionHelper.defaultAction();
    action.setDestinationIsAFile(false);
    action.setCreateDestinationFolder(true);
    sourceFolder = Files.createDirectories(testFolder.resolve("source"));
    destinationFolder = Files.createDirectories(testFolder.resolve("dest"));
  }

  /** #9175 case 1a: last entry fails with limit 1 — lesser than means strict &lt;, so false. */
  @Test
  void errorsLesserThanIsFalseWhenLastEntryHitsTheLimit() throws Exception {
    Path ok = write(sourceFolder.resolve("ok.txt"), "ok");
    Path missing = sourceFolder.resolve("MISSING.txt");

    action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_ERRORS_LESS);
    action.setNrErrorsLessThan("1");
    action.setFilesToMove(
        List.of(
            entry(ok.toString(), destinationFolder.toString()),
            entry(missing.toString(), destinationFolder.toString())));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "1 error is not lesser than 1");
    assertEquals(1, result.getNrErrors());
  }

  /**
   * #9175 case 1b: first entry hits the limit — early exit must use the same success condition as
   * the normal end (also false), not leave the initial result=false for a different reason while
   * case 1a would have been true under the old &lt;= check.
   */
  @Test
  void errorsLesserThanIsFalseWhenFirstEntryHitsTheLimit() throws Exception {
    Path ok = write(sourceFolder.resolve("ok.txt"), "ok");
    Path missing = sourceFolder.resolve("MISSING.txt");

    action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_ERRORS_LESS);
    action.setNrErrorsLessThan("1");
    action.setFilesToMove(
        List.of(
            entry(missing.toString(), destinationFolder.toString()),
            entry(ok.toString(), destinationFolder.toString())));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "1 error is not lesser than 1");
    assertEquals(1, result.getNrErrors());
  }

  /** With limit 2, a single missing source still succeeds under "errors lesser than". */
  @Test
  void errorsLesserThanAllowsFewerErrorsThanTheLimit() throws Exception {
    Path ok = write(sourceFolder.resolve("ok.txt"), "ok");
    Path missing = sourceFolder.resolve("MISSING.txt");

    action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_ERRORS_LESS);
    action.setNrErrorsLessThan("2");
    action.setFilesToMove(
        List.of(
            entry(ok.toString(), destinationFolder.toString()),
            entry(missing.toString(), destinationFolder.toString())));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  /**
   * #9175 case 2: destination exists + Fail must count one error, not two (inner FAIL + outer
   * updateErrors on processFileFolder).
   */
  @Test
  void failWhenDestinationExistsCountsOneError() throws Exception {
    Path sourceFile = write(sourceFolder.resolve("a.txt"), "SOURCE");
    Path destinationFile = write(destinationFolder.resolve("a.txt"), "ALREADY-HERE");

    action.setDestinationIsAFile(true);
    action.setIfFileExists(ActionMoveFiles.FAIL);
    action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_NO_ERRORS);
    action.setFilesToMove(List.of(entry(sourceFile.toString(), destinationFile.toString())));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors(), "one existing destination must be one error");
  }

  /**
   * Folder source + Fail: moveOneFile must propagate moveFile's false (it used to force true and
   * hide the failure after updateErrors was moved to the caller).
   */
  @Test
  void failWhenDestinationExistsInFolderSourceCountsOneError() throws Exception {
    write(sourceFolder.resolve("a.txt"), "SOURCE");
    write(destinationFolder.resolve("a.txt"), "ALREADY-HERE");

    action.setDestinationIsAFile(false);
    action.setIfFileExists(ActionMoveFiles.FAIL);
    action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_ERRORS_LESS);
    action.setNrErrorsLessThan("1");
    action.setFilesToMove(
        List.of(entry(sourceFolder.toString(), destinationFolder.toString(), ".*\\.txt")));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors(), "one existing destination must be one error");
  }

  /**
   * Folder source + Move to folder: when the destination file exists but the move-to folder does
   * not yet contain that name, the move succeeds and must set retVal / updateSuccess (otherwise
   * each moved file is counted as an error).
   */
  @Test
  void moveToFolderSucceedsWhenDestinationExistsAndMoveToIsFree() throws Exception {
    write(sourceFolder.resolve("a.txt"), "SOURCE");
    write(destinationFolder.resolve("a.txt"), "ALREADY-HERE");
    Path moveToFolder = Files.createDirectories(testFolder.resolve("move-to"));

    action.setDestinationIsAFile(false);
    action.setIfFileExists(ActionMoveFiles.MOVE_FILE);
    action.setDestinationFolder(moveToFolder.toString());
    action.setIfMovedFileExists(ActionMoveFiles.FAIL);
    action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_NO_ERRORS);
    action.setFilesToMove(
        List.of(entry(sourceFolder.toString(), destinationFolder.toString(), ".*\\.txt")));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "successful move-to-folder must not count as an error");
    assertEquals(0, result.getNrErrors());
    assertFalse(Files.exists(sourceFolder.resolve("a.txt")));
    assertTrue(Files.exists(moveToFolder.resolve("a.txt")));
  }

  /**
   * Single-file source + Move to folder: same moveFile() branch as the folder-source case; without
   * updateSuccess / retVal=true a successful move was counted as an error (already wrong on main).
   */
  @Test
  void moveToFolderSucceedsForSingleFileWhenDestinationExistsAndMoveToIsFree() throws Exception {
    Path sourceFile = write(sourceFolder.resolve("a.txt"), "SOURCE");
    Path destinationFile = write(destinationFolder.resolve("a.txt"), "ALREADY-HERE");
    Path moveToFolder = Files.createDirectories(testFolder.resolve("move-to-file"));

    action.setDestinationIsAFile(true);
    action.setIfFileExists(ActionMoveFiles.MOVE_FILE);
    action.setDestinationFolder(moveToFolder.toString());
    action.setIfMovedFileExists(ActionMoveFiles.FAIL);
    action.setSuccessCondition(ActionMoveFiles.SUCCESS_IF_NO_ERRORS);
    action.setFilesToMove(List.of(entry(sourceFile.toString(), destinationFile.toString())));

    Result result = action.execute(new Result(), 0);

    assertTrue(
        result.isResult(), "successful single-file move-to-folder must not count as an error");
    assertEquals(0, result.getNrErrors());
    assertFalse(Files.exists(sourceFile));
    assertTrue(Files.exists(moveToFolder.resolve("a.txt")));
  }

  private static ActionMoveFiles.FileToMove entry(String source, String destination) {
    return entry(source, destination, null);
  }

  private static ActionMoveFiles.FileToMove entry(
      String source, String destination, String wildcard) {
    ActionMoveFiles.FileToMove fileToMove = new ActionMoveFiles.FileToMove();
    fileToMove.setSourceFileFolder(source);
    fileToMove.setDestinationFileFolder(destination);
    fileToMove.setWildcard(wildcard);
    return fileToMove;
  }

  private static Path write(Path path, String content) throws Exception {
    Files.createDirectories(path.getParent());
    return Files.writeString(path, content, StandardCharsets.UTF_8);
  }
}
