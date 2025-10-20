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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkflowActionMoveFilesLocalTest {

  @TempDir Path testFolder;

  private ActionMoveFiles action;
  private File sourceFolder;
  private File destinationFolder;
  private static final String TEST_FILE_CONTENT = "test file content";

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws IOException {
    action = MoveFilesActionHelper.defaultAction();
    sourceFolder = testFolder.resolve("source").toFile();
    sourceFolder.mkdirs();
    destinationFolder = testFolder.resolve("destination").toFile();
    destinationFolder.mkdirs();
  }

  @Test
  void testBasicFileMoveOperation() throws IOException, HopException {
    Path sourceFile = createTestFilePath(sourceFolder, "test.txt");
    Path destFile = destinationFolder.toPath().resolve("test.txt");

    action.sourceFileFolder = new String[] {sourceFile.toString()};
    action.destinationFileFolder = new String[] {destFile.toString()};
    action.setDestinationIsAFile(true);

    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Move operation should succeed");
    assertFalse(Files.exists(sourceFile), "Source file should not exist");
    assertTrue(Files.exists(destFile), "Destination file should exist");
    assertEquals(
        TEST_FILE_CONTENT,
        Files.readString(destFile, StandardCharsets.UTF_8),
        "File content should match");
  }

  @Test
  void testMoveWithWildcard() throws IOException, HopException {
    createTestFilePath(sourceFolder, "test1.txt");
    createTestFilePath(sourceFolder, "test2.txt");
    createTestFilePath(sourceFolder, "other.txt");

    action.sourceFileFolder = new String[] {sourceFolder.getAbsolutePath()};
    action.destinationFileFolder = new String[] {destinationFolder.getAbsolutePath()};
    action.wildcard = new String[] {"test.*\\.txt"};
    action.setDestinationIsAFile(false);

    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Move operation should succeed");
    assertTrue(
        Files.exists(destinationFolder.toPath().resolve("test1.txt")), "test1.txt should be moved");
    assertTrue(
        Files.exists(destinationFolder.toPath().resolve("test2.txt")), "test2.txt should be moved");
    assertTrue(Files.exists(sourceFolder.toPath().resolve("other.txt")), "other.txt should remain");
  }

  @Test
  void testMoveToExistingFile() throws IOException, HopException {
    Path sourceFile = createTestFilePath(sourceFolder, "test.txt");
    Path destFile = destinationFolder.toPath().resolve("test.txt");
    String originalContent = "original content";
    Files.writeString(destFile, originalContent, StandardCharsets.UTF_8);

    action.sourceFileFolder = new String[] {sourceFile.toString()};
    action.destinationFileFolder = new String[] {destFile.toString()};
    action.setDestinationIsAFile(true);

    Result result = action.execute(new Result(), 0);
    assertFalse(result.isResult(), "Move should not succeed when destination exists");
    assertTrue(Files.exists(sourceFile), "Source file should still exist");
    assertEquals(
        originalContent,
        Files.readString(destFile, StandardCharsets.UTF_8),
        "Destination content should be unchanged");
  }

  @Test
  void testMoveWithOverwrite() throws IOException, HopException {
    Path sourceFile = createTestFilePath(sourceFolder, "test.txt");
    Path destFile = createTestFilePath(destinationFolder, "test.txt");

    action.sourceFileFolder = new String[] {sourceFile.toString()};
    action.destinationFileFolder = new String[] {destFile.toString()};
    action.setDestinationIsAFile(true);
    action.setIfFileExists("overwrite_file");

    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Move with overwrite should succeed");
    assertFalse(Files.exists(sourceFile), "Source file should not exist");
    assertTrue(Files.exists(destFile), "Destination file should exist");
    assertEquals(
        TEST_FILE_CONTENT,
        Files.readString(destFile, StandardCharsets.UTF_8),
        "File content should match source");
  }

  @Test
  void testCreateDestinationFolder() throws IOException, HopException {
    Files.deleteIfExists(destinationFolder.toPath());

    Path sourceFile = createTestFilePath(sourceFolder, "test.txt");
    Path destFile = destinationFolder.toPath().resolve("test.txt");

    action.sourceFileFolder = new String[] {sourceFile.toString()};
    action.destinationFileFolder = new String[] {destFile.toString()};
    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(true);

    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Move should succeed");
    assertTrue(Files.exists(destinationFolder.toPath()), "Destination folder should be created");
    assertTrue(Files.exists(destFile), "File should be moved");
  }

  private Path createTestFilePath(File folder, String filename) throws IOException {
    Path filePath = folder.toPath().resolve(filename);
    Files.writeString(filePath, TEST_FILE_CONTENT, StandardCharsets.UTF_8);
    return filePath;
  }
}
