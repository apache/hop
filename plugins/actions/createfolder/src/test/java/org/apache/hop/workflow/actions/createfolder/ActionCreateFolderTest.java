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
 *
 */

package org.apache.hop.workflow.actions.createfolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link ActionCreateFolder} */
class ActionCreateFolderTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @Test
  void testSerialization() throws Exception {
    ActionCreateFolder action =
        ActionSerializationTestUtil.testSerialization(
            "/create-folder-action.xml", ActionCreateFolder.class);

    assertEquals("${TEST_FOLDER}/one", action.getFolderName());
    assertTrue(action.isFailIfFolderExists());
  }

  @Test
  void testClone() throws Exception {
    ActionCreateFolder action =
        ActionSerializationTestUtil.testSerialization(
            "/create-folder-action.xml", ActionCreateFolder.class);

    ActionCreateFolder clone = (ActionCreateFolder) action.clone();

    assertEquals(action.getFolderName(), clone.getFolderName());
    assertEquals(action.isFailIfFolderExists(), clone.isFailIfFolderExists());
  }

  @Test
  void constructorLeavesFolderNameEmptyAndFailsIfItExists() {
    ActionCreateFolder action = new ActionCreateFolder();

    assertNull(action.getFolderName());
    assertTrue(action.isFailIfFolderExists());
    assertTrue(action.isEvaluation());
  }

  @Test
  void getRealFolderNameResolvesVariables() {
    ActionCreateFolder action = new ActionCreateFolder();
    action.setVariable("TEST_FOLDER", tempDir.toString());
    action.setFolderName("${TEST_FOLDER}/one");

    assertEquals(tempDir.toString() + "/one", action.getRealFolderName());
  }

  @Test
  void checkAcceptsAFolderName() {
    ActionCreateFolder action = new ActionCreateFolder();
    action.setFolderName(tempDir.resolve("missing").toString());

    List<ICheckResult> remarks = check(action);

    assertTrue(errors(remarks).isEmpty());
    assertTrue(remarks.stream().anyMatch(remark -> remark.getText().contains("folderName is OK")));
    assertTrue(remarks.stream().noneMatch(remark -> remark.getText().contains("Reason: filename")));
  }

  @Test
  void checkReportsAMissingFolderName() {
    ActionCreateFolder action = new ActionCreateFolder();

    List<String> errors = errors(check(action));

    assertEquals(1, errors.size());
    assertTrue(errors.getFirst().contains("folderName cannot be null or blank"));
    assertFalse(errors.getFirst().contains("Reason: filename"));
  }

  @Test
  void checkReportsAnEmptyFolderName() {
    ActionCreateFolder action = new ActionCreateFolder();
    action.setFolderName("");

    List<String> errors = errors(check(action));

    assertEquals(1, errors.size());
    assertTrue(errors.getFirst().contains("folderName cannot be null or blank"));
  }

  @Test
  void checkReportsAWhitespaceFolderName() {
    ActionCreateFolder action = new ActionCreateFolder();
    action.setFolderName("   ");

    List<String> errors = errors(check(action));

    assertEquals(1, errors.size());
    assertTrue(errors.getFirst().contains("folderName cannot be null or blank"));
  }

  @Test
  void executeFailsWhenNoFolderNameIsDefined() {
    ActionCreateFolder action = new ActionCreateFolder();

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void executeCreatesAMissingFolder() {
    ActionCreateFolder action = new ActionCreateFolder();
    Path folder = tempDir.resolve("created");
    action.setFolderName(folder.toString());

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertTrue(Files.isDirectory(folder));
  }

  @Test
  void executeFailsWhenTheFolderAlreadyExists() throws Exception {
    ActionCreateFolder action = new ActionCreateFolder();
    Path folder = Files.createDirectory(tempDir.resolve("exists"));
    action.setFolderName(folder.toString());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void executeSucceedsWhenAnExistingFolderIsAllowed() throws Exception {
    ActionCreateFolder action = new ActionCreateFolder();
    Path folder = Files.createDirectory(tempDir.resolve("exists"));
    action.setFolderName(folder.toString());
    action.setFailIfFolderExists(false);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void executeFailsWhenAFileOccupiesTheFolderName() throws Exception {
    ActionCreateFolder action = new ActionCreateFolder();
    Path file = Files.createFile(tempDir.resolve("not-a-folder"));
    action.setFolderName(file.toString());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void executeReportsAnErrorWhenTheFolderCannotBeCreated() throws Exception {
    ActionCreateFolder action = new ActionCreateFolder();
    Path file = Files.createFile(tempDir.resolve("parent-is-a-file"));
    action.setFolderName(file.resolve("child").toString());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  private static List<ICheckResult> check(ActionCreateFolder action) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, action, null);
    return remarks;
  }

  private static List<String> errors(List<ICheckResult> remarks) {
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
