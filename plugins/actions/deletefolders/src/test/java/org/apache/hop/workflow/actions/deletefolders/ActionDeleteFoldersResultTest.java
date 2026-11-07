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

package org.apache.hop.workflow.actions.deletefolders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Result / nrErrors behaviour for Delete folders (issue #9264): success-condition boundary must not
 * depend on whether the failing entry is last or earlier in the list.
 */
class ActionDeleteFoldersResultTest {

  @TempDir Path testFolder;

  private ActionDeleteFolders action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    action = attachedAction();
    action.setArgFromPrevious(false);
    action.setSuccessCondition(ActionDeleteFolders.SuccessCondition.ERRORS_LESS);
  }

  /** #9264: last entry fails with limit 1 — "less than" means strict &lt;, so false. */
  @Test
  void errorsLessThanIsFalseWhenLastEntryHitsTheLimit() throws Exception {
    Path folder = Files.createDirectories(testFolder.resolve("keep-me"));
    Path notAFolder = writeFile(testFolder.resolve("not-a-folder.txt"), "x");

    action.setLimitFolders("1");
    action.setFileItems(
        List.of(new FileItem(folder.toString()), new FileItem(notAFolder.toString())));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "1 error is not less than 1");
    assertEquals(1, result.getNrErrors());
    assertFalse(Files.exists(folder), "the real folder must still be deleted before the error");
  }

  /**
   * #9264: first entry hits the limit — early exit and final status both false (same as last-entry
   * case), and the remaining folder is not deleted.
   */
  @Test
  void errorsLessThanIsFalseWhenFirstEntryHitsTheLimit() throws Exception {
    Path folder = Files.createDirectories(testFolder.resolve("keep-me"));
    Files.writeString(folder.resolve("marker.txt"), "x", StandardCharsets.UTF_8);
    Path notAFolder = writeFile(testFolder.resolve("not-a-folder.txt"), "x");

    action.setLimitFolders("1");
    action.setFileItems(
        List.of(new FileItem(notAFolder.toString()), new FileItem(folder.toString())));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "1 error is not less than 1");
    assertEquals(1, result.getNrErrors());
    assertTrue(Files.exists(folder), "remaining folder must not be deleted after the limit");
  }

  /** With limit 2, a single non-folder entry still succeeds under "errors less than". */
  @Test
  void errorsLessThanAllowsFewerErrorsThanTheLimit() throws Exception {
    Path folder = Files.createDirectories(testFolder.resolve("ok"));
    Path notAFolder = writeFile(testFolder.resolve("not-a-folder.txt"), "x");

    action.setLimitFolders("2");
    action.setFileItems(
        List.of(new FileItem(folder.toString()), new FileItem(notAFolder.toString())));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getNrErrors());
    assertFalse(Files.exists(folder));
  }

  private static Path writeFile(Path path, String content) throws Exception {
    Files.createDirectories(path.getParent());
    return Files.writeString(path, content, StandardCharsets.UTF_8);
  }

  private static ActionDeleteFolders attachedAction() {
    ActionDeleteFolders deleteFolders = new ActionDeleteFolders("delete folders");
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.setStopped(false);
    deleteFolders.setParentWorkflow(workflow);
    return deleteFolders;
  }
}
