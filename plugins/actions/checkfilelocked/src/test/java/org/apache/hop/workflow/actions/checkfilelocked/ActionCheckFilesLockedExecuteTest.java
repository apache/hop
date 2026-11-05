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

package org.apache.hop.workflow.actions.checkfilelocked;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/** See https://github.com/apache/hop/issues/9117 */
class ActionCheckFilesLockedExecuteTest {

  @TempDir Path folder;

  private ActionCheckFilesLocked action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionCheckFilesLocked();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);

    Files.writeString(folder.resolve("data.txt"), "data", StandardCharsets.UTF_8);
  }

  @Test
  void unlockedFolderPasses() {
    action.getCheckedFiles().add(new ActionCheckFilesLocked.CheckedFile(folder.toString(), ".*"));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void invalidWildcardFailsTheCheck() {
    action.getCheckedFiles().add(new ActionCheckFilesLocked.CheckedFile(folder.toString(), "["));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "nothing was checked, so the check must not pass");
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void resultRowWithOneFieldIsChecked() {
    action.setArgFromPrevious(true);
    RowMetaAndData row = new RowMetaAndData();
    row.addValue(new ValueMetaString("filename"), folder.toString());
    Result previous = new Result();
    previous.setRows(List.of(row));

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  /**
   * A file that can not be renamed counts as locked. In a read-only folder that is every file, so
   * the one-field row must be checked and fail the action.
   */
  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void resultRowWithOneFieldFindsALockedFile() throws Exception {
    Path readOnly = Files.createDirectory(folder.resolve("read-only"));
    Path lockedFile = Files.writeString(readOnly.resolve("locked.txt"), "x");
    Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeFalse(
          Files.isWritable(readOnly), "folder is still writable, cannot run as this user (root?)");
      action.setArgFromPrevious(true);
      RowMetaAndData row = new RowMetaAndData();
      row.addValue(new ValueMetaString("filename"), lockedFile.toString());
      Result previous = new Result();
      previous.setRows(List.of(row));

      Result result = action.execute(previous, 0);

      assertFalse(result.isResult(), "the file can not be renamed, so it counts as locked");
    } finally {
      Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("rwx------"));
    }
  }
}
