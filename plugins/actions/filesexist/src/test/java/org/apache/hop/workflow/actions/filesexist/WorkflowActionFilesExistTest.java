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

package org.apache.hop.workflow.actions.filesexist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.utils.TestUtils;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowActionFilesExistTest {
  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionFilesExist action;

  private String existingFile1;
  private String existingFile2;
  private String folderPath;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionFilesExist();

    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    WorkflowMeta mockWorkflowMeta = mock(WorkflowMeta.class);
    action.setParentWorkflowMeta(mockWorkflowMeta);

    workflow.setStopped(false);

    String base = getClass().getSimpleName();
    existingFile1 = TestUtils.createRamFile(base + "/existingFile1.ext", action);
    existingFile2 = TestUtils.createRamFile(base + "/existingFile2.ext", action);

    folderPath = "ram://" + base + "/folder";
    try (FileObject folder = HopVfs.getFileObject(folderPath, action)) {
      folder.createFolder();
    }
    TestUtils.createRamFile(base + "/folder/match.txt", action);
    TestUtils.createRamFile(base + "/folder/skip.tmp", action);
    TestUtils.createRamFile(base + "/folder/other.log", action);
    TestUtils.createRamFile(base + "/folder/sub/nested.txt", action);
  }

  @Test
  void testSerialization() throws Exception {
    ActionFilesExist meta =
        ActionSerializationTestUtil.testSerialization(
            "/files-exist-action.xml", ActionFilesExist.class);

    assertEquals("/folder", meta.getFileItems().get(0).getFileName());
    assertEquals(".*\\.txt", meta.getFileItems().get(0).getFileMask());
    assertEquals(".*\\.tmp", meta.getFileItems().get(0).getExcludeFileMask());
    assertTrue(meta.getFileItems().get(0).isIncludeSubfolders());
    assertEquals("/archive.zip", meta.getFileItems().get(1).getFileName());
    assertFalse(meta.getFileItems().get(1).isIncludeSubfolders());
    assertEquals(2, meta.getFileItems().size());
  }

  @Test
  void testSetNrErrorsFalseResult() {
    action.setFileItems(List.of(new FileItem("nonExistingFile.ext")));

    Result res = action.execute(new Result(), 0);

    assertFalse(res.isResult(), "Entry should fail");
    assertEquals(
        0,
        res.getNrErrors(),
        "Files not found. Result is false. But... No of errors should be zero");
  }

  @Test
  void testExecuteWithException() {
    action.setFileItems(List.of(new FileItem(null)));

    Result res = action.execute(new Result(), 0);

    assertFalse(res.isResult(), "Action should fail");
    assertEquals(
        1, res.getNrErrors(), "File with wrong name was specified. One error should be reported");
  }

  @Test
  void testExecuteSuccess() {
    action.setFileItems(List.of(new FileItem(existingFile1), new FileItem(existingFile2)));

    Result res = action.execute(new Result(), 0);
    assertTrue(res.isResult());
  }

  @Test
  void testExecuteFail() {
    action.setFileItems(
        List.of(
            new FileItem(existingFile1),
            new FileItem(existingFile2),
            new FileItem("nonExistingFile1.ext"),
            new FileItem("nonExistingFile2.ext")));

    Result res = action.execute(new Result(), 0);
    assertFalse(res.isResult());
  }

  @Test
  void testFolderExistsWithoutWildcard() {
    action.setFileItems(List.of(new FileItem(folderPath)));

    Result res = action.execute(new Result(), 0);
    assertTrue(res.isResult(), "Folder path without wildcards should succeed when folder exists");
  }

  @Test
  void testFolderWithMatchingWildcard() {
    action.setFileItems(List.of(new FileItem(folderPath, ".*\\.txt")));

    Result res = action.execute(new Result(), 0);
    assertTrue(res.isResult(), "Matching files in folder should succeed");
  }

  @Test
  void testFolderWithNonMatchingWildcard() {
    action.setFileItems(List.of(new FileItem(folderPath, ".*\\.csv")));

    Result res = action.execute(new Result(), 0);
    assertFalse(res.isResult(), "No matching files should fail");
  }

  @Test
  void testFolderWithExcludeWildcard() {
    // Only skip.tmp would match .*, but exclude removes .tmp — match.txt and other.log remain
    action.setFileItems(List.of(new FileItem(folderPath, ".*", ".*\\.tmp")));

    Result res = action.execute(new Result(), 0);
    assertTrue(res.isResult(), "Files remaining after exclude should succeed");
  }

  @Test
  void testFolderWithExcludeRemovesAllMatches() {
    // Include only .tmp, then exclude .tmp → nothing left
    action.setFileItems(List.of(new FileItem(folderPath, ".*\\.tmp", ".*\\.tmp")));

    Result res = action.execute(new Result(), 0);
    assertFalse(res.isResult(), "All matches excluded should fail");
  }

  @Test
  void testIncludeSubfoldersPerRow() {
    // nested.txt is in a subfolder; without includeSubfolders it is not found
    action.setFileItems(List.of(new FileItem(folderPath, "nested\\.txt", null, false)));

    Result withoutSub = action.execute(new Result(), 0);
    assertFalse(withoutSub.isResult(), "Nested file should not match without subfolders");

    action.setFileItems(List.of(new FileItem(folderPath, "nested\\.txt", null, true)));
    Result withSub = action.execute(new Result(), 0);
    assertTrue(withSub.isResult(), "Nested file should match with include subfolders");
  }
}
