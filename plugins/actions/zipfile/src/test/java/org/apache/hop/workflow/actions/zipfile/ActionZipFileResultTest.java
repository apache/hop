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

package org.apache.hop.workflow.actions.zipfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Result behaviour for Zip file (issue #9175 cases 5 and 6). */
class ActionZipFileResultTest {

  private String ramFolder;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    ramFolder = "ram:///" + UUID.randomUUID();
  }

  @AfterEach
  void tearDown() throws Exception {
    HopVfs.getFileObject(ramFolder).deleteAll();
  }

  /** #9175 case 5b: empty source must set result false, not inherit the previous true. */
  @Test
  void emptySourceDirectoryFailsTheAction() {
    ActionZipFile action = newAction();
    action.setFromPrevious(false);
    action.setSourceDirectory("");
    action.setZipFilename(ramFolder + "/out/empty.zip");

    Result previous = new Result();
    previous.setResult(true);

    Result result = action.execute(previous, 0);

    assertFalse(result.isResult(), "empty source must not keep the previous true result");
    assertEquals(1, result.getNrErrors());
  }

  /**
   * Get arguments from previous with no result rows must set result false (not inherit the previous
   * true).
   */
  @Test
  void fromPreviousWithNoRowsFailsTheAction() {
    ActionZipFile action = newAction();
    action.setFromPrevious(true);

    Result previous = new Result();
    previous.setResult(true);
    previous.setRows(List.of());

    Result result = action.execute(previous, 0);

    assertFalse(result.isResult(), "no previous rows must not keep the previous true result");
    assertEquals(1, result.getNrErrors());
  }

  /**
   * Get arguments from previous: rows with an empty source or zip file name are skipped. When every
   * row is skipped, the action is false (it does not inherit the previous true result).
   */
  @Test
  void fromPreviousWithOnlySkippedRowsFailsTheAction() {
    ActionZipFile action = newAction();
    action.setFromPrevious(true);

    Result previous = new Result();
    previous.setResult(true);
    previous.setRows(
        List.of(
            row("", ramFolder + "/out/skipped.zip"),
            row(ramFolder + "/source/a.txt", ""),
            row("", "")));

    Result result = action.execute(previous, 0);

    assertFalse(
        result.isResult(), "all-skipped previous rows must not keep the previous true result");
    assertEquals(1, result.getNrErrors());
  }

  /**
   * #9175 case 5: Get arguments from previous must set result true on success so a prior false hop
   * is not inherited.
   */
  @Test
  void fromPreviousSuccessSetsResultTrue() throws Exception {
    writeFile(ramFolder + "/source/a.txt", "zip-me");
    String zip = ramFolder + "/out/from-previous.zip";

    ActionZipFile action = newAction();
    action.setFromPrevious(true);
    action.setCreateParentFolder(true);

    Result previous = new Result();
    previous.setResult(false);
    previous.setNrErrors(0);
    previous.setRows(List.of(row(ramFolder + "/source/a.txt", zip)));

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult(), "successful fromPrevious zip must set result true");
    assertEquals(0, result.getNrErrors());
    assertTrue(HopVfs.getFileObject(zip).exists(), "zip file should exist");
  }

  /**
   * #9175 case 6: a failed after-zip Move must leave the action false (successResult must not be
   * forced back to true).
   */
  @Test
  void failedMoveAfterZipFailsTheAction() throws Exception {
    writeFile(ramFolder + "/source/a.txt", "zip-then-move");
    String zip = ramFolder + "/out/archive.zip";
    String moveTo = ramFolder + "/move-to";
    HopVfs.getFileObject(moveTo).createFolder();

    ActionZipFile action = newAction();
    action.setFromPrevious(false);
    action.setSourceDirectory(ramFolder + "/source/a.txt");
    action.setZipFilename(zip);
    action.setCreateParentFolder(true);
    action.setAfterZip(2); // Move files
    action.setMoveToDirectory(moveTo);
    action.setCreateMoveToDirectory(false);
    action.setIfZipFileExists(2);

    Result result;
    try (MockedStatic<HopVfs> vfs = Mockito.mockStatic(HopVfs.class, Mockito.CALLS_REAL_METHODS)) {
      // Fail only the after-zip source move, not the temp-zip rename during creation.
      vfs.when(() -> HopVfs.moveFile(any(), any()))
          .thenAnswer(
              invocation -> {
                FileObject destination = invocation.getArgument(1, FileObject.class);
                if (destination.getName().getPath().contains("move-to")) {
                  throw new FileSystemException("move failed");
                }
                return invocation.callRealMethod();
              });
      result = action.execute(new Result(), 0);
    }

    assertFalse(result.isResult(), "failed post-zip move must fail the action");
    assertTrue(HopVfs.getFileObject(zip).exists(), "zip should still have been created");
  }

  private static ActionZipFile newAction() {
    ActionZipFile action = new ActionZipFile();
    action.setParentWorkflow(new LocalWorkflowEngine(new WorkflowMeta()));
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setLogLevel(LogLevel.NOTHING);
    action.setCompressionRate(1);
    action.setAfterZip(0);
    action.setIfZipFileExists(2);
    action.setAddFileToResult(false);
    return action;
  }

  private static RowMetaAndData row(String source, String zip) {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString("source"));
    meta.addValueMeta(new ValueMetaString("wildcard"));
    meta.addValueMeta(new ValueMetaString("wildcard_exclude"));
    meta.addValueMeta(new ValueMetaString("zipfile"));
    return new RowMetaAndData(meta, source, "", "", zip);
  }

  private static void writeFile(String filename, String content) throws Exception {
    FileObject file = HopVfs.getFileObject(filename);
    file.getParent().createFolder();
    try (OutputStream out = HopVfs.getOutputStream(file, false)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }
}
