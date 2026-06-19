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
package org.apache.hop.workflow.actions.copyfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Node;

/**
 * Integration-style tests using a real filesystem and full {@link ActionCopyFiles#execute} path.
 */
class WorkflowActionCopyFilesLocalTest {

  @TempDir Path tempDir;

  private ActionCopyFiles action;
  private IWorkflowEngine<WorkflowMeta> workflow;

  @BeforeAll
  static void initLog() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionCopyFiles();
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setLogLevel(LogLevel.ERROR);
  }

  @Test
  void copyFileIntoExistingFolder() throws Exception {
    Path srcFile = tempDir.resolve("src/hello.txt");
    Files.createDirectories(srcFile.getParent());
    Files.writeString(srcFile, "hello", StandardCharsets.UTF_8);
    Path destDir = tempDir.resolve("dest");
    Files.createDirectories(destDir);

    action.setDestinationIsAFile(false);
    action.setCreateDestinationFolder(false);
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                srcFile.toAbsolutePath().toString(), destDir.toAbsolutePath().toString(), "")));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "copy should succeed");
    assertEquals(0, result.getNrErrors());
    Path out = destDir.resolve("hello.txt");
    assertTrue(Files.isRegularFile(out));
    assertEquals("hello", Files.readString(out, StandardCharsets.UTF_8));
  }

  @Test
  void copyFileToFileCreatesParentWhenConfigured() throws Exception {
    Path src = tempDir.resolve("source/a.txt");
    Files.createDirectories(src.getParent());
    Files.writeString(src, "content", StandardCharsets.UTF_8);
    Path destFile = tempDir.resolve("nested/out/b.txt");

    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(true);
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                src.toAbsolutePath().toString(), destFile.toAbsolutePath().toString(), "")));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertTrue(Files.isRegularFile(destFile));
    assertEquals("content", Files.readString(destFile, StandardCharsets.UTF_8));
  }

  @Test
  void failsWhenDestinationParentMissingAndCreateDisabled() throws Exception {
    Path src = tempDir.resolve("only.txt");
    Files.writeString(src, "x", StandardCharsets.UTF_8);
    Path destFile = tempDir.resolve("noSuchParent/child/c.txt");

    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(false);
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                src.toAbsolutePath().toString(), destFile.toAbsolutePath().toString(), "")));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void folderSourceWithDestinationIsFileLogsError() throws Exception {
    Path srcDir = tempDir.resolve("folderSrc");
    Files.createDirectories(srcDir);
    Files.writeString(srcDir.resolve("f.txt"), "in", StandardCharsets.UTF_8);
    Path destFile = tempDir.resolve("destAsFile.txt");

    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(true);
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                srcDir.toAbsolutePath().toString(), destFile.toAbsolutePath().toString(), "")));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void copyFolderToFolderCopiesFileInRoot() throws Exception {
    Path srcRoot = tempDir.resolve("srcTree");
    Files.createDirectories(srcRoot);
    Files.writeString(srcRoot.resolve("doc.txt"), "doc", StandardCharsets.UTF_8);
    Path destRoot = tempDir.resolve("destTree");
    Files.createDirectories(destRoot);

    action.setDestinationIsAFile(false);
    action.setIncludeSubFolders(false);
    action.setCreateDestinationFolder(false);
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                srcRoot.toAbsolutePath().toString(), destRoot.toAbsolutePath().toString(), "")));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    Path copied = destRoot.resolve("doc.txt");
    assertTrue(Files.isRegularFile(copied));
    assertEquals("doc", Files.readString(copied, StandardCharsets.UTF_8));
  }

  @Test
  void removeSourceFilesDeletesSourceAfterFileToFileCopy() throws Exception {
    Path srcFile = tempDir.resolve("moveSrc/x.txt");
    Files.createDirectories(srcFile.getParent());
    Files.writeString(srcFile, "gone", StandardCharsets.UTF_8);
    Path destFile = tempDir.resolve("moveDest/y.txt");
    Files.createDirectories(destFile.getParent());

    action.setDestinationIsAFile(true);
    action.setCreateDestinationFolder(false);
    action.setRemoveSourceFiles(true);
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                srcFile.toAbsolutePath().toString(), destFile.toAbsolutePath().toString(), "")));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertFalse(Files.exists(srcFile), "source file should be removed");
    assertTrue(Files.isRegularFile(destFile));
    assertEquals("gone", Files.readString(destFile, StandardCharsets.UTF_8));
  }

  @Test
  void argFromPreviousUsesResultRows() throws Exception {
    Path srcFile = tempDir.resolve("prev/a.txt");
    Files.createDirectories(srcFile.getParent());
    Files.writeString(srcFile, "row", StandardCharsets.UTF_8);
    Path destDir = tempDir.resolve("prevDest");
    Files.createDirectories(destDir);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("source"));
    rowMeta.addValueMeta(new ValueMetaString("dest"));
    rowMeta.addValueMeta(new ValueMetaString("wildcard"));
    RowMetaAndData row =
        new RowMetaAndData(
            rowMeta,
            new Object[] {
              srcFile.toAbsolutePath().toString(), destDir.toAbsolutePath().toString(), ""
            });

    Result previous = new Result();
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(row);
    previous.setRows(rows);

    action.setArgFromPrevious(true);
    action.setDestinationIsAFile(false);
    action.setFileRows(List.of());

    Result result = action.execute(previous, 0);

    assertTrue(result.isResult());
    assertTrue(Files.isRegularFile(destDir.resolve("a.txt")));
  }

  @Test
  void normalizeLegacyVfsPathsStripsRowIndexedPrefixes() {
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                ActionCopyFiles.SOURCE_URL + "0-alpha", ActionCopyFiles.DEST_URL + "0-beta", "w")));
    action.normalizeLegacyVfsPaths();
    assertEquals("alpha", action.getFileRows().get(0).getSourceFileFolder());
    assertEquals("beta", action.getFileRows().get(0).getDestinationFileFolder());
    assertEquals("w", action.getFileRows().get(0).getWildcard());
  }

  @Test
  void convertLegacyXmlDelegatesToNormalize() throws HopException {
    action.setFileRows(
        List.of(
            new CopyFilesItem(
                ActionCopyFiles.SOURCE_URL + "0-q", ActionCopyFiles.DEST_URL + "0-r", null)));
    action.convertLegacyXml(mock(Node.class));
    assertEquals("q", action.getFileRows().get(0).getSourceFileFolder());
    assertEquals("r", action.getFileRows().get(0).getDestinationFileFolder());
  }

  @Test
  void checkPassesWhenSourceExists() throws Exception {
    Path src = tempDir.resolve("check.txt");
    Files.writeString(src, "ok", StandardCharsets.UTF_8);
    action.setFileRows(
        List.of(new CopyFilesItem(src.toAbsolutePath().toString(), tempDir.toString(), "")));

    List<org.apache.hop.core.ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, new WorkflowMeta(), new Variables(), new MemoryMetadataProvider());

    assertTrue(
        remarks.stream()
            .noneMatch(r -> r.getType() == org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR),
        remarks::toString);
  }

  @Test
  void isEvaluationTrue() {
    assertTrue(new ActionCopyFiles().isEvaluation());
  }
}
