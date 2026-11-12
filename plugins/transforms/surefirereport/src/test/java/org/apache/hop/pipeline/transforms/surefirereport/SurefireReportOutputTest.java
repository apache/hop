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

package org.apache.hop.pipeline.transforms.surefirereport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SurefireReportOutputTest {

  private static final Class<?> PKG = SurefireReportOutputMeta.class;

  @TempDir Path tempDir;

  private TransformMockHelper<SurefireReportOutputMeta, SurefireReportOutputData> helper;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "surefire report output",
            SurefireReportOutputMeta.class,
            SurefireReportOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void missingFolderFailsInInitWhenCreateParentFolderIsOff() throws Exception {
    Path report = tempDir.resolve("nofolder/a/b/r.xml");
    SurefireReportOutput transform = createTransform(report.toString(), false);

    assertFalse(transform.init(), "init() must fail before any test runs");

    verify(helper.iLogChannel).logError(expectedMessage("ParentFolderDoesNotExist", report));
    assertFalse(Files.exists(tempDir.resolve("nofolder")), "no folder may be created");
  }

  @Test
  void missingFolderIsCreatedInInitWhenCreateParentFolderIsOn() throws Exception {
    Path report = tempDir.resolve("nofolder/a/b/r.xml");
    SurefireReportOutput transform = createTransform(report.toString(), true);

    assertTrue(transform.init());
    assertTrue(Files.isDirectory(report.getParent()), "the folder is created in init()");

    runToEnd(transform);
    assertTrue(Files.readString(report, StandardCharsets.UTF_8).contains("tests=\"1\""));
  }

  @Test
  void existingFolderIsWrittenWhenCreateParentFolderIsOff() throws Exception {
    Path report = tempDir.resolve("r.xml");
    SurefireReportOutput transform = createTransform(report.toString(), false);

    assertTrue(transform.init());
    runToEnd(transform);

    assertTrue(Files.readString(report, StandardCharsets.UTF_8).contains("tests=\"1\""));
  }

  @Test
  void fileUrlIsResolvedThroughVfs() throws Exception {
    Path report = tempDir.resolve("r.xml");
    SurefireReportOutput transform = createTransform(report.toUri().toString(), false);

    assertTrue(transform.init());
    runToEnd(transform);

    assertTrue(Files.readString(report, StandardCharsets.UTF_8).contains("tests=\"1\""));
  }

  @Test
  void parentThatIsAFileFails() throws Exception {
    Files.writeString(tempDir.resolve("data"), "not a folder");
    Path report = tempDir.resolve("data/r.xml");

    for (boolean createParentFolder : new boolean[] {false, true}) {
      HopException e =
          assertThrows(
              HopException.class,
              () -> SurefireReportOutput.prepareParentFolder(vfs(report), createParentFolder));
      assertEquals(expectedMessage("ParentFolderIsNotAFolder", report), e.getMessage().trim());
    }
  }

  @Test
  void folderRemovedDuringTheRunFailsWithAClearMessage() throws Exception {
    Path folder = Files.createDirectory(tempDir.resolve("reports"));
    Path report = folder.resolve("r.xml");
    SurefireReportOutput transform = createTransform(report.toString(), false);
    assertTrue(transform.init());

    Files.delete(folder);

    HopException e = assertThrows(HopException.class, () -> runToEnd(transform));
    assertEquals(expectedMessage("ParentFolderDoesNotExist", report), e.getMessage().trim());
  }

  private static String expectedMessage(String key, Path report) throws Exception {
    FileObject file = vfs(report);
    return BaseMessages.getString(
        PKG,
        "SurefireReportOutput.Error." + key,
        HopVfs.getFriendlyURI(file.getParent()),
        HopVfs.getFriendlyURI(file));
  }

  private static FileObject vfs(Path path) throws HopException {
    return HopVfs.getFileObject(path.toString());
  }

  private SurefireReportOutput createTransform(String filename, boolean createParentFolder) {
    SurefireReportOutputMeta meta = new SurefireReportOutputMeta();
    meta.setDefault();
    meta.setFilename(filename);
    meta.setSuiteName("suite");
    meta.setCreateParentFolder(createParentFolder);
    meta.setTestNameField("testName");
    meta.setDurationField("");
    meta.setResultField("");
    meta.setSystemOutField("");
    meta.setSystemErrField("");
    meta.setFailureMessageField("");
    meta.setFailureTypeField("");

    return new SurefireReportOutput(
        helper.transformMeta,
        meta,
        new SurefireReportOutputData(),
        0,
        helper.pipelineMeta,
        helper.pipeline);
  }

  /** Feeds one test-result row and the end of the stream, which writes the report. */
  private void runToEnd(SurefireReportOutput transform) throws HopException {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("testName"));
    IRowSet rowSet = helper.getMockInputRowSet(new Object[] {"main-0001-test.hwf"});
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    transform.addRowSetToInputRowSets(rowSet);

    while (transform.processRow()) {
      // one row, then the end of the stream writes the report
    }
  }
}
