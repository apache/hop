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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SurefireReportOutputTest {

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
  void missingFolderFailsWhenCreateParentFolderIsOff() {
    Path report = tempDir.resolve("nofolder/a/b/r.xml");

    HopException e = assertThrows(HopException.class, () -> runTransform(report, false));

    assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
    assertFalse(Files.exists(tempDir.resolve("nofolder")), "no folder may be created");
  }

  @Test
  void missingFolderIsCreatedWhenCreateParentFolderIsOn() throws Exception {
    Path report = tempDir.resolve("nofolder/a/b/r.xml");

    runTransform(report, true);

    assertTrue(Files.readString(report, StandardCharsets.UTF_8).contains("tests=\"1\""));
  }

  @Test
  void existingFolderIsWrittenWhenCreateParentFolderIsOff() throws Exception {
    Path report = tempDir.resolve("r.xml");

    runTransform(report, false);

    assertTrue(Files.readString(report, StandardCharsets.UTF_8).contains("tests=\"1\""));
  }

  /** Feeds one test-result row and the end of the stream, which writes the report. */
  private void runTransform(Path report, boolean createParentFolder) throws HopException {
    SurefireReportOutputMeta meta = new SurefireReportOutputMeta();
    meta.setDefault();
    meta.setFilename(report.toString());
    meta.setSuiteName("suite");
    meta.setCreateParentFolder(createParentFolder);
    meta.setTestNameField("testName");
    meta.setDurationField("");
    meta.setResultField("");
    meta.setSystemOutField("");
    meta.setSystemErrField("");
    meta.setFailureMessageField("");
    meta.setFailureTypeField("");

    SurefireReportOutput transform =
        new SurefireReportOutput(
            helper.transformMeta,
            meta,
            new SurefireReportOutputData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    assertTrue(transform.init());

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
