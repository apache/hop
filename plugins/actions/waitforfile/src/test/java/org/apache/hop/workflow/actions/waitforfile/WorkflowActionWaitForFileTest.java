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
package org.apache.hop.workflow.actions.waitforfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link ActionWaitForFile} */
class WorkflowActionWaitForFileTest extends WorkflowActionLoadSaveTestSupport<ActionWaitForFile> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @Override
  protected Class<ActionWaitForFile> getActionClass() {
    return ActionWaitForFile.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList(
        "filename",
        "maximumTimeout",
        "checkCycleTime",
        "successOnTimeout",
        "fileSizeCheck",
        "addFilenameToResult");
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
        "filename", "getFilename",
        "maximumTimeout", "getMaximumTimeout",
        "checkCycleTime", "getCheckCycleTime",
        "successOnTimeout", "isSuccessOnTimeout",
        "fileSizeCheck", "isFileSizeCheck",
        "addFilenameToResult", "isAddFilenameToResult");
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
        "filename", "setFilename",
        "maximumTimeout", "setMaximumTimeout",
        "checkCycleTime", "setCheckCycleTime",
        "successOnTimeout", "setSuccessOnTimeout",
        "fileSizeCheck", "setFileSizeCheck",
        "addFilenameToResult", "setAddFilenameToResult");
  }

  @Test
  void unreadableZipPathIsFalseWithOneError() throws Exception {
    Path fakeZip =
        Files.writeString(
            tempDir.resolve("not-a-zip.zip"),
            "this is plain text, not a zip",
            StandardCharsets.UTF_8);

    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.setStopped(false);

    ActionWaitForFile action = new ActionWaitForFile();
    action.setParentWorkflow(workflow);
    action.setFilename("zip:" + fakeZip.toAbsolutePath() + "!/inside.txt");
    action.setMaximumTimeout("5");
    action.setCheckCycleTime("1");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }
}
