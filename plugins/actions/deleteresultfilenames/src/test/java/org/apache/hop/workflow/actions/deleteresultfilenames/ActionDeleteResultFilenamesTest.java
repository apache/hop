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

package org.apache.hop.workflow.actions.deleteresultfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link ActionDeleteResultFilenames} */
class ActionDeleteResultFilenamesTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionDeleteResultFilenames action =
        ActionSerializationTestUtil.testSerialization(
            "/action-delete-result-files.xml", ActionDeleteResultFilenames.class);

    assertTrue(action.isSpecifyWildcard());
    assertEquals("wildcard", action.getWildcard());
    assertEquals("exclude", action.getWildcardExclude());
  }

  @Test
  void checkDoesNotReportAMissingFilename() {
    ActionDeleteResultFilenames action = new ActionDeleteResultFilenames();
    List<ICheckResult> remarks = new ArrayList<>();

    action.check(remarks, null, action, null);

    assertTrue(remarks.isEmpty());
  }

  @Test
  void matchingResultFileIsRemovedEvenWhenItNoLongerExists() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.setStopped(false);
    ActionDeleteResultFilenames action = new ActionDeleteResultFilenames();
    action.setParentWorkflow(workflow);
    action.setSpecifyWildcard(true);
    action.setWildcard(".*\\.csv");

    Result result = new Result();
    addResultFile(result, tempDir.resolve("deleted-earlier.csv"));
    addResultFile(result, Files.createFile(tempDir.resolve("kept.txt")));

    result = action.execute(result, 0);

    assertTrue(result.isResult());
    assertEquals(1, result.getResultFiles().size());
    assertTrue(result.getResultFiles().keySet().iterator().next().endsWith("kept.txt"));
  }

  private static void addResultFile(Result result, Path path) throws Exception {
    ResultFile resultFile =
        new ResultFile(
            ResultFile.FILE_TYPE_GENERAL,
            HopVfs.getFileObject(path.toString()),
            "workflow",
            "action");
    result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
  }
}
