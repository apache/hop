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

package org.apache.hop.workflow.actions.evalfilesmetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link ActionEvalFilesMetrics} */
class ActionEvalFilesMetricsTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionEvalFilesMetrics action =
        ActionSerializationTestUtil.testSerialization(
            "/action-evaluate-file-metrics.xml", ActionEvalFilesMetrics.class);

    assertEquals("wildcard", action.getResultFilenamesWildcard());
    assertEquals("file-result-field", action.getResultFieldFile());
    assertEquals("wildcard-result-field", action.getResultFieldWildcard());
    assertEquals("include-result-field", action.getResultFieldIncludeSubFolders());
    assertEquals("12345", action.getCompareValue());
    assertEquals("1000", action.getMinValue());
    assertEquals("9999", action.getMaxValue());
    assertEquals(
        ActionEvalFilesMetrics.SuccesConditionType.GREATER_EQUAL, action.getSuccessConditionType());
    assertEquals(
        ActionEvalFilesMetrics.SourceFilesType.PREVIOUS_RESULT, action.getSourceFilesType());
    assertEquals(ActionEvalFilesMetrics.EvaluationType.SIZE, action.getEvaluationType());
    assertEquals(ActionEvalFilesMetrics.Scale.BYTES, action.getScale());

    assertEquals(3, action.getSourceFiles().size());
    ActionEvalFilesMetrics.SourceFile f = action.getSourceFiles().getFirst();
    assertEquals("folder1", f.getSourceFileFolder());
    assertEquals("wildcard1", f.getSourceWildcard());
    assertEquals("Y", f.getSourceIncludeSubfolders());

    f = action.getSourceFiles().get(1);
    assertEquals("folder2", f.getSourceFileFolder());
    assertEquals("wildcard2", f.getSourceWildcard());
    assertEquals("N", f.getSourceIncludeSubfolders());

    f = action.getSourceFiles().getLast();
    assertEquals("folder3", f.getSourceFileFolder());
    assertEquals("wildcard3", f.getSourceWildcard());
    assertEquals("Y", f.getSourceIncludeSubfolders());
  }

  @Test
  void invalidResultFilenamesWildcardMakesActionFalseWithOneError() throws Exception {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    workflow.setStopped(false);

    ActionEvalFilesMetrics action = new ActionEvalFilesMetrics();
    action.setParentWorkflow(workflow);
    action.setSourceFilesType(ActionEvalFilesMetrics.SourceFilesType.FILENAMES_RESULT);
    action.setEvaluationType(ActionEvalFilesMetrics.EvaluationType.COUNT);
    action.setSuccessConditionType(ActionEvalFilesMetrics.SuccesConditionType.EQUAL);
    action.setCompareValue("1");
    action.setResultFilenamesWildcard("[");

    Result result = new Result();
    Path source = Files.writeString(tempDir.resolve("sales.csv"), "id\n1\n");
    ResultFile resultFile =
        new ResultFile(
            ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject(source.toString()), "wf", "action");
    result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

    result = action.execute(result, 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }
}
