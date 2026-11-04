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

package org.apache.hop.workflow.actions.deletefiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Result behaviour for Delete files (issue #9175 cases 3 and 4). */
class ActionDeleteFilesResultTest {

  @TempDir Path testFolder;

  private ActionDeleteFiles action;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopLogStore.init();
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    action = new ActionDeleteFiles();
    IWorkflowEngine<WorkflowMeta> parentWorkflow = mock(Workflow.class);
    doReturn(false).when(parentWorkflow).isStopped();
    doReturn(LogLevel.BASIC).when(parentWorkflow).getLogLevel();
    action.setParentWorkflow(parentWorkflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setIncludeSubfolders(false);
  }

  /** #9175 case 3: invalid regex must fail the action, not succeed with zero deletes. */
  @Test
  void invalidRegexWildcardFailsTheAction() throws Exception {
    Path folder = Files.createDirectories(testFolder.resolve("folder"));
    Path keep = folder.resolve("keep-me.txt");
    Files.writeString(keep, "should remain", StandardCharsets.UTF_8);

    action.setArgFromPrevious(false);
    action.setFileItems(List.of(new FileItem(folder.toString(), "[")));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "invalid regex must not succeed");
    assertTrue(result.getNrErrors() > 0);
    assertTrue(Files.exists(keep), "file must not be deleted when the mask is invalid");
  }

  /** #9175 case 4: a previous result row with fewer than two fields must fail the action. */
  @Test
  void previousRowWithFewerThanTwoFieldsFailsTheAction() throws Exception {
    action.setArgFromPrevious(true);

    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString("path_only"));
    Result previous = new Result();
    previous.setRows(List.of(new RowMetaAndData(meta, testFolder.resolve("folder").toString())));
    previous.setResult(true);

    Result result = action.execute(previous, 0);

    assertFalse(result.isResult(), "short previous row must fail Delete files");
    assertEquals(1, result.getNrErrors());
  }
}
