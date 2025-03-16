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
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.utils.TestUtils;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WorkflowActionFilesExistTest {
  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionFilesExist action;

  private String existingFile1;
  private String existingFile2;

  @BeforeAll
  public static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  public void setUp() {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionFilesExist();

    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    WorkflowMeta mockWorkflowMeta = mock(WorkflowMeta.class);
    action.setParentWorkflowMeta(mockWorkflowMeta);

    workflow.setStopped(false);

    existingFile1 =
        TestUtils.createRamFile(getClass().getSimpleName() + "/existingFile1.ext", action);
    existingFile2 =
        TestUtils.createRamFile(getClass().getSimpleName() + "/existingFile2.ext", action);
  }

  @Test
  void testSerialization() throws Exception {
    ActionFilesExist meta =
        ActionSerializationTestUtil.testSerialization(
            "/files-exist-action.xml", ActionFilesExist.class);

    assertEquals("/folder", meta.getFileItems().get(0).getFileName());
    assertEquals("/archive.zip", meta.getFileItems().get(1).getFileName());
    assertEquals(2, meta.getFileItems().size());
  }

  @Test
  public void testSetNrErrorsFalseResult() {
    action.setFileItems(List.of(new FileItem("nonExistingFile.ext")));

    Result res = action.execute(new Result(), 0);

    assertFalse(res.getResult(), "Entry should fail");
    assertEquals(
        0,
        res.getNrErrors(),
        "Files not found. Result is false. But... No of errors should be zero");
  }

  @Test
  public void testExecuteWithException() {
    action.setFileItems(List.of(new FileItem(null)));

    Result res = action.execute(new Result(), 0);

    assertFalse(res.getResult(), "Action should fail");
    assertEquals(
        1, res.getNrErrors(), "File with wrong name was specified. One error should be reported");
  }

  @Test
  public void testExecuteSuccess() {
    action.setFileItems(List.of(new FileItem(existingFile1), new FileItem(existingFile2)));

    Result res = action.execute(new Result(), 0);
    assertTrue(res.getResult());
  }

  @Test
  public void testExecuteFail() {
    action.setFileItems(
        List.of(
            new FileItem(existingFile1),
            new FileItem(existingFile2),
            new FileItem("nonExistingFile1.ext"),
            new FileItem("nonExistingFile2.ext")));

    Result res = action.execute(new Result(), 0);
    assertFalse(res.getResult());
  }
}
