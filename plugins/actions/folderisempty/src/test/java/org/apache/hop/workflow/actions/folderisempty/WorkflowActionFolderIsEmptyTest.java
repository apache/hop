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

package org.apache.hop.workflow.actions.folderisempty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowActionFolderIsEmptyTest {
  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionFolderIsEmpty action;

  private String emptyDir;
  private String nonEmptyDir;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionFolderIsEmpty();

    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    WorkflowMeta mockWorkflowMeta = mock(WorkflowMeta.class);
    action.setParentWorkflowMeta(mockWorkflowMeta);

    workflow.setStopped(false);

    File dir = Files.createTempDirectory("dir", new FileAttribute<?>[0]).toFile();
    dir.deleteOnExit();
    emptyDir = dir.getPath();

    dir = Files.createTempDirectory("dir", new FileAttribute<?>[0]).toFile();
    dir.deleteOnExit();
    nonEmptyDir = dir.getPath();

    File file = File.createTempFile("existingFile", "ext", dir);
    file.deleteOnExit();
  }

  @Test
  void testSetNrErrorsSuccess() {
    action.setFolderName(emptyDir);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult(), "For empty folder result should be true");
    assertEquals(0, result.getNrErrors(), "There should be no errors");
  }

  @Test
  void testSetNrErrorsFail() {
    action.setFolderName(nonEmptyDir);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult(), "For non-empty folder result should be false");
    assertEquals(0, result.getNrErrors(), "There should be still no errors");
  }
}
