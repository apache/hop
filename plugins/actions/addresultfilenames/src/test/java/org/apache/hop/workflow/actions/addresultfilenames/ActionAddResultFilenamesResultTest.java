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

package org.apache.hop.workflow.actions.addresultfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Result behaviour for Add filenames to result (issue #9264). */
class ActionAddResultFilenamesResultTest {

  @TempDir Path testFolder;

  private ActionAddResultFilenames action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionAddResultFilenames();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    workflow.setStopped(false);
    action.setArgFromPrevious(false);
    action.setDeleteAllBefore(true);
    action.setIncludeSubFolders(false);
  }

  /** #9264: invalid regex must fail the action, not succeed with zero result files. */
  @Test
  void invalidRegexWildcardFailsTheAction() throws Exception {
    Path folder = Files.createDirectories(testFolder.resolve("ar"));
    Files.writeString(folder.resolve("a.txt"), "a", StandardCharsets.UTF_8);
    Files.writeString(folder.resolve("b.log"), "b", StandardCharsets.UTF_8);

    action.setArguments(List.of(new Argument(folder.toString(), "([")));

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult(), "invalid regex must not succeed");
    assertTrue(result.getNrErrors() > 0);
    assertEquals(0, result.getResultFiles().size(), "no files must be added for an invalid mask");
  }

  @Test
  void validRegexAddsMatchingFiles() throws Exception {
    Path folder = Files.createDirectories(testFolder.resolve("ar"));
    Files.writeString(folder.resolve("a.txt"), "a", StandardCharsets.UTF_8);
    Files.writeString(folder.resolve("b.log"), "b", StandardCharsets.UTF_8);

    action.setArguments(List.of(new Argument(folder.toString(), ".*\\.txt")));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getResultFiles().size());
    assertTrue(
        result.getResultFiles().keySet().stream().anyMatch(k -> k.endsWith("a.txt")),
        "only a.txt should match");
  }
}
