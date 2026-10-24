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

package org.apache.hop.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.replay.engine.ReplayWorkflowEngine;
import org.apache.hop.replay.workflow.WorkflowReplayGateAfterActionXp;
import org.apache.hop.replay.workflow.WorkflowReplayGateBeforeActionXp;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkflowReplayGateXpTest {

  @TempDir Path tempFolder;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testAfterActionRecordsManifestAndBeforeActionSkipsOnReplay() throws Exception {
    String spoolDir = tempFolder.resolve("workflow-spool").toUri().toString();
    ILogChannel log = mock(ILogChannel.class);

    // Setup workflow and metadata
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("TestWorkflow");

    Map<String, Map<String, String>> attributesMap = workflowMeta.getAttributesMap();
    Map<String, String> replayGroup = ReplayGateUtil.getOrCreateReplayGroup(attributesMap);

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory(spoolDir);
    ReplayGateUtil.storeReplayGate(replayGroup, "ActionOne", gate);

    ReplayWorkflowEngine workflowEngine = new ReplayWorkflowEngine(workflowMeta);

    // Action 1: ActionOne
    IAction actionOne = mock(IAction.class);
    when(actionOne.getName()).thenReturn("ActionOne");
    when(actionOne.getPluginId()).thenReturn("PIPELINE");
    ActionMeta actionMetaOne = new ActionMeta(actionOne);
    actionMetaOne.setName("ActionOne");

    Result successResult = new Result();
    successResult.setResult(true);
    successResult.setNrErrors(0L);
    successResult.setNrLinesOutput(42L);
    FileObject testFile = HopVfs.getFileObject(tempFolder.resolve("test.bin").toUri().toString());
    ResultFile resultFile =
        new ResultFile(ResultFile.FILE_TYPE_GENERAL, testFile, "TestWorkflow", "ActionOne");
    successResult.getResultFiles().put(resultFile.getFile().toString(), resultFile);

    // Simulate ActionOne execution completing
    WorkflowExecutionExtension afterExt =
        new WorkflowExecutionExtension(workflowEngine, null, actionMetaOne, true);
    afterExt.actionExecutionResult = successResult;

    WorkflowReplayGateAfterActionXp afterXp = new WorkflowReplayGateAfterActionXp();
    afterXp.callExtensionPoint(log, workflowEngine, afterExt);

    // Verify action_manifest.json was written to spool
    String actionSpoolPath =
        ReplayGateUtil.getActionSpoolPath(workflowEngine, gate, "TestWorkflow", "ActionOne");
    FileObject manifestFile =
        HopVfs.getFileObject(actionSpoolPath + "/action_manifest.json", workflowEngine);
    assertTrue(manifestFile.exists());

    // Now simulate Run 2 (Replay) with ReplayWorkflowEngine
    ReplayWorkflowEngine replayRun = new ReplayWorkflowEngine(workflowMeta);
    replayRun.setReplaying(true);
    replayRun.setResumePointReached(false);

    WorkflowExecutionExtension beforeExt =
        new WorkflowExecutionExtension(replayRun, null, actionMetaOne, true);

    WorkflowReplayGateBeforeActionXp beforeXp = new WorkflowReplayGateBeforeActionXp();
    beforeXp.callExtensionPoint(log, replayRun, beforeExt);

    // Verify ActionOne is SKIPPED on replay because it has a SEALED gate!
    assertFalse(beforeExt.executeAction);
    assertNotNull(beforeExt.result);
    assertTrue(beforeExt.result.getResult());
    assertEquals(0L, beforeExt.result.getNrErrors());
    assertEquals(42L, beforeExt.result.getNrLinesOutput());
    assertEquals(1, beforeExt.result.getResultFiles().size());
  }

  @Test
  void testStartActionIsNotSkipped() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("TestWorkflow");

    ReplayWorkflowEngine replayRun = new ReplayWorkflowEngine(workflowMeta);
    replayRun.setReplaying(true);

    IAction startAction = mock(IAction.class);
    when(startAction.getName()).thenReturn("Start");
    when(startAction.isStart()).thenReturn(true);
    ActionMeta startMeta = new ActionMeta(startAction);
    startMeta.setName("Start");

    WorkflowExecutionExtension beforeExt =
        new WorkflowExecutionExtension(replayRun, null, startMeta, true);
    WorkflowReplayGateBeforeActionXp beforeXp = new WorkflowReplayGateBeforeActionXp();
    beforeXp.callExtensionPoint(mock(ILogChannel.class), replayRun, beforeExt);

    assertTrue(beforeExt.executeAction);
    assertFalse(replayRun.isResumePointReached());
  }
}
