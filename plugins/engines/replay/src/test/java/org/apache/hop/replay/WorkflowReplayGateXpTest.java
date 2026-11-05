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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.replay.engine.ReplayWorkflowEngine;
import org.apache.hop.replay.engine.ReplayWorkflowRunConfiguration;
import org.apache.hop.replay.manifest.ActionSnapshotManifest;
import org.apache.hop.replay.workflow.WorkflowReplayGateAfterActionXp;
import org.apache.hop.replay.workflow.WorkflowReplayGateBeforeActionXp;
import org.apache.hop.replay.workflow.WorkflowReplayGateFinishedXp;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
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
  void localRunSealsAndReplayRestoresResultFilesAndVariables() throws Exception {
    String spoolDir = tempFolder.resolve("workflow-spool").toUri().toString();
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("TestWorkflow");

    ActionDummy action = new ActionDummy("ActionOne");
    action.setDescription("load ${COUNTRY}");
    ActionMeta actionMeta = new ActionMeta(action);
    workflowMeta.addAction(actionMeta);
    enableGate(actionMeta, spoolDir);

    LocalWorkflowEngine local = new LocalWorkflowEngine(workflowMeta);
    local.setVariable("COUNTRY", "BE");
    local.addParameterDefinition("COUNTRY", "BE", "country");
    local.setParameterValue("COUNTRY", "BE");

    WorkflowReplayGateBeforeActionXp beforeXp = new WorkflowReplayGateBeforeActionXp();
    WorkflowReplayGateAfterActionXp afterXp = new WorkflowReplayGateAfterActionXp();
    beforeXp.callExtensionPoint(
        LogChannel.GENERAL, local, new WorkflowExecutionExtension(local, null, actionMeta, true));
    local.setVariable("LOADED", "yes");

    Result successResult = new Result();
    successResult.setResult(true);
    successResult.setNrErrors(0L);
    successResult.setNrLinesOutput(42L);
    FileObject testFile = HopVfs.getFileObject(tempFolder.resolve("test.bin").toUri().toString());
    ResultFile resultFile =
        new ResultFile(ResultFile.FILE_TYPE_GENERAL, testFile, "TestWorkflow", "ActionOne");
    successResult.getResultFiles().put(resultFile.getFile().toString(), resultFile);

    WorkflowExecutionExtension afterExt =
        new WorkflowExecutionExtension(local, null, actionMeta, true);
    afterExt.actionExecutionResult = successResult;
    afterXp.callExtensionPoint(LogChannel.GENERAL, local, afterExt);

    String actionSpoolPath =
        ReplayGateUtil.getActionSpoolPath(
            local,
            ReplayGateUtil.getReplayGate(actionMeta),
            "TestWorkflow",
            local.getLogChannelId(),
            "ActionOne");
    FileObject manifestFile =
        HopVfs.getFileObject(actionSpoolPath + "/action_manifest.json", local);
    assertTrue(manifestFile.exists());
    ActionSnapshotManifest sealed =
        ActionSnapshotManifest.fromJson(
            IOUtils.toString(HopVfs.getInputStream(manifestFile), StandardCharsets.UTF_8));
    assertEquals(ActionSnapshotManifest.STATUS_SEALED, sealed.getStatus());
    assertEquals("yes", sealed.getVariables().get("LOADED"));
    assertEquals(
        ReplayGateIdentity.configurationHash(local, actionMeta), sealed.getConfigurationHash());

    ReplayWorkflowEngine replay = replayEngine(workflowMeta, local.getLogChannelId(), null);
    replay.setVariable("COUNTRY", "BE");
    replay.addParameterDefinition("COUNTRY", "BE", "country");
    replay.setParameterValue("COUNTRY", "BE");
    WorkflowExecutionExtension beforeExt =
        new WorkflowExecutionExtension(replay, null, actionMeta, true);
    beforeXp.callExtensionPoint(LogChannel.GENERAL, replay, beforeExt);

    assertFalse(beforeExt.executeAction);
    assertNotNull(beforeExt.result);
    assertTrue(beforeExt.result.getResult());
    assertEquals(42L, beforeExt.result.getNrLinesOutput());
    assertEquals(1, beforeExt.result.getResultFiles().size());
    assertEquals("yes", replay.getVariable("LOADED"));
    assertTrue(
        replay.getActionResults().stream()
            .anyMatch(
                result -> "ActionOne".equals(result.getActionName()) && result.isCheckpoint()));
    assertFalse(replay.isLiveExecution());
  }

  @Test
  void changedParameterInvalidatesTheGateAndMarksThePathLive() throws Exception {
    String spoolDir = tempFolder.resolve("param-spool").toUri().toString();
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("Params");
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("Load"));
    workflowMeta.addAction(actionMeta);
    enableGate(actionMeta, spoolDir);

    LocalWorkflowEngine local = new LocalWorkflowEngine(workflowMeta);
    local.addParameterDefinition("COUNTRY", "BE", "");
    local.setParameterValue("COUNTRY", "BE");
    seal(local, actionMeta);

    ReplayWorkflowEngine replay = replayEngine(workflowMeta, local.getLogChannelId(), null);
    replay.addParameterDefinition("COUNTRY", "BE", "");
    replay.setParameterValue("COUNTRY", "NL");
    WorkflowExecutionExtension beforeExt =
        new WorkflowExecutionExtension(replay, null, actionMeta, true);
    new WorkflowReplayGateBeforeActionXp()
        .callExtensionPoint(LogChannel.GENERAL, replay, beforeExt);

    assertTrue(beforeExt.executeAction);
    assertTrue(replay.isLiveExecution());
  }

  @Test
  void anotherExecutionIdDoesNotReuseTheGate() throws Exception {
    String spoolDir = tempFolder.resolve("id-spool").toUri().toString();
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("Ids");
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("Load"));
    workflowMeta.addAction(actionMeta);
    enableGate(actionMeta, spoolDir);

    LocalWorkflowEngine local = new LocalWorkflowEngine(workflowMeta);
    seal(local, actionMeta);

    ReplayWorkflowEngine replay = replayEngine(workflowMeta, "some-other-execution", null);
    WorkflowExecutionExtension beforeExt =
        new WorkflowExecutionExtension(replay, null, actionMeta, true);
    new WorkflowReplayGateBeforeActionXp()
        .callExtensionPoint(LogChannel.GENERAL, replay, beforeExt);

    assertTrue(beforeExt.executeAction);
  }

  @Test
  void startActionNameIsCaseSensitive() throws Exception {
    String spoolDir = tempFolder.resolve("case-spool").toUri().toString();
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("Case");
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("Load"));
    workflowMeta.addAction(actionMeta);
    enableGate(actionMeta, spoolDir);

    LocalWorkflowEngine local = new LocalWorkflowEngine(workflowMeta);
    seal(local, actionMeta);

    ReplayWorkflowEngine lower = replayEngine(workflowMeta, local.getLogChannelId(), "load");
    WorkflowExecutionExtension lowerExt =
        new WorkflowExecutionExtension(lower, null, actionMeta, true);
    new WorkflowReplayGateBeforeActionXp().callExtensionPoint(LogChannel.GENERAL, lower, lowerExt);
    assertFalse(lowerExt.executeAction);

    ReplayWorkflowEngine exact = replayEngine(workflowMeta, local.getLogChannelId(), "Load");
    WorkflowExecutionExtension exactExt =
        new WorkflowExecutionExtension(exact, null, actionMeta, true);
    new WorkflowReplayGateBeforeActionXp().callExtensionPoint(LogChannel.GENERAL, exact, exactExt);
    assertTrue(exactExt.executeAction);
    assertTrue(exact.isLiveExecution());
  }

  @Test
  void successfulWorkflowDeletesTheExecutionSpool() throws Exception {
    String spoolDir = tempFolder.resolve("done-spool").toUri().toString();
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("Done");
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("Load"));
    workflowMeta.addAction(actionMeta);
    enableGate(actionMeta, spoolDir);

    LocalWorkflowEngine local = new LocalWorkflowEngine(workflowMeta);
    seal(local, actionMeta);
    String folder =
        ReplayGateUtil.getExecutionSpoolPath(
            local, ReplayGateUtil.getReplayGate(actionMeta), "Done", local.getLogChannelId());
    assertTrue(HopVfs.getFileObject(folder, local).exists());

    Result success = new Result();
    success.setResult(true);
    local.setResult(success);
    new WorkflowReplayGateFinishedXp().callExtensionPoint(LogChannel.GENERAL, local, local);

    assertFalse(HopVfs.getFileObject(folder, local).exists());
  }

  private static void enableGate(ActionMeta actionMeta, String spoolDir) {
    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory(spoolDir);
    ReplayGateUtil.storeReplayGate(actionMeta, gate);
  }

  private static void seal(LocalWorkflowEngine local, ActionMeta actionMeta) throws Exception {
    WorkflowReplayGateBeforeActionXp beforeXp = new WorkflowReplayGateBeforeActionXp();
    beforeXp.callExtensionPoint(
        LogChannel.GENERAL, local, new WorkflowExecutionExtension(local, null, actionMeta, true));
    Result result = new Result();
    result.setResult(true);
    WorkflowExecutionExtension afterExt =
        new WorkflowExecutionExtension(local, null, actionMeta, true);
    afterExt.actionExecutionResult = result;
    new WorkflowReplayGateAfterActionXp().callExtensionPoint(LogChannel.GENERAL, local, afterExt);
  }

  private static ReplayWorkflowEngine replayEngine(
      WorkflowMeta workflowMeta, String sourceExecutionId, String startActionName) {
    ReplayWorkflowEngine replay = new ReplayWorkflowEngine(workflowMeta);
    ReplayWorkflowRunConfiguration config = new ReplayWorkflowRunConfiguration();
    config.setSourceExecutionId(sourceExecutionId);
    config.setStartActionName(startActionName);
    replay.setWorkflowRunConfiguration(
        new WorkflowRunConfiguration("replay", "", "", config, false));
    replay.setReplaying(true);
    return replay;
  }
}
