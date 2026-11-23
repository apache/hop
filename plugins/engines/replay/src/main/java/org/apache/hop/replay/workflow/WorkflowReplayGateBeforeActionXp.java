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

package org.apache.hop.replay.workflow;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateIdentity;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.replay.engine.ReplayWorkflowEngine;
import org.apache.hop.replay.engine.ReplayWorkflowRunConfiguration;
import org.apache.hop.replay.manifest.ActionSnapshotManifest;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.action.ActionMeta;
import org.w3c.dom.Node;

@ExtensionPoint(
    id = "WorkflowReplayGateBeforeActionXp",
    description = "Evaluate Replay Gate before action execution",
    extensionPointId = "WorkflowBeforeActionExecution")
public class WorkflowReplayGateBeforeActionXp
    implements IExtensionPoint<WorkflowExecutionExtension> {
  private static final String CONST_REPLAY_WORKFLOW_ENGINE_ACTION =
      "ReplayWorkflowEngine: action [";

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, WorkflowExecutionExtension extension)
      throws HopException {
    if (extension == null || extension.actionMeta == null || extension.workflow == null) {
      return;
    }
    try {
      if (extension.workflow instanceof ReplayWorkflowEngine replayEngine
          && replayEngine.isReplaying()) {
        applyReplayDecision(log, replayEngine, extension);
      }
    } catch (Exception e) {
      log.logError(
          "Error evaluating action snapshot manifest for " + extension.actionMeta.getName(), e);
      extension.executeAction = true;
      if (extension.workflow instanceof ReplayWorkflowEngine replayEngine) {
        replayEngine.markLiveExecution();
      }
    } finally {
      if (extension.executeAction) {
        ReplayGateIdentity.snapshotVariables(extension.workflow, extension.actionMeta);
      }
    }
  }

  private void applyReplayDecision(
      ILogChannel log, ReplayWorkflowEngine replayEngine, WorkflowExecutionExtension extension)
      throws Exception {
    ActionMeta actionMeta = extension.actionMeta;
    String actionName = actionMeta.getName();

    if (actionMeta.isStart()) {
      extension.executeAction = true;
      return;
    }

    if (replayEngine.isLiveExecution()) {
      extension.executeAction = true;
      return;
    }

    ReplayWorkflowRunConfiguration replayConfig = replayEngine.getReplayWorkflowRunConfiguration();
    String startActionName =
        replayConfig == null ? null : replayEngine.resolve(replayConfig.getStartActionName());
    if (StringUtils.isNotEmpty(startActionName) && startActionName.equals(actionName)) {
      replayEngine.markLiveExecution();
      extension.executeAction = true;
      log.logBasic(
          "ReplayWorkflowEngine: running ["
              + actionName
              + "] because it is the configured start action. The rest of this path runs as well.");
      return;
    }

    ReplayGate gate = ReplayGateUtil.getReplayGate(actionMeta);
    if (gate != null && gate.isEnabled() && restoreSealedGate(log, replayEngine, extension, gate)) {
      return;
    }

    replayEngine.markLiveExecution();
    extension.executeAction = true;
    log.logBasic(
        CONST_REPLAY_WORKFLOW_ENGINE_ACTION
            + actionName
            + "] has no matching sealed gate. This path runs from here.");
  }

  private boolean restoreSealedGate(
      ILogChannel log,
      ReplayWorkflowEngine replayEngine,
      WorkflowExecutionExtension extension,
      ReplayGate gate)
      throws Exception {
    String actionName = extension.actionMeta.getName();
    String executionId = ReplayGateIdentity.executionId(replayEngine);
    String targetFolder =
        ReplayGateUtil.getActionSpoolPath(
            replayEngine, gate, replayEngine.getWorkflowMeta().getName(), executionId, actionName);
    FileObject manifestFile =
        HopVfs.getFileObject(targetFolder + "/action_manifest.json", replayEngine);
    if (!manifestFile.exists()) {
      return false;
    }
    ActionSnapshotManifest manifest;
    try (InputStream in = HopVfs.getInputStream(manifestFile)) {
      manifest = ActionSnapshotManifest.fromJson(IOUtils.toString(in, StandardCharsets.UTF_8));
    }
    if (!ActionSnapshotManifest.STATUS_SEALED.equals(manifest.getStatus())) {
      return false;
    }
    String expectedHash = ReplayGateIdentity.configurationHash(replayEngine, extension.actionMeta);
    if (StringUtils.isEmpty(manifest.getConfigurationHash())
        || !manifest.getConfigurationHash().equals(expectedHash)) {
      log.logBasic(
          CONST_REPLAY_WORKFLOW_ENGINE_ACTION
              + actionName
              + "] configuration, parameters or referenced variables changed since the sealed gate. Re-executing.");
      return false;
    }

    extension.executeAction = false;
    Result restoredResult = restoredResult(log, actionName, manifest);
    extension.result = restoredResult;
    if (manifest.getVariables() != null) {
      for (Map.Entry<String, String> entry : manifest.getVariables().entrySet()) {
        replayEngine.setVariable(entry.getKey(), entry.getValue());
      }
    }
    replayEngine.recordRestoredAction(extension.actionMeta, restoredResult);
    log.logBasic(
        CONST_REPLAY_WORKFLOW_ENGINE_ACTION
            + actionName
            + "] matches the sealed gate for execution "
            + executionId
            + ". Skipping it and restoring the result.");
    return true;
  }

  private Result restoredResult(
      ILogChannel log, String actionName, ActionSnapshotManifest manifest) {
    if (StringUtils.isNotEmpty(manifest.getResultXml())) {
      try {
        Node resultNode = XmlHandler.loadXmlString(manifest.getResultXml(), Result.XML_TAG);
        return new Result(resultNode);
      } catch (Exception e) {
        log.logError(
            "Error deserializing result XML for " + actionName + ", falling back to basic result",
            e);
      }
    }
    Result basic = new Result();
    basic.setResult(manifest.isResult());
    basic.setNrErrors(manifest.getNrErrors());
    basic.setNrLinesInput(manifest.getNrLinesInput());
    basic.setNrLinesOutput(manifest.getNrLinesOutput());
    basic.setNrLinesRead(manifest.getNrLinesRead());
    basic.setNrLinesWritten(manifest.getNrLinesWritten());
    basic.setNrLinesUpdated(manifest.getNrLinesUpdated());
    basic.setNrLinesRejected(manifest.getNrLinesRejected());
    basic.setExitStatus((int) manifest.getExitStatus());
    return basic;
  }
}
