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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateIdentity;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.replay.manifest.ActionSnapshotManifest;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

@ExtensionPoint(
    id = "WorkflowReplayGateAfterActionXp",
    description = "Record Replay Gate manifest after action execution on every workflow engine",
    extensionPointId = "WorkflowAfterActionExecution")
public class WorkflowReplayGateAfterActionXp
    implements IExtensionPoint<WorkflowExecutionExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, WorkflowExecutionExtension extension)
      throws HopException {
    if (extension == null
        || extension.actionMeta == null
        || extension.workflow == null
        || !extension.executeAction) {
      return;
    }

    ActionMeta actionMeta = extension.actionMeta;
    ReplayGate gate = ReplayGateUtil.getReplayGate(actionMeta);
    Map<String, String> changedVariables =
        ReplayGateIdentity.changedVariables(extension.workflow, actionMeta);
    if (gate == null || !gate.isEnabled()) {
      return;
    }

    String actionName = actionMeta.getName();
    IWorkflowEngine<?> workflow = extension.workflow;
    String executionId = ReplayGateIdentity.executionId(workflow);
    String targetFolder =
        ReplayGateUtil.getActionSpoolPath(
            workflow, gate, workflow.getWorkflowMeta().getName(), executionId, actionName);

    Result result = extension.actionExecutionResult;
    boolean success =
        result != null
            && result.getResult()
            && result.getNrErrors() == 0
            && !result.isStopped()
            && !workflow.isStopped();
    if (!success) {
      deleteManifest(log, workflow, targetFolder, actionName);
      return;
    }

    ActionSnapshotManifest manifest = new ActionSnapshotManifest();
    manifest.setManifestId(UUID.randomUUID().toString());
    manifest.setWorkflowName(workflow.getWorkflowMeta().getName());
    manifest.setActionName(actionName);
    manifest.setExecutionId(executionId);
    manifest.setActionType(
        actionMeta.getAction() != null ? actionMeta.getAction().getPluginId() : "unknown");
    manifest.setExecutionDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
    manifest.setStatus(ActionSnapshotManifest.STATUS_SEALED);
    manifest.setConfigurationHash(ReplayGateIdentity.configurationHash(workflow, actionMeta));
    manifest.setVariables(changedVariables);
    manifest.setElapsedTimeMillis(result.getElapsedTimeMillis());
    manifest.setResult(result.getResult());
    manifest.setNrErrors(result.getNrErrors());
    manifest.setNrLinesInput(result.getNrLinesInput());
    manifest.setNrLinesOutput(result.getNrLinesOutput());
    manifest.setNrLinesRead(result.getNrLinesRead());
    manifest.setNrLinesWritten(result.getNrLinesWritten());
    manifest.setNrLinesUpdated(result.getNrLinesUpdated());
    manifest.setNrLinesRejected(result.getNrLinesRejected());
    manifest.setExitStatus(result.getExitStatus());
    try {
      manifest.setResultXml(result.getXml());
    } catch (Exception e) {
      log.logError("Error serializing result XML for " + actionName, e);
    }

    try {
      FileObject folderObj = HopVfs.getFileObject(targetFolder, workflow);
      if (!folderObj.exists()) {
        folderObj.createFolder();
      }
      FileObject manifestFile =
          HopVfs.getFileObject(targetFolder + "/action_manifest.json", workflow);
      try (OutputStream out = HopVfs.getOutputStream(manifestFile, false)) {
        out.write(manifest.toJson().getBytes(StandardCharsets.UTF_8));
      }
      log.logBasic(
          "Replay Gate ["
              + actionName
              + "]: sealed execution "
              + executionId
              + " at "
              + targetFolder);
    } catch (Exception e) {
      log.logError(
          "Replay Gate ["
              + actionName
              + "]: the action succeeded but its manifest could not be written. Replay will run it again.",
          e);
    }
  }

  private void deleteManifest(
      ILogChannel log, IWorkflowEngine<?> workflow, String targetFolder, String actionName) {
    try {
      FileObject manifestFile =
          HopVfs.getFileObject(targetFolder + "/action_manifest.json", workflow);
      if (manifestFile.exists()) {
        manifestFile.delete();
        log.logBasic(
            "Replay Gate ["
                + actionName
                + "]: removed the sealed manifest because the action did not complete successfully.");
      }
    } catch (Exception e) {
      log.logError("Replay Gate [" + actionName + "]: unable to remove the sealed manifest", e);
    }
  }
}
