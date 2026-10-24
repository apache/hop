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
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.replay.engine.ReplayWorkflowEngine;
import org.apache.hop.replay.manifest.ActionSnapshotManifest;
import org.apache.hop.workflow.WorkflowExecutionExtension;

@ExtensionPoint(
    id = "WorkflowReplayGateAfterActionXp",
    description = "Record Replay Gate manifest after action execution",
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
        || !extension.executeAction
        || !(extension.workflow instanceof ReplayWorkflowEngine)) {
      return;
    }

    String actionName = extension.actionMeta.getName();
    Map<String, String> replayGroup =
        extension
            .workflow
            .getWorkflowMeta()
            .getAttributesMap()
            .get(ReplayDefaults.REPLAY_GATE_GROUP);
    ReplayGate gate = ReplayGateUtil.getActionReplayGate(replayGroup, actionName);

    if (gate != null && gate.isEnabled()) {
      Result result = extension.actionExecutionResult;
      boolean success = result != null && result.getResult() && result.getNrErrors() == 0;
      String status =
          success ? ActionSnapshotManifest.STATUS_SEALED : ActionSnapshotManifest.STATUS_FAILED;

      ActionSnapshotManifest manifest = new ActionSnapshotManifest();
      manifest.setManifestId(UUID.randomUUID().toString());
      manifest.setWorkflowName(extension.workflow.getWorkflowMeta().getName());
      manifest.setActionName(actionName);
      manifest.setActionType(
          extension.actionMeta.getAction() != null
              ? extension.actionMeta.getAction().getPluginId()
              : "unknown");
      manifest.setExecutionDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
      manifest.setStatus(status);

      if (result != null) {
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
      }

      String targetFolder =
          ReplayGateUtil.getActionSpoolPath(
              extension.workflow, gate, extension.workflow.getWorkflowMeta().getName(), actionName);

      try {
        FileObject folderObj = HopVfs.getFileObject(targetFolder, extension.workflow);
        if (!folderObj.exists()) {
          folderObj.createFolder();
        }

        FileObject manifestFile =
            HopVfs.getFileObject(targetFolder + "/action_manifest.json", extension.workflow);
        String manifestJson = manifest.toJson();
        try (OutputStream out = HopVfs.getOutputStream(manifestFile, false)) {
          out.write(manifestJson.getBytes(StandardCharsets.UTF_8));
        }

        log.logBasic(
            "Replay Gate ["
                + actionName
                + "]: Action snapshot manifest written with status "
                + status
                + " to "
                + targetFolder);
      } catch (Exception e) {
        log.logError("Error writing action snapshot manifest for " + actionName, e);
        throw new HopException("Error writing action snapshot manifest for " + actionName, e);
      }
    }
  }
}
