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
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.replay.engine.ReplayWorkflowEngine;
import org.apache.hop.replay.engine.ReplayWorkflowRunConfiguration;
import org.apache.hop.replay.manifest.ActionSnapshotManifest;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.w3c.dom.Node;

@ExtensionPoint(
    id = "WorkflowReplayGateBeforeActionXp",
    description = "Evaluate Replay Gate before action execution",
    extensionPointId = "WorkflowBeforeActionExecution")
public class WorkflowReplayGateBeforeActionXp
    implements IExtensionPoint<WorkflowExecutionExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, WorkflowExecutionExtension extension)
      throws HopException {
    if (extension == null || extension.actionMeta == null || extension.workflow == null) {
      return;
    }

    if (!(extension.workflow instanceof ReplayWorkflowEngine replayEngine)) {
      return;
    }

    if (!replayEngine.isReplaying()) {
      return;
    }

    if (extension.actionMeta.isStart()) {
      extension.executeAction = true;
      return;
    }

    ReplayWorkflowRunConfiguration replayConfig = replayEngine.getReplayWorkflowRunConfiguration();
    String startActionName =
        replayConfig != null ? replayEngine.resolve(replayConfig.getStartActionName()) : null;
    String actionName = extension.actionMeta.getName();

    if (StringUtils.isNotEmpty(startActionName) && actionName.equalsIgnoreCase(startActionName)) {
      replayEngine.setResumePointReached(true);
      log.logBasic(
          "ReplayWorkflowEngine: Resuming active execution at designated start action ["
              + actionName
              + "].");
      extension.executeAction = true;
      return;
    }

    if (replayEngine.isResumePointReached()) {
      extension.executeAction = true;
      return;
    }

    Map<String, String> replayGroup =
        extension
            .workflow
            .getWorkflowMeta()
            .getAttributesMap()
            .get(ReplayDefaults.REPLAY_GATE_GROUP);
    ReplayGate gate = ReplayGateUtil.getActionReplayGate(replayGroup, actionName);

    if (gate != null && gate.isEnabled()) {
      String targetFolder =
          ReplayGateUtil.getActionSpoolPath(
              extension.workflow, gate, extension.workflow.getWorkflowMeta().getName(), actionName);
      try {
        FileObject manifestFile =
            HopVfs.getFileObject(targetFolder + "/action_manifest.json", extension.workflow);
        if (manifestFile.exists()) {
          try (InputStream in = HopVfs.getInputStream(manifestFile)) {
            String json = IOUtils.toString(in, StandardCharsets.UTF_8);
            ActionSnapshotManifest manifest = ActionSnapshotManifest.fromJson(json);
            if (ActionSnapshotManifest.STATUS_SEALED.equals(manifest.getStatus())) {
              // Check if the underlying action file has been modified since the snapshot was taken
              String filename =
                  extension.actionMeta.getAction() != null
                      ? extension.actionMeta.getAction().getFilename()
                      : null;
              if (StringUtils.isNotEmpty(filename)) {
                String resolvedFile = extension.workflow.resolve(filename);
                FileObject fileObj = HopVfs.getFileObject(resolvedFile, extension.workflow);
                if (fileObj.exists()) {
                  long lastMod = fileObj.getContent().getLastModifiedTime();
                  if (StringUtils.isNotEmpty(manifest.getExecutionDate())) {
                    long manifestTime =
                        java.time.Instant.parse(manifest.getExecutionDate()).toEpochMilli();
                    if (lastMod > manifestTime) {
                      log.logBasic(
                          "ReplayWorkflowEngine: Action ["
                              + actionName
                              + "] definition has been modified since last sealed snapshot. Re-executing action.");
                      replayEngine.setResumePointReached(true);
                      extension.executeAction = true;
                      return;
                    }
                  }
                }
              }

              extension.executeAction = false;
              Result restoredResult = null;
              if (StringUtils.isNotEmpty(manifest.getResultXml())) {
                try {
                  Node resultNode =
                      XmlHandler.loadXmlString(manifest.getResultXml(), Result.XML_TAG);
                  restoredResult = new Result(resultNode);
                } catch (Exception e) {
                  log.logError(
                      "Error deserializing result XML for "
                          + actionName
                          + ", falling back to basic result",
                      e);
                }
              }
              if (restoredResult == null) {
                restoredResult = createBasicResult(manifest);
              }
              extension.result = restoredResult;

              if (manifest.getVariables() != null) {
                for (Map.Entry<String, String> entry : manifest.getVariables().entrySet()) {
                  extension.workflow.setVariable(entry.getKey(), entry.getValue());
                }
              }

              log.logBasic(
                  "ReplayWorkflowEngine: Action ["
                      + actionName
                      + "] has a valid SEALED gate snapshot. Skipping execution and restoring result.");
              return;
            }
          }
        }
      } catch (Exception e) {
        log.logError("Error evaluating action snapshot manifest for " + actionName, e);
      }
    }

    if (StringUtils.isEmpty(startActionName)) {
      replayEngine.setResumePointReached(true);
      log.logBasic(
          "ReplayWorkflowEngine: Action ["
              + actionName
              + "] is not sealed. Resuming execution from this point.");
      extension.executeAction = true;
    }
  }

  private Result createBasicResult(ActionSnapshotManifest manifest) {
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
