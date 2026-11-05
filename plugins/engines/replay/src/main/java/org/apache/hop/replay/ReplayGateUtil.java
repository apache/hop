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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

public class ReplayGateUtil {

  public static void storeReplayGate(ActionMeta actionMeta, ReplayGate gate) {
    if (actionMeta == null || gate == null) {
      return;
    }
    actionMeta.setAttribute(
        ReplayDefaults.REPLAY_GATE_GROUP,
        ReplayDefaults.GATE_ATTR_ENABLED,
        Boolean.toString(gate.isEnabled()));
    putOrRemove(
        actionMeta,
        ReplayDefaults.GATE_ATTR_SPOOL_DIR,
        StringUtils.trimToNull(gate.getSpoolDirectory()));
    putOrRemove(
        actionMeta,
        ReplayDefaults.GATE_ATTR_DESCRIPTION,
        StringUtils.trimToNull(gate.getDescription()));
  }

  public static ReplayGate getReplayGate(ActionMeta actionMeta) {
    if (actionMeta == null) {
      return null;
    }
    String enabled =
        actionMeta.getAttribute(ReplayDefaults.REPLAY_GATE_GROUP, ReplayDefaults.GATE_ATTR_ENABLED);
    if (StringUtils.isEmpty(enabled)) {
      return null;
    }
    ReplayGate gate = new ReplayGate();
    gate.setEnabled(Const.toBoolean(enabled));
    gate.setSpoolDirectory(
        Const.NVL(
            actionMeta.getAttribute(
                ReplayDefaults.REPLAY_GATE_GROUP, ReplayDefaults.GATE_ATTR_SPOOL_DIR),
            ""));
    gate.setDescription(
        Const.NVL(
            actionMeta.getAttribute(
                ReplayDefaults.REPLAY_GATE_GROUP, ReplayDefaults.GATE_ATTR_DESCRIPTION),
            ""));
    return gate;
  }

  public static void clearReplayGate(ActionMeta actionMeta) {
    if (actionMeta == null || actionMeta.getAttributesMap() == null) {
      return;
    }
    actionMeta.getAttributesMap().remove(ReplayDefaults.REPLAY_GATE_GROUP);
  }

  public static String resolveSpoolDirectory(IVariables variables, ReplayGate gate) {
    String spoolDir =
        (gate != null && StringUtils.isNotEmpty(gate.getSpoolDirectory()))
            ? gate.getSpoolDirectory()
            : ReplayDefaults.DEFAULT_SPOOL_DIR;
    if (variables != null) {
      spoolDir = variables.resolve(spoolDir);
    }
    if (spoolDir.contains("${PROJECT_HOME}")) {
      String projectHome = variables != null ? variables.getVariable("PROJECT_HOME") : null;
      if (StringUtils.isEmpty(projectHome)) {
        projectHome = System.getProperty("user.dir");
      }
      spoolDir = spoolDir.replace("${PROJECT_HOME}", projectHome);
    }
    if (spoolDir.endsWith("/") || spoolDir.endsWith("\\")) {
      spoolDir = spoolDir.substring(0, spoolDir.length() - 1);
    }
    return spoolDir;
  }

  public static String sanitizeName(String name) {
    if (name == null || name.trim().isEmpty()) {
      return "unnamed";
    }
    return name.trim().replaceAll("[/\\\\:*?\"<>|]", "_");
  }

  public static String getExecutionSpoolPath(
      IVariables variables, ReplayGate gate, String workflowName, String executionId) {
    String baseDir = resolveSpoolDirectory(variables, gate);
    return baseDir + "/workflows/" + sanitizeName(workflowName) + "/" + sanitizeName(executionId);
  }

  public static String getActionSpoolPath(
      IVariables variables,
      ReplayGate gate,
      String workflowName,
      String executionId,
      String actionName) {
    return getExecutionSpoolPath(variables, gate, workflowName, executionId)
        + "/"
        + sanitizeName(actionName);
  }

  public static void deleteExecutionSpool(ILogChannel log, IWorkflowEngine<WorkflowMeta> workflow) {
    if (workflow == null || workflow.getWorkflowMeta() == null) {
      return;
    }
    String executionId = ReplayGateIdentity.executionId(workflow);
    String workflowName = workflow.getWorkflowMeta().getName();
    Set<String> folders = new LinkedHashSet<>();
    for (ActionMeta actionMeta : workflow.getWorkflowMeta().getActions()) {
      ReplayGate gate = getReplayGate(actionMeta);
      if (gate == null || !gate.isEnabled()) {
        continue;
      }
      folders.add(getExecutionSpoolPath(workflow, gate, workflowName, executionId));
    }
    for (String folder : folders) {
      try {
        FileObject folderObject = HopVfs.getFileObject(folder, workflow);
        if (folderObject.exists()) {
          folderObject.deleteAll();
          if (log != null) {
            log.logBasic(
                "Replay Gate: removed sealed gates for execution " + executionId + " at " + folder);
          }
        }
      } catch (Exception e) {
        if (log != null) {
          log.logError("Replay Gate: unable to remove spool folder " + folder, e);
        }
      }
    }
  }

  private static void putOrRemove(ActionMeta actionMeta, String key, String value) {
    if (value == null) {
      Map<String, String> attributes = actionMeta.getAttributes(ReplayDefaults.REPLAY_GATE_GROUP);
      if (attributes != null) {
        attributes.remove(key);
      }
      return;
    }
    actionMeta.setAttribute(ReplayDefaults.REPLAY_GATE_GROUP, key, value);
  }

  private ReplayGateUtil() {
    // Utility
  }
}
