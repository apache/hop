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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;

public class ReplayGateUtil {

  public static Map<String, String> getOrCreateReplayGroup(
      Map<String, Map<String, String>> attributesMap) {
    return attributesMap.computeIfAbsent(ReplayDefaults.REPLAY_GATE_GROUP, k -> new HashMap<>());
  }

  public static void storeReplayGate(
      Map<String, String> replayGroup, String name, ReplayGate gate) {
    if (replayGroup == null || StringUtils.isEmpty(name)) {
      return;
    }
    String prefix = name + " : ";
    replayGroup.put(prefix + ReplayDefaults.GATE_ATTR_ENABLED, Boolean.toString(gate.isEnabled()));
    if (StringUtils.isNotEmpty(gate.getSpoolDirectory())) {
      replayGroup.put(prefix + ReplayDefaults.GATE_ATTR_SPOOL_DIR, gate.getSpoolDirectory());
    } else {
      replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_SPOOL_DIR);
    }
    if (StringUtils.isNotEmpty(gate.getCompression())) {
      replayGroup.put(prefix + ReplayDefaults.GATE_ATTR_COMPRESSION, gate.getCompression());
    } else {
      replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_COMPRESSION);
    }
    if (gate.getRowLimit() > 0) {
      replayGroup.put(
          prefix + ReplayDefaults.GATE_ATTR_ROW_LIMIT, Integer.toString(gate.getRowLimit()));
    } else {
      replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_ROW_LIMIT);
    }
    if (StringUtils.isNotEmpty(gate.getDescription())) {
      replayGroup.put(prefix + ReplayDefaults.GATE_ATTR_DESCRIPTION, gate.getDescription());
    } else {
      replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_DESCRIPTION);
    }
  }

  public static ReplayGate getReplayGate(Map<String, String> replayGroup, String name) {
    if (replayGroup == null || StringUtils.isEmpty(name)) {
      return null;
    }
    String prefix = name + " : ";
    String enabledStr = replayGroup.get(prefix + ReplayDefaults.GATE_ATTR_ENABLED);
    if (StringUtils.isEmpty(enabledStr)) {
      return null;
    }

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(ValueMetaStringtoBoolean(enabledStr));
    gate.setSpoolDirectory(
        Const.NVL(replayGroup.get(prefix + ReplayDefaults.GATE_ATTR_SPOOL_DIR), ""));
    gate.setCompression(
        Const.NVL(replayGroup.get(prefix + ReplayDefaults.GATE_ATTR_COMPRESSION), ""));
    gate.setRowLimit(Const.toInt(replayGroup.get(prefix + ReplayDefaults.GATE_ATTR_ROW_LIMIT), 0));
    gate.setDescription(
        Const.NVL(replayGroup.get(prefix + ReplayDefaults.GATE_ATTR_DESCRIPTION), ""));
    return gate;
  }

  public static void clearReplayGate(Map<String, String> replayGroup, String name) {
    if (replayGroup == null || StringUtils.isEmpty(name)) {
      return;
    }
    String prefix = name + " : ";
    replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_ENABLED);
    replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_SPOOL_DIR);
    replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_COMPRESSION);
    replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_ROW_LIMIT);
    replayGroup.remove(prefix + ReplayDefaults.GATE_ATTR_DESCRIPTION);
  }

  public static boolean hasReplayGate(Map<String, String> replayGroup, String name) {
    if (replayGroup == null || StringUtils.isEmpty(name)) {
      return false;
    }
    String prefix = name + " : ";
    String enabledStr = replayGroup.get(prefix + ReplayDefaults.GATE_ATTR_ENABLED);
    return ValueMetaStringtoBoolean(enabledStr);
  }

  public static ReplayGate getTransformReplayGate(
      Map<String, String> replayGroup, String transformName) {
    return getReplayGate(replayGroup, transformName);
  }

  public static ReplayGate getActionReplayGate(Map<String, String> replayGroup, String actionName) {
    return getReplayGate(replayGroup, actionName);
  }

  public static String resolveSpoolDirectory(
      org.apache.hop.core.variables.IVariables variables, ReplayGate gate) {
    String spoolDir =
        (gate != null && StringUtils.isNotEmpty(gate.getSpoolDirectory()))
            ? gate.getSpoolDirectory()
            : ReplayDefaults.DEFAULT_SPOOL_DIR;
    if (variables != null) {
      spoolDir = variables.resolve(spoolDir);
    }
    // Fallback if ${PROJECT_HOME} remained unresolved
    if (spoolDir.contains("${PROJECT_HOME}")) {
      String projectHome = (variables != null) ? variables.getVariable("PROJECT_HOME") : null;
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

  public static String getTransformSpoolPath(
      org.apache.hop.core.variables.IVariables variables,
      ReplayGate gate,
      String pipelineName,
      String transformName) {
    String baseDir = resolveSpoolDirectory(variables, gate);
    return baseDir + "/pipelines/" + sanitizeName(pipelineName) + "/" + sanitizeName(transformName);
  }

  public static String getActionSpoolPath(
      org.apache.hop.core.variables.IVariables variables,
      ReplayGate gate,
      String workflowName,
      String actionName) {
    String baseDir = resolveSpoolDirectory(variables, gate);
    return baseDir + "/workflows/" + sanitizeName(workflowName) + "/" + sanitizeName(actionName);
  }

  public static boolean isRunningUnderReplay(
      org.apache.hop.pipeline.engine.IPipelineEngine<?> pipeline) {
    if (pipeline == null) {
      return false;
    }
    org.apache.hop.workflow.engine.IWorkflowEngine<?> parentWf = pipeline.getParentWorkflow();
    if (parentWf instanceof org.apache.hop.replay.engine.ReplayWorkflowEngine) {
      return true;
    }
    org.apache.hop.pipeline.engine.IPipelineEngine<?> parentPl = pipeline.getParentPipeline();
    if (parentPl != null) {
      return isRunningUnderReplay(parentPl);
    }
    return false;
  }

  private static boolean ValueMetaStringtoBoolean(String str) {
    if (str == null) {
      return false;
    }
    return "true".equalsIgnoreCase(str) || "y".equalsIgnoreCase(str) || "1".equals(str);
  }

  private ReplayGateUtil() {
    // Utility
  }
}
