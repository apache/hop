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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.replay.engine.ReplayWorkflowEngine;
import org.apache.hop.replay.engine.ReplayWorkflowRunConfiguration;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** Execution id and the SHA-256 used to decide whether a sealed action gate is still valid. */
public final class ReplayGateIdentity {

  private static final Pattern VARIABLE = Pattern.compile("\\$\\{([^{}]+)}");
  private static final String SNAPSHOT_PREFIX = "replay-gate-variables:";

  private ReplayGateIdentity() {}

  /**
   * The spool key for this run. A variable set for one launch wins over the value saved on the run
   * configuration. Otherwise a Replay run uses its source execution ID, and any other engine uses
   * its own log channel ID.
   */
  public static String executionId(IWorkflowEngine<?> workflow) {
    if (workflow == null) {
      return "unknown";
    }
    String fromVariable = workflow.getVariable(ReplayDefaults.VARIABLE_SOURCE_EXECUTION_ID);
    if (StringUtils.isNotEmpty(fromVariable)) {
      return fromVariable;
    }
    if (workflow instanceof ReplayWorkflowEngine replayEngine) {
      ReplayWorkflowRunConfiguration config = replayEngine.getReplayWorkflowRunConfiguration();
      if (config != null && StringUtils.isNotEmpty(config.getSourceExecutionId())) {
        return workflow.resolve(config.getSourceExecutionId());
      }
    }
    return Const.NVL(workflow.getLogChannelId(), "unknown");
  }

  public static String configurationHash(IWorkflowEngine<?> workflow, ActionMeta actionMeta) {
    String xml = actionXml(actionMeta);
    StringBuilder builder = new StringBuilder();
    builder.append(xml).append('\n');
    appendParameters(builder, workflow);
    builder.append('\n');
    appendReferencedVariables(builder, workflow, xml);
    return sha256(builder.toString());
  }

  public static void snapshotVariables(IWorkflowEngine<?> workflow, ActionMeta actionMeta) {
    if (workflow == null || actionMeta == null || workflow.getExtensionDataMap() == null) {
      return;
    }
    ReplayGate gate = ReplayGateUtil.getReplayGate(actionMeta);
    if (gate == null || !gate.isEnabled()) {
      return;
    }
    Map<String, String> snapshot = readVariables(workflow);
    synchronized (workflow.getExtensionDataMap()) {
      workflow.getExtensionDataMap().put(snapshotKey(actionMeta), snapshot);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, String> changedVariables(
      IWorkflowEngine<?> workflow, ActionMeta actionMeta) {
    if (workflow == null || actionMeta == null || workflow.getExtensionDataMap() == null) {
      return Map.of();
    }
    Object stored;
    synchronized (workflow.getExtensionDataMap()) {
      stored = workflow.getExtensionDataMap().remove(snapshotKey(actionMeta));
    }
    if (!(stored instanceof Map<?, ?> before)) {
      return Map.of();
    }
    Map<String, String> after = readVariables(workflow);
    Map<String, String> changed = new TreeMap<>();
    for (Map.Entry<String, String> entry : after.entrySet()) {
      String previous = (String) before.get(entry.getKey());
      if (!entry.getValue().equals(Const.NVL(previous, ""))) {
        changed.put(entry.getKey(), entry.getValue());
      }
    }
    return changed;
  }

  public static String sha256(String content) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(Const.NVL(content, "").getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(hash.length * 2);
      for (byte value : hash) {
        hex.append(String.format("%02x", value));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  private static String actionXml(ActionMeta actionMeta) {
    if (actionMeta == null) {
      return "";
    }
    IAction action = actionMeta.getAction();
    if (action == null) {
      return "";
    }
    try {
      return Const.NVL(action.getXml(), "");
    } catch (Exception e) {
      return "";
    }
  }

  private static void appendParameters(StringBuilder builder, IWorkflowEngine<?> workflow) {
    if (workflow == null) {
      return;
    }
    String[] names = workflow.listParameters();
    if (names == null || names.length == 0) {
      return;
    }
    Arrays.sort(names);
    for (String name : names) {
      String value = "";
      try {
        value = Const.NVL(workflow.getParameterValue(name), "");
      } catch (UnknownParamException e) {
        value = "";
      }
      builder.append(name).append('=').append(value).append('\n');
    }
  }

  private static void appendReferencedVariables(
      StringBuilder builder, IWorkflowEngine<?> workflow, String xml) {
    if (workflow == null || StringUtils.isEmpty(xml)) {
      return;
    }
    Map<String, String> referenced = new TreeMap<>();
    Matcher matcher = VARIABLE.matcher(xml);
    while (matcher.find()) {
      String name = matcher.group(1);
      if (StringUtils.isEmpty(name) || name.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
        continue;
      }
      referenced.put(name, Const.NVL(workflow.resolve("${" + name + "}"), ""));
    }
    for (Map.Entry<String, String> entry : referenced.entrySet()) {
      builder.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
    }
  }

  private static Map<String, String> readVariables(IWorkflowEngine<?> workflow) {
    Map<String, String> snapshot = new TreeMap<>();
    String[] names = workflow.getVariableNames();
    if (names == null) {
      return snapshot;
    }
    for (String name : names) {
      if (StringUtils.isEmpty(name) || name.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
        continue;
      }
      snapshot.put(name, Const.NVL(workflow.getVariable(name), ""));
    }
    return snapshot;
  }

  private static String snapshotKey(ActionMeta actionMeta) {
    return SNAPSHOT_PREFIX
        + System.identityHashCode(actionMeta)
        + ":"
        + Thread.currentThread().threadId();
  }
}
