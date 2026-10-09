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

package org.apache.hop.ai.advisors.workflow;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.engine.AiAdvisorEngine;
import org.apache.hop.ai.engine.AiAdvisorMetadataContext;
import org.apache.hop.ai.engine.AiCheckResultsSerializer;
import org.apache.hop.ai.engine.AiM2PromptSupport;
import org.apache.hop.ai.engine.AiNodeSettings;
import org.apache.hop.ai.engine.AiPluginCatalog;
import org.apache.hop.ai.engine.AiPromptLoader;
import org.apache.hop.ai.engine.AiTextUtil;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

public final class WorkflowAiContextBuilder {

  static final String PROMPT_ROOT = "/org/apache/hop/ai/prompts/workflow/";
  private static final int MAX_TOPOLOGY_XML_CHARS = 120_000;
  private static final int MAX_FOCUS_XML_CHARS = 40_000;
  private static final int MAX_SETTINGS_CHARS = 40_000;
  private static final int MAX_LOG_CHARS = 20_000;

  private WorkflowAiContextBuilder() {}

  public static AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException {
    if (!(request.getArtifact() instanceof WorkflowMeta workflowMeta)) {
      throw new HopException(
          BaseMessages.getString(AiAdvisorEngine.class, "AiContextBuilder.NotLinked.Workflow"));
    }
    if (Utils.isEmpty(request.getUserPrompt())) {
      throw new HopException(
          BaseMessages.getString(AiAdvisorEngine.class, "AiContextBuilder.NoQuestion"));
    }
    String scenarioId =
        Utils.isEmpty(request.getScenarioId()) ? "workflow-general" : request.getScenarioId();
    String system =
        AiPromptLoader.load(PROMPT_ROOT, "preamble-hop.txt")
            + "\n\n"
            + AiPromptLoader.load(PROMPT_ROOT, scenarioId + ".txt")
            + "\n\n"
            + AiM2PromptSupport.buildSupplement();
    return new AiAdvisorPrompt(system, buildUserPrompt(workflowMeta, request));
  }

  static String buildUserPrompt(WorkflowMeta workflowMeta, AiAdvisorRequest request)
      throws HopException {
    IVariables variables = request.getVariables();
    IHopMetadataProvider metadataProvider = request.getMetadataProvider();
    StringBuilder prompt = new StringBuilder();
    // Every turn sends what is checked. The history only replays questions and answers, so a
    // follow-up that skipped this context would lose it, including the log of a new run.
    AiTextUtil.appendSection(prompt, "workflow_summary", serializeSummary(workflowMeta));
    AiTextUtil.appendSection(
        prompt,
        "workflow_structure",
        serializeStructure(
            workflowMeta,
            request.getFocusNodeName(),
            request.inclusionEnabled(AiAdvisorInclusions.SETTINGS)));
    AiAdvisorMetadataContext.appendTypeKeys(prompt, metadataProvider);
    AiAdvisorMetadataContext.appendDatabaseCatalog(prompt);
    if (request.inclusionEnabled(AiAdvisorInclusions.CATALOG)) {
      AiTextUtil.appendSection(prompt, "plugin_catalog", serializeActionCatalog());
    }
    if (request.inclusionEnabled(AiAdvisorInclusions.XML)
        && HopAiConfigSingleton.getConfig().isAllowSendFullXml()) {
      AiTextUtil.appendSection(
          prompt,
          "workflow_xml",
          AiTextUtil.redactSecrets(
              AiTextUtil.truncate(workflowMeta.getXml(variables), MAX_TOPOLOGY_XML_CHARS)));
    }
    if (request.inclusionEnabled(AiAdvisorInclusions.LOGS)) {
      AiTextUtil.appendSection(
          prompt,
          "execution_log",
          AiTextUtil.redactSecrets(AiTextUtil.truncate(request.getLogExcerpt(), MAX_LOG_CHARS)));
    }
    AiAdvisorMetadataContext.appendToPrompt(prompt, request);
    appendFocusAction(prompt, workflowMeta, request.getFocusNodeName());
    if (request.inclusionEnabled(AiAdvisorInclusions.CHECKS) && metadataProvider != null) {
      List<ICheckResult> results = new ArrayList<>();
      workflowMeta.checkActions(results, false, null, variables, metadataProvider);
      AiTextUtil.appendSection(
          prompt, "check_results", AiCheckResultsSerializer.serialize(results));
    }
    AiM2PromptSupport.appendAppliedSummaries(prompt, request.getAppliedChangeSummaries());
    AiTextUtil.appendSection(prompt, "question", request.getUserPrompt());
    // Repeated from the instructions: small models follow the last thing they read best, and
    // otherwise drift to English after a long English context.
    prompt.append(
        "Write your answer in the language that the text in the <question> block is written"
            + " in.\n");
    return prompt.toString();
  }

  static void appendFocusAction(StringBuilder prompt, WorkflowMeta workflowMeta, String focusName) {
    if (Utils.isEmpty(focusName)) {
      return;
    }
    AiTextUtil.appendSection(prompt, "focus_action", focusName);
    ActionMeta action = workflowMeta.findAction(focusName);
    if (action == null) {
      return;
    }
    try {
      String xml = action.getXml();
      if (Utils.isEmpty(xml)) {
        return;
      }
      AiTextUtil.appendSection(
          prompt,
          "focus_action_xml",
          AiTextUtil.redactSecrets(AiTextUtil.truncate(xml, MAX_FOCUS_XML_CHARS)));
    } catch (Exception e) {
      // Skip unreadable action XML rather than failing the whole prompt.
    }
  }

  public static String serializeStructure(WorkflowMeta workflowMeta, String focusActionName) {
    return serializeStructure(workflowMeta, focusActionName, false);
  }

  /**
   * @param includeSettings add each action's settings, until {@link #MAX_SETTINGS_CHARS} is used
   */
  public static String serializeStructure(
      WorkflowMeta workflowMeta, String focusActionName, boolean includeSettings) {
    StringBuilder json = new StringBuilder();
    json.append("{\"actions\":[");
    List<ActionMeta> actions = workflowMeta.getActions();
    int settingsChars = 0;
    for (int i = 0; i < actions.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      ActionMeta action = actions.get(i);
      json.append("{\"name\":").append(AiTextUtil.jsonString(action.getName()));
      json.append(",\"pluginId\":")
          .append(
              AiTextUtil.jsonString(
                  action.getAction() != null ? action.getAction().getPluginId() : ""));
      json.append(",\"parallel\":").append(action.isLaunchingInParallel());
      if (includeSettings && settingsChars < MAX_SETTINGS_CHARS) {
        String settings = AiNodeSettings.toJson(action.getAction());
        if (settings != null) {
          json.append(",\"settings\":").append(settings);
          settingsChars += settings.length();
        }
      }
      json.append('}');
    }
    json.append("],\"hops\":[");
    for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
      if (i > 0) {
        json.append(',');
      }
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop(i);
      String from = hop.getFromAction() != null ? hop.getFromAction().getName() : "";
      String to = hop.getToAction() != null ? hop.getToAction().getName() : "";
      json.append("{\"from\":").append(AiTextUtil.jsonString(from));
      json.append(",\"to\":").append(AiTextUtil.jsonString(to));
      json.append(",\"unconditional\":").append(hop.isUnconditional());
      json.append(",\"evaluation\":").append(hop.isEvaluation());
      json.append('}');
    }
    json.append(']');
    if (!Utils.isEmpty(focusActionName)) {
      json.append(",\"focusAction\":").append(AiTextUtil.jsonString(focusActionName));
    }
    json.append('}');
    return json.toString();
  }

  public static String serializeSummary(WorkflowMeta workflowMeta) {
    StringBuilder json = new StringBuilder();
    json.append("{\"name\":").append(AiTextUtil.jsonString(workflowMeta.getName()));
    json.append(",\"filename\":").append(AiTextUtil.jsonString(workflowMeta.getFilename()));
    json.append(",\"actionCount\":").append(workflowMeta.nrActions());
    json.append(",\"hopCount\":").append(workflowMeta.nrWorkflowHops());
    json.append(",\"parameterNames\":[");
    String[] params = workflowMeta.listParameters();
    for (int i = 0; i < params.length; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(AiTextUtil.jsonString(params[i]));
    }
    json.append("]}");
    return json.toString();
  }

  static String serializeActionCatalog() {
    return AiPluginCatalog.compact(ActionPluginType.class);
  }
}
