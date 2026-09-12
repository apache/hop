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
import java.util.Comparator;
import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.engine.AiAdvisorMetadataContext;
import org.apache.hop.ai.engine.AiCheckResultsSerializer;
import org.apache.hop.ai.engine.AiM2PromptSupport;
import org.apache.hop.ai.engine.AiPromptLoader;
import org.apache.hop.ai.engine.AiTextUtil;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

public final class WorkflowAiContextBuilder {

  static final String PROMPT_ROOT = "/org/apache/hop/ai/prompts/workflow/";
  private static final int MAX_TOPOLOGY_XML_CHARS = 120_000;
  private static final int MAX_LOG_CHARS = 20_000;
  private static final int MAX_CATALOG_ENTRIES = 120;

  private WorkflowAiContextBuilder() {}

  public static AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException {
    if (!(request.getArtifact() instanceof WorkflowMeta workflowMeta)) {
      throw new HopException("No workflow is bound to this session.");
    }
    if (Utils.isEmpty(request.getUserPrompt())) {
      throw new HopException("Please enter a question for the AI advisor");
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
    boolean includeFull = !request.isFollowUp();
    StringBuilder prompt = new StringBuilder();
    prompt.append("User question:\n").append(request.getUserPrompt()).append("\n\n");
    prompt
        .append("Workflow structure JSON:\n")
        .append(serializeStructure(workflowMeta, request.getFocusNodeName()))
        .append("\n\n");
    if (includeFull) {
      prompt
          .append("Workflow summary JSON:\n")
          .append(serializeSummary(workflowMeta))
          .append("\n\n");
      if (request.inclusionEnabled(AiAdvisorInclusions.CATALOG)) {
        prompt
            .append("Available action plugins JSON:\n")
            .append(serializeActionCatalog())
            .append("\n\n");
      }
      if (request.inclusionEnabled(AiAdvisorInclusions.XML)
          && HopAiConfigSingleton.getConfig().isAllowSendFullXml()) {
        prompt
            .append("Workflow topology XML:\n")
            .append(
                AiTextUtil.redactSecrets(
                    AiTextUtil.truncate(workflowMeta.getXml(variables), MAX_TOPOLOGY_XML_CHARS)))
            .append("\n\n");
      }
      if (request.inclusionEnabled(AiAdvisorInclusions.LOGS)) {
        prompt
            .append("Execution log excerpt:\n")
            .append(
                AiTextUtil.redactSecrets(
                    AiTextUtil.truncate(request.getLogExcerpt(), MAX_LOG_CHARS)))
            .append("\n\n");
      }
    }
    AiAdvisorMetadataContext.appendToPrompt(prompt, request);
    if (!Utils.isEmpty(request.getFocusNodeName())) {
      prompt.append("Focus action:\n").append(request.getFocusNodeName()).append("\n\n");
    }
    if (request.inclusionEnabled(AiAdvisorInclusions.CHECKS) && metadataProvider != null) {
      List<ICheckResult> results = new ArrayList<>();
      workflowMeta.checkActions(results, false, null, variables, metadataProvider);
      prompt
          .append("Workflow check results JSON:\n")
          .append(AiCheckResultsSerializer.serialize(results))
          .append("\n\n");
    }
    AiM2PromptSupport.appendAppliedSummaries(prompt, request.getAppliedChangeSummaries());
    return prompt.toString();
  }

  public static String serializeStructure(WorkflowMeta workflowMeta, String focusActionName) {
    StringBuilder json = new StringBuilder();
    json.append("{\"actions\":[");
    List<ActionMeta> actions = workflowMeta.getActions();
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
    StringBuilder json = new StringBuilder();
    json.append("{\"actions\":[");
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = new ArrayList<>(registry.getPlugins(ActionPluginType.class));
    plugins.sort(
        Comparator.comparing(
                IPlugin::getCategory, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER))
            .thenComparing(IPlugin::getName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)));
    int count = 0;
    for (IPlugin plugin : plugins) {
      if (count >= MAX_CATALOG_ENTRIES) {
        break;
      }
      if (plugin.getIds() == null || plugin.getIds().length == 0) {
        continue;
      }
      if (count > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(AiTextUtil.jsonString(plugin.getIds()[0]));
      json.append(",\"name\":").append(AiTextUtil.jsonString(plugin.getName()));
      json.append(",\"category\":").append(AiTextUtil.jsonString(plugin.getCategory()));
      json.append('}');
      count++;
    }
    json.append("]}");
    return json.toString();
  }
}
