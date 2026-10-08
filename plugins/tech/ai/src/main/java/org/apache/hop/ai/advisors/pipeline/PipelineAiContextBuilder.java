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

package org.apache.hop.ai.advisors.pipeline;

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
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

public final class PipelineAiContextBuilder {

  static final String PROMPT_ROOT = "/org/apache/hop/ai/prompts/pipeline/";
  private static final int MAX_TOPOLOGY_XML_CHARS = 120_000;
  private static final int MAX_FOCUS_XML_CHARS = 40_000;
  private static final int MAX_SETTINGS_CHARS = 40_000;
  private static final int MAX_LOG_CHARS = 20_000;

  private PipelineAiContextBuilder() {}

  public static AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException {
    if (!(request.getArtifact() instanceof PipelineMeta pipelineMeta)) {
      throw new HopException(
          BaseMessages.getString(AiAdvisorEngine.class, "AiContextBuilder.NotLinked.Pipeline"));
    }
    if (Utils.isEmpty(request.getUserPrompt())) {
      throw new HopException(
          BaseMessages.getString(AiAdvisorEngine.class, "AiContextBuilder.NoQuestion"));
    }
    String scenarioId =
        Utils.isEmpty(request.getScenarioId()) ? "pipeline-general" : request.getScenarioId();
    String system =
        AiPromptLoader.load(PROMPT_ROOT, "preamble-hop.txt")
            + "\n\n"
            + AiPromptLoader.load(PROMPT_ROOT, scenarioId + ".txt")
            + "\n\n"
            + AiM2PromptSupport.buildSupplement();
    return new AiAdvisorPrompt(system, buildUserPrompt(pipelineMeta, request));
  }

  static String buildUserPrompt(PipelineMeta pipelineMeta, AiAdvisorRequest request)
      throws HopException {
    IVariables variables = request.getVariables();
    IHopMetadataProvider metadataProvider = request.getMetadataProvider();
    StringBuilder prompt = new StringBuilder();
    // Every turn sends what is checked. The history only replays questions and answers, so a
    // follow-up that skipped this context would lose it, including the log of a new run.
    AiTextUtil.appendSection(prompt, "pipeline_summary", serializeSummary(pipelineMeta));
    AiTextUtil.appendSection(
        prompt,
        "pipeline_structure",
        serializeStructure(
            pipelineMeta,
            request.getFocusNodeName(),
            request.inclusionEnabled(AiAdvisorInclusions.SETTINGS)));
    AiAdvisorMetadataContext.appendTypeKeys(prompt, metadataProvider);
    AiAdvisorMetadataContext.appendDatabaseCatalog(prompt);
    if (request.inclusionEnabled(AiAdvisorInclusions.CATALOG)) {
      AiTextUtil.appendSection(prompt, "plugin_catalog", serializeTransformCatalog());
    }
    if (request.inclusionEnabled(AiAdvisorInclusions.XML)
        && HopAiConfigSingleton.getConfig().isAllowSendFullXml()) {
      AiTextUtil.appendSection(
          prompt,
          "pipeline_xml",
          AiTextUtil.redactSecrets(
              AiTextUtil.truncate(pipelineMeta.getXml(variables), MAX_TOPOLOGY_XML_CHARS)));
    }
    if (request.inclusionEnabled(AiAdvisorInclusions.LOGS)) {
      AiTextUtil.appendSection(
          prompt,
          "execution_log",
          AiTextUtil.redactSecrets(AiTextUtil.truncate(request.getLogExcerpt(), MAX_LOG_CHARS)));
    }
    AiAdvisorMetadataContext.appendToPrompt(prompt, request);
    appendFocusTransform(prompt, pipelineMeta, request.getFocusNodeName());
    if (request.inclusionEnabled(AiAdvisorInclusions.CHECKS) && metadataProvider != null) {
      List<ICheckResult> results = new ArrayList<>();
      pipelineMeta.checkTransforms(results, false, null, variables, metadataProvider);
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

  static void appendFocusTransform(
      StringBuilder prompt, PipelineMeta pipelineMeta, String focusName) {
    if (Utils.isEmpty(focusName)) {
      return;
    }
    AiTextUtil.appendSection(prompt, "focus_transform", focusName);
    TransformMeta transform = pipelineMeta.findTransform(focusName);
    if (transform == null) {
      return;
    }
    try {
      String xml = transform.getXml();
      if (Utils.isEmpty(xml)) {
        return;
      }
      AiTextUtil.appendSection(
          prompt,
          "focus_transform_xml",
          AiTextUtil.redactSecrets(AiTextUtil.truncate(xml, MAX_FOCUS_XML_CHARS)));
    } catch (Exception e) {
      // Skip unreadable transform XML rather than failing the whole prompt.
    }
  }

  public static String serializeStructure(PipelineMeta pipelineMeta, String focusTransformName) {
    return serializeStructure(pipelineMeta, focusTransformName, false);
  }

  /**
   * @param includeSettings add each transform's settings, until {@link #MAX_SETTINGS_CHARS} is used
   */
  public static String serializeStructure(
      PipelineMeta pipelineMeta, String focusTransformName, boolean includeSettings) {
    StringBuilder json = new StringBuilder();
    json.append("{\"transforms\":[");
    List<TransformMeta> transforms = pipelineMeta.getTransforms();
    int settingsChars = 0;
    for (int i = 0; i < transforms.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      TransformMeta transform = transforms.get(i);
      json.append("{\"name\":").append(AiTextUtil.jsonString(transform.getName()));
      json.append(",\"pluginId\":").append(AiTextUtil.jsonString(transform.getTransformPluginId()));
      json.append(",\"copies\":").append(AiTextUtil.jsonString(transform.getCopiesString()));
      json.append(",\"distributes\":").append(transform.isDistributes());
      if (includeSettings && settingsChars < MAX_SETTINGS_CHARS) {
        String settings = AiNodeSettings.toJson(transform.getTransform());
        if (settings != null) {
          json.append(",\"settings\":").append(settings);
          settingsChars += settings.length();
        }
      }
      json.append('}');
    }
    json.append("],\"hops\":[");
    List<PipelineHopMeta> hops = pipelineMeta.getPipelineHops();
    for (int i = 0; i < hops.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      PipelineHopMeta hop = hops.get(i);
      String from = hop.getFromTransform() != null ? hop.getFromTransform().getName() : "";
      String to = hop.getToTransform() != null ? hop.getToTransform().getName() : "";
      json.append("{\"from\":").append(AiTextUtil.jsonString(from));
      json.append(",\"to\":").append(AiTextUtil.jsonString(to));
      json.append(",\"enabled\":").append(hop.isEnabled());
      json.append('}');
    }
    json.append(']');
    if (!Utils.isEmpty(focusTransformName)) {
      json.append(",\"focusTransform\":").append(AiTextUtil.jsonString(focusTransformName));
    }
    json.append('}');
    return json.toString();
  }

  public static String serializeSummary(PipelineMeta pipelineMeta) {
    StringBuilder json = new StringBuilder();
    json.append("{\"name\":").append(AiTextUtil.jsonString(pipelineMeta.getName()));
    json.append(",\"filename\":").append(AiTextUtil.jsonString(pipelineMeta.getFilename()));
    json.append(",\"transformCount\":").append(pipelineMeta.getTransforms().size());
    json.append(",\"hopCount\":").append(pipelineMeta.nrPipelineHops());
    json.append(",\"parameterNames\":[");
    String[] params = pipelineMeta.listParameters();
    for (int i = 0; i < params.length; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(AiTextUtil.jsonString(params[i]));
    }
    json.append("]}");
    return json.toString();
  }

  static String serializeTransformCatalog() {
    return AiPluginCatalog.compact(TransformPluginType.class);
  }
}
