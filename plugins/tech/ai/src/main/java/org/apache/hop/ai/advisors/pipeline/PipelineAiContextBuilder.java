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
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

public final class PipelineAiContextBuilder {

  static final String PROMPT_ROOT = "/org/apache/hop/ai/prompts/pipeline/";
  private static final int MAX_TOPOLOGY_XML_CHARS = 120_000;
  private static final int MAX_FOCUS_XML_CHARS = 40_000;
  private static final int MAX_LOG_CHARS = 20_000;
  private static final int MAX_CATALOG_ENTRIES = 180;

  private PipelineAiContextBuilder() {}

  public static AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException {
    if (!(request.getArtifact() instanceof PipelineMeta pipelineMeta)) {
      throw new HopException("No pipeline is bound to this session.");
    }
    if (Utils.isEmpty(request.getUserPrompt())) {
      throw new HopException("Please enter a question for the AI advisor");
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
    boolean includeFull = !request.isFollowUp();
    StringBuilder prompt = new StringBuilder();
    prompt.append("User question:\n").append(request.getUserPrompt()).append("\n\n");
    prompt
        .append("Pipeline structure JSON:\n")
        .append(serializeStructure(pipelineMeta, request.getFocusNodeName()))
        .append("\n\n");
    if (includeFull) {
      prompt
          .append("Pipeline summary JSON:\n")
          .append(serializeSummary(pipelineMeta))
          .append("\n\n");
      AiAdvisorMetadataContext.appendTypeKeys(prompt, metadataProvider);
      AiAdvisorMetadataContext.appendDatabaseCatalog(prompt);
      if (request.inclusionEnabled(AiAdvisorInclusions.CATALOG)) {
        prompt
            .append("Available transform plugins JSON:\n")
            .append(serializeTransformCatalog())
            .append("\n\n");
      }
      if (request.inclusionEnabled(AiAdvisorInclusions.XML)
          && HopAiConfigSingleton.getConfig().isAllowSendFullXml()) {
        prompt
            .append("Pipeline topology XML:\n")
            .append(
                AiTextUtil.redactSecrets(
                    AiTextUtil.truncate(pipelineMeta.getXml(variables), MAX_TOPOLOGY_XML_CHARS)))
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
    appendFocusTransform(prompt, pipelineMeta, request.getFocusNodeName());
    if (request.inclusionEnabled(AiAdvisorInclusions.CHECKS) && metadataProvider != null) {
      List<ICheckResult> results = new ArrayList<>();
      pipelineMeta.checkTransforms(results, false, null, variables, metadataProvider);
      prompt
          .append("Pipeline check results JSON:\n")
          .append(AiCheckResultsSerializer.serialize(results))
          .append("\n\n");
    }
    AiM2PromptSupport.appendAppliedSummaries(prompt, request.getAppliedChangeSummaries());
    return prompt.toString();
  }

  static void appendFocusTransform(
      StringBuilder prompt, PipelineMeta pipelineMeta, String focusName) {
    if (Utils.isEmpty(focusName)) {
      return;
    }
    prompt.append("Focus transform:\n").append(focusName).append("\n\n");
    TransformMeta transform = pipelineMeta.findTransform(focusName);
    if (transform == null) {
      return;
    }
    try {
      String xml = transform.getXml();
      if (Utils.isEmpty(xml)) {
        return;
      }
      prompt
          .append("Focus transform XML:\n")
          .append(AiTextUtil.redactSecrets(AiTextUtil.truncate(xml, MAX_FOCUS_XML_CHARS)))
          .append("\n\n");
    } catch (Exception e) {
      // Skip unreadable transform XML rather than failing the whole prompt.
    }
  }

  public static String serializeStructure(PipelineMeta pipelineMeta, String focusTransformName) {
    StringBuilder json = new StringBuilder();
    json.append("{\"transforms\":[");
    List<TransformMeta> transforms = pipelineMeta.getTransforms();
    for (int i = 0; i < transforms.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      TransformMeta transform = transforms.get(i);
      json.append("{\"name\":").append(AiTextUtil.jsonString(transform.getName()));
      json.append(",\"pluginId\":").append(AiTextUtil.jsonString(transform.getTransformPluginId()));
      json.append(",\"copies\":").append(AiTextUtil.jsonString(transform.getCopiesString()));
      json.append(",\"distributes\":").append(transform.isDistributes());
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
    StringBuilder json = new StringBuilder();
    json.append("{\"transforms\":[");
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = new ArrayList<>(registry.getPlugins(TransformPluginType.class));
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
