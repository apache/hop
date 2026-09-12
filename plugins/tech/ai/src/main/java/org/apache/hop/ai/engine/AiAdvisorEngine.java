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

package org.apache.hop.ai.engine;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Runs one advisory turn: advisor prompt → {@link AiChatFactory} → parsed response. */
public final class AiAdvisorEngine {

  private AiAdvisorEngine() {}

  public static AiAdvisorResponse advise(
      AiAdvisorSession session,
      IAiAdvisor advisor,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    return advise(session, advisor, variables, metadataProvider, null);
  }

  /**
   * @param logExcerpt execution log captured on the UI thread before this method runs in the
   *     background. When non-null it is used as-is so SWT log widgets are not touched here.
   */
  public static AiAdvisorResponse advise(
      AiAdvisorSession session,
      IAiAdvisor advisor,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String logExcerpt)
      throws HopException {
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (!config.isAiEnabled()) {
      throw new HopException(
          "AI advisory is disabled. Enable it under Configuration → Plugins → AI Assistant.");
    }
    if (advisor == null) {
      throw new HopException("Select an AI advisor for this session.");
    }
    AiProvider provider = loadProvider(session, metadataProvider);
    AiAdvisorRequest request = toRequest(session, variables, metadataProvider, logExcerpt);

    if (session.isCancelled()) {
      throw new HopException("AI request was cancelled");
    }
    AiAdvisorPrompt prompt = advisor.buildPrompt(request);
    List<ChatMessage> history = historyFrom(session);
    String raw =
        AiChatFactory.generate(
            provider, variables, prompt.getSystemPrompt(), prompt.getUserPrompt(), history);
    if (session.isCancelled() || Thread.currentThread().isInterrupted()) {
      throw new HopException("AI request was cancelled");
    }
    return advisor.parseResponse(raw);
  }

  static AiAdvisorRequest toRequest(
      AiAdvisorSession session,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String logExcerpt) {
    AiAdvisorRequest request = new AiAdvisorRequest();
    if (session == null) {
      return request;
    }
    request.setLocation(session.getLocation());
    request.setScenarioId(session.getScenarioId());
    request.setUserPrompt(
        session.isEmpty()
            ? ""
            : session.getTurns().get(session.getTurns().size() - 1).getUserPrompt());
    request.setFocusNodeName(session.getFocusNodeName());
    request.setAiProviderName(session.getProviderName());
    request.setVariables(variables);
    request.setMetadataProvider(metadataProvider);
    request.setArtifact(session.getArtifact());
    request.setInclusions(
        session.getInclusions() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(session.getInclusions()));
    request.setMetadataSelections(
        session.getMetadataSelections() == null
            ? new ArrayList<>()
            : new ArrayList<>(session.getMetadataSelections()));
    request.setAttributes(copyAttributes(session.getAttributes()));
    request.setInclusionSelections(copyInclusionSelections(session.getInclusionSelections()));
    request.setFollowUp(hasSuccessfulPriorTurn(session));
    request.setAppliedChangeSummaries(session.consumePendingAppliedSummaries());
    if (logExcerpt != null) {
      request.setLogExcerpt(logExcerpt);
    } else if (session.getLogSupplier() != null) {
      request.setLogExcerpt(session.getLogSupplier().get());
    }
    return request;
  }

  static Map<String, Object> copyAttributes(Map<String, Object> source) {
    return source == null ? new LinkedHashMap<>() : new LinkedHashMap<>(source);
  }

  static Map<String, List<String>> copyInclusionSelections(Map<String, List<String>> source) {
    Map<String, List<String>> copy = new LinkedHashMap<>();
    if (source == null) {
      return copy;
    }
    for (Map.Entry<String, List<String>> entry : source.entrySet()) {
      copy.put(
          entry.getKey(),
          entry.getValue() == null ? new ArrayList<>() : new ArrayList<>(entry.getValue()));
    }
    return copy;
  }

  public static AiProvider loadProvider(
      AiAdvisorSession session, IHopMetadataProvider metadataProvider) throws HopException {
    String name = session.getProviderName();
    if (Utils.isEmpty(name)) {
      name = HopAiConfigSingleton.getConfig().getDefaultProviderName();
    }
    if (Utils.isEmpty(name)) {
      throw new HopException(
          "No AI provider is selected. Create one under Metadata → AI Provider.");
    }
    AiProvider provider = metadataProvider.getSerializer(AiProvider.class).load(name);
    if (provider == null) {
      throw new HopException("AI provider '" + name + "' was not found.");
    }
    return provider;
  }

  static boolean hasSuccessfulPriorTurn(AiAdvisorSession session) {
    List<AiAdvisorTurn> turns = session.getTurns();
    for (int i = 0; i < turns.size() - 1; i++) {
      if (!Utils.isEmpty(turns.get(i).getAssistantAdvice())) {
        return true;
      }
    }
    return false;
  }

  static List<ChatMessage> historyFrom(AiAdvisorSession session) {
    List<AiAdvisorTurn> turns = session.getTurns();
    if (turns.size() <= 1) {
      return List.of();
    }
    int from = Math.max(0, turns.size() - 1 - AiAdvisorSession.MAX_HISTORY_TURNS);
    List<ChatMessage> history = new ArrayList<>();
    for (int i = from; i < turns.size() - 1; i++) {
      AiAdvisorTurn turn = turns.get(i);
      if (!Utils.isEmpty(turn.getUserPrompt()) && !Utils.isEmpty(turn.getAssistantAdvice())) {
        history.add(new UserMessage(turn.getUserPrompt()));
        history.add(new AiMessage(turn.getAssistantAdvice()));
      }
    }
    return history;
  }
}
