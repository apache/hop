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
package org.apache.hop.ai.session;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.engine.AiMetadataBackup;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;

/**
 * Keeps the AI Assistant sessions of a project between Hop GUI runs, in the audit folder ({@code
 * HOP_AUDIT_FOLDER}), where Hop GUI also remembers the open files of each project.
 *
 * <p>Questions, answers, proposals and the session options are kept. The context that was sent with
 * a question is not: it is gathered again from the open file. The pipeline or workflow itself is
 * found again by its file name when it is opened.
 */
public final class AiAdvisorSessionArchive {

  static final String AUDIT_TYPE = "ai-assistant";
  static final String AUDIT_NAME = "sessions";
  static final String DEFAULT_GROUP = "hop-gui";

  /** The newest sessions per project that are kept; older ones are dropped when saving. */
  static final int MAX_SESSIONS = 50;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AiAdvisorSessionArchive() {}

  public static void save(String scope, List<AiAdvisorSession> sessions) throws HopException {
    List<AiAdvisorSession> kept =
        sessions.size() > MAX_SESSIONS
            ? sessions.subList(sessions.size() - MAX_SESSIONS, sessions.size())
            : sessions;
    List<Map<String, Object>> list = new ArrayList<>();
    for (AiAdvisorSession session : kept) {
      list.add(toMap(session));
    }
    Map<String, Object> state = new LinkedHashMap<>();
    try {
      state.put("json", MAPPER.writeValueAsString(list));
    } catch (Exception e) {
      throw new HopException("Unable to write the AI Assistant sessions", e);
    }
    AuditManager.getActive()
        .storeState(group(scope), AUDIT_TYPE, new AuditState(AUDIT_NAME, state));
  }

  public static List<AiAdvisorSession> load(String scope) throws HopException {
    List<AiAdvisorSession> sessions = new ArrayList<>();
    AuditState state = AuditManager.getActive().retrieveState(group(scope), AUDIT_TYPE, AUDIT_NAME);
    if (state == null || state.getStateMap() == null) {
      return sessions;
    }
    Object json = state.getStateMap().get("json");
    if (!(json instanceof String text) || Utils.isEmpty(text)) {
      return sessions;
    }
    try {
      List<Map<String, Object>> list =
          MAPPER.readValue(text, new TypeReference<List<Map<String, Object>>>() {});
      for (Map<String, Object> map : list) {
        AiAdvisorSession session = fromMap(map);
        session.setScope(scope);
        sessions.add(session);
      }
    } catch (Exception e) {
      throw new HopException("Unable to read the saved AI Assistant sessions", e);
    }
    return sessions;
  }

  static final String LAST_PROVIDER_NAME = "last-provider";

  /**
   * Remember the provider picked last in a project, for new sessions when no default provider is
   * configured. Kept even when conversations are not: it holds only a name.
   */
  public static void saveLastProvider(String scope, String providerName) throws HopException {
    Map<String, Object> state = new LinkedHashMap<>();
    state.put("name", providerName == null ? "" : providerName);
    AuditManager.getActive()
        .storeState(group(scope), AUDIT_TYPE, new AuditState(LAST_PROVIDER_NAME, state));
  }

  /** The provider picked last in a project, or null when none was remembered. */
  public static String loadLastProvider(String scope) throws HopException {
    AuditState state =
        AuditManager.getActive().retrieveState(group(scope), AUDIT_TYPE, LAST_PROVIDER_NAME);
    if (state == null
        || state.getStateMap() == null
        || !(state.getStateMap().get("name") instanceof String name)
        || Utils.isEmpty(name)) {
      return null;
    }
    return name;
  }

  static String group(String scope) {
    return Utils.isEmpty(scope) ? DEFAULT_GROUP : scope;
  }

  static Map<String, Object> toMap(AiAdvisorSession session) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("title", session.getTitle());
    map.put("advisorPluginId", session.getAdvisorPluginId());
    map.put("scenarioId", session.getScenarioId());
    map.put("providerName", session.getProviderName());
    map.put("location", session.getLocation());
    map.put("areaLabel", session.getAreaLabel());
    map.put("artifactName", session.getArtifactName());
    map.put("artifactKind", session.getArtifactKind());
    map.put("artifactFilename", session.getArtifactFilename());
    map.put("focusNodeName", session.getFocusNodeName());
    map.put("inclusions", new LinkedHashMap<>(session.getInclusions()));
    map.put("userChosenInclusions", new ArrayList<>(session.getUserChosenInclusions()));
    List<Map<String, String>> selections = new ArrayList<>();
    for (AiAdvisorMetadataSelection selection : session.getMetadataSelections()) {
      Map<String, String> entry = new LinkedHashMap<>();
      entry.put("typeKey", selection.getTypeKey());
      entry.put("name", selection.getName());
      selections.add(entry);
    }
    map.put("metadataSelections", selections);
    map.put("inclusionSelections", new LinkedHashMap<>(session.getInclusionSelections()));
    List<Map<String, Object>> turns = new ArrayList<>();
    for (AiAdvisorTurn turn : session.getTurns()) {
      turns.add(toMap(turn));
    }
    map.put("turns", turns);
    return map;
  }

  static Map<String, Object> toMap(AiAdvisorTurn turn) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("userPrompt", turn.getUserPrompt());
    map.put("assistantAdvice", turn.getAssistantAdvice());
    map.put("rawAnswer", turn.getRawAnswer());
    map.put("errorMessage", turn.getErrorMessage());
    map.put("proposalBlockPresent", turn.isProposalBlockPresent());
    map.put("proposalParseError", turn.getProposalParseError());
    map.put("inputTokenCount", turn.getInputTokenCount());
    map.put("outputTokenCount", turn.getOutputTokenCount());
    map.put("durationMs", turn.getDurationMs());
    map.put("appliedSummaries", new ArrayList<>(turn.getAppliedSummaries()));
    // Undo of saved metadata stays possible after a restart. The earlier version is kept as the
    // metadata JSON stores it, passwords encoded.
    List<Map<String, Object>> backups = new ArrayList<>();
    for (AiMetadataBackup backup : turn.getMetadataBackups()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("typeKey", backup.typeKey());
      entry.put("name", backup.name());
      entry.put("previousJson", backup.previousJson());
      backups.add(entry);
    }
    map.put("metadataBackups", backups);
    List<Map<String, Object>> proposals = new ArrayList<>();
    for (AiProposal proposal : turn.getProposals()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("id", proposal.getId());
      entry.put("description", proposal.getDescription());
      entry.put("riskLevel", proposal.getRiskLevel());
      entry.put("type", proposal.getType());
      entry.put("parameters", new LinkedHashMap<>(proposal.getParameters()));
      proposals.add(entry);
    }
    map.put("proposals", proposals);
    return map;
  }

  @SuppressWarnings("unchecked")
  static AiAdvisorSession fromMap(Map<String, Object> map) {
    AiAdvisorSession session = new AiAdvisorSession();
    session.setTitle(text(map, "title"));
    session.setAdvisorPluginId(text(map, "advisorPluginId"));
    session.setScenarioId(text(map, "scenarioId"));
    session.setProviderName(text(map, "providerName"));
    if (!Utils.isEmpty(text(map, "location"))) {
      session.setLocation(text(map, "location"));
    }
    session.setAreaLabel(text(map, "areaLabel"));
    session.setArtifactName(text(map, "artifactName"));
    session.setArtifactKind(text(map, "artifactKind"));
    session.setArtifactFilename((String) map.get("artifactFilename"));
    session.setFocusNodeName(text(map, "focusNodeName"));
    if (map.get("inclusions") instanceof Map<?, ?> inclusions) {
      for (Map.Entry<?, ?> entry : inclusions.entrySet()) {
        session
            .getInclusions()
            .put(String.valueOf(entry.getKey()), Boolean.TRUE.equals(entry.getValue()));
      }
    }
    if (map.get("userChosenInclusions") instanceof List<?> chosen) {
      session.setUserChosenInclusions(new HashSet<>());
      for (Object id : chosen) {
        session.getUserChosenInclusions().add(String.valueOf(id));
      }
    }
    if (map.get("metadataSelections") instanceof List<?> selections) {
      for (Object item : selections) {
        if (item instanceof Map<?, ?> entry) {
          session
              .getMetadataSelections()
              .add(
                  new AiAdvisorMetadataSelection(
                      String.valueOf(entry.get("typeKey")), String.valueOf(entry.get("name"))));
        }
      }
    }
    if (map.get("inclusionSelections") instanceof Map<?, ?> selections) {
      for (Map.Entry<?, ?> entry : selections.entrySet()) {
        List<String> ids = new ArrayList<>();
        if (entry.getValue() instanceof List<?> values) {
          for (Object value : values) {
            ids.add(String.valueOf(value));
          }
        }
        session.getInclusionSelections().put(String.valueOf(entry.getKey()), ids);
      }
    }
    if (map.get("turns") instanceof List<?> turns) {
      for (Object item : turns) {
        if (item instanceof Map<?, ?> turnMap) {
          session.addTurn(turnFromMap((Map<String, Object>) turnMap));
        }
      }
    }
    return session;
  }

  @SuppressWarnings("unchecked")
  static AiAdvisorTurn turnFromMap(Map<String, Object> map) {
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt(text(map, "userPrompt"));
    turn.setAssistantAdvice(text(map, "assistantAdvice"));
    turn.setRawAnswer((String) map.get("rawAnswer"));
    turn.setErrorMessage((String) map.get("errorMessage"));
    turn.setProposalBlockPresent(Boolean.TRUE.equals(map.get("proposalBlockPresent")));
    turn.setProposalParseError((String) map.get("proposalParseError"));
    turn.setInputTokenCount(number(map.get("inputTokenCount")));
    turn.setOutputTokenCount(number(map.get("outputTokenCount")));
    Integer duration = number(map.get("durationMs"));
    turn.setDurationMs(duration == null ? null : duration.longValue());
    if (map.get("appliedSummaries") instanceof List<?> applied) {
      for (Object summary : applied) {
        turn.getAppliedSummaries().add(String.valueOf(summary));
      }
    }
    if (map.get("metadataBackups") instanceof List<?> backups) {
      for (Object item : backups) {
        if (item instanceof Map<?, ?> entry
            && entry.get("typeKey") instanceof String typeKey
            && entry.get("name") instanceof String name) {
          turn.getMetadataBackups()
              .add(new AiMetadataBackup(typeKey, name, (String) entry.get("previousJson")));
        }
      }
    }
    if (map.get("proposals") instanceof List<?> proposals) {
      for (Object item : proposals) {
        if (item instanceof Map<?, ?> entry) {
          AiProposal proposal = new AiProposal();
          proposal.setId((String) entry.get("id"));
          proposal.setDescription((String) entry.get("description"));
          if (entry.get("riskLevel") instanceof String risk) {
            proposal.setRiskLevel(risk);
          }
          proposal.setType((String) entry.get("type"));
          if (entry.get("parameters") instanceof Map<?, ?> parameters) {
            for (Map.Entry<?, ?> parameter : parameters.entrySet()) {
              proposal
                  .getParameters()
                  .put(String.valueOf(parameter.getKey()), String.valueOf(parameter.getValue()));
            }
          }
          turn.getProposals().add(proposal);
        }
      }
    }
    return turn;
  }

  private static String text(Map<String, Object> map, String key) {
    Object value = map.get(key);
    return value == null ? "" : String.valueOf(value);
  }

  private static Integer number(Object value) {
    return value instanceof Number number ? number.intValue() : null;
  }
}
