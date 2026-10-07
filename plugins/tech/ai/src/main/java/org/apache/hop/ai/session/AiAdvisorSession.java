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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.ai.engine.AiProposalPreview;
import org.apache.hop.i18n.BaseMessages;

/**
 * One advisory conversation. Lives in {@link AiAdvisorSessionStore} so perspective, dialog and dock
 * hosts share the same sessions.
 */
@Getter
@Setter
public class AiAdvisorSession {

  public static final int MAX_HISTORY_TURNS = 6;

  private final String id = UUID.randomUUID().toString();
  private String title = "";
  private String advisorPluginId = "";
  private String scenarioId = "";
  private String providerName = "";
  private String location = AiAdvisorLocations.PERSPECTIVE;
  private String areaLabel = "";
  private String artifactName = "";
  private String artifactKind = "";
  private String focusNodeName = "";

  /**
   * The file of the pipeline or workflow, kept when its tab closes and {@link #getArtifact()} is
   * let go, so the session is found again when the file is reopened.
   */
  private String artifactFilename;

  /** The project the session was started in; see {@link AiAdvisorSessionStore#getSessions()}. */
  private String scope;

  private Object artifact;
  private Supplier<String> logSupplier;

  /** The latest run of the pipeline or workflow; see {@code AiAdvisorOpenRequest}. */
  private Supplier<String> runIdSupplier;

  /**
   * The run that was the latest when the user last switched Logs on or off. That choice holds until
   * the next run: a new log is what a question after a run is usually about.
   */
  private String logChoiceRunId;

  private Map<String, Boolean> inclusions = new LinkedHashMap<>();

  /** Inclusions the user switched on or off, which the assistant then leaves alone. */
  private Set<String> userChosenInclusions = new HashSet<>();

  private List<AiAdvisorMetadataSelection> metadataSelections = new ArrayList<>();
  private Map<String, Object> attributes = new LinkedHashMap<>();
  private Map<String, List<String>> inclusionSelections = new LinkedHashMap<>();
  private final List<AiAdvisorTurn> turns = new ArrayList<>();
  private final List<String> pendingAppliedSummaries = new ArrayList<>();
  private String statusMessage = "";
  private boolean working;
  private volatile boolean cancelled;
  private volatile Thread workerThread;

  /** The id of the latest run, or null when there was none or it is not known. */
  public String currentRunId() {
    Supplier<String> supplier = runIdSupplier;
    try {
      return supplier == null ? null : supplier.get();
    } catch (RuntimeException e) {
      return null;
    }
  }

  public boolean isEmpty() {
    return turns.isEmpty();
  }

  public void addTurn(AiAdvisorTurn turn) {
    turns.add(turn);
  }

  public void requestCancel() {
    cancelled = true;
    Thread thread = workerThread;
    if (thread != null) {
      thread.interrupt();
    }
  }

  public void clearTurns() {
    turns.clear();
    pendingAppliedSummaries.clear();
    statusMessage = "";
  }

  public List<String> consumePendingAppliedSummaries() {
    List<String> copy = List.copyOf(pendingAppliedSummaries);
    pendingAppliedSummaries.clear();
    return copy;
  }

  /**
   * Forget the summaries a question was sent with, once its answer is recorded. Changes applied
   * while it was waiting stay for the next question.
   */
  public void removePendingAppliedSummaries(List<String> sent) {
    if (sent != null) {
      for (String summary : sent) {
        pendingAppliedSummaries.remove(summary);
      }
    }
  }

  public void recordApplied(AiAdvisorTurn turn, List<AiProposal> applied) {
    recordApplied(turn, applied, null);
  }

  public void recordApplied(AiAdvisorTurn turn, List<AiProposal> applied, IAiAdvisor advisor) {
    if (applied == null || applied.isEmpty()) {
      return;
    }
    List<String> summaries = new ArrayList<>();
    for (AiProposal proposal : applied) {
      summaries.add(
          advisor != null
              ? advisor.summarizeApplied(proposal)
              : AiProposalPreview.appliedSummary(proposal));
    }
    pendingAppliedSummaries.addAll(summaries);
    if (turn != null) {
      turn.getAppliedSummaries().addAll(summaries);
    }
  }

  public String areaLabel() {
    if (areaLabel != null && !areaLabel.isBlank()) {
      return areaLabel;
    }
    return BaseMessages.getString(AiAdvisorSession.class, "AiAdvisorSession.Area.General");
  }

  public String displayTitle() {
    if (title != null && !title.isBlank()) {
      return title;
    }
    if (artifactName != null && !artifactName.isBlank()) {
      return artifactName;
    }
    return BaseMessages.getString(AiAdvisorSession.class, "AiAdvisorSession.Title.New");
  }
}
