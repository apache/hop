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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.engine.AiProposalPreview;

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
  private Object artifact;
  private Supplier<String> logSupplier;
  private Map<String, Boolean> inclusions = new LinkedHashMap<>();
  private List<AiAdvisorMetadataSelection> metadataSelections = new ArrayList<>();
  private final List<AiAdvisorTurn> turns = new ArrayList<>();
  private final List<String> pendingAppliedSummaries = new ArrayList<>();
  private String statusMessage = "";
  private boolean working;
  private volatile boolean cancelled;
  private volatile Thread workerThread;

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

  public void recordApplied(AiAdvisorTurn turn, List<AiProposal> applied) {
    if (applied == null || applied.isEmpty()) {
      return;
    }
    List<String> summaries = new ArrayList<>();
    for (AiProposal proposal : applied) {
      summaries.add(AiProposalPreview.appliedSummary(proposal));
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
    return "General";
  }

  public String displayTitle() {
    if (title != null && !title.isBlank()) {
      return title;
    }
    if (artifactName != null && !artifactName.isBlank()) {
      return artifactName;
    }
    return "New session";
  }
}
