/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.advisor;

import java.util.List;
import org.apache.hop.core.exception.HopException;

/**
 * A pluggable AI advisory integration (pipeline building, Data Vault modeling, lineage, …).
 * Implementations are discovered via {@link AiAdvisorPlugin}. Location ids are free-form; GUI entry
 * points are separate toolbar / context-action plugins.
 *
 * <p>Keep this interface free of langchain4j and SWT. The GUI workbench lists advisors, asks them
 * to build a prompt, and optionally to validate/apply proposals.
 */
public interface IAiAdvisor {

  String getId();

  String getName();

  /**
   * Location ids this advisor is meant for (see {@link AiAdvisorPlugin#locations()}). Empty means
   * the workbench may offer it for any session. Plugins add new ids as they add file types.
   */
  default String[] getLocations() {
    return new String[0];
  }

  List<AiAdvisorScenario> listScenarios();

  /**
   * Optional extra context the user may attach to a prompt. Best practice: every item has {@code
   * defaultSelected = false} so the session starts sharing nothing beyond {@link
   * #listBaselineSharing()}.
   */
  default List<AiAdvisorInclusion> listInclusions() {
    return List.of();
  }

  /**
   * Short phrases for context this advisor always puts in the prompt (aside from the user's
   * question), for example {@code graph structure}. Shown on the collapsed Sharing line. Empty
   * means the question is the only baseline.
   */
  default List<String> listBaselineSharing() {
    return List.of();
  }

  /**
   * Assemble system and user prompts, including redacted context. Must not call the language model.
   *
   * <p>Do not load plugin-folder notes here. The workbench appends {@code ai-context.md} and {@code
   * ai-context/<id>.md} from this plugin's folder (or classpath) after {@code buildPrompt}.
   */
  AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException;

  /**
   * Optional computed standing notes for the system prompt. Prefer shipping {@code ai-context.md}
   * in the plugin folder. Default empty.
   */
  default String getStandingContext() {
    return "";
  }

  /**
   * Parse raw assistant text into advice and optional {@code hop_proposals} blocks. Override to
   * parse a different fence (for example {@code dv_proposals}). Chat-only advisors that never emit
   * a fence can leave this default.
   */
  default AiAdvisorResponse parseResponse(String raw) {
    return AiProposalParser.parse(raw);
  }

  /**
   * Optional custom preview text for a proposal in the review dialog. Return null or blank to use
   * the workbench parameter list.
   */
  default String previewProposal(AiProposal proposal) {
    return null;
  }

  default List<AiProposalValidation> validateProposals(
      AiAdvisorRequest request, List<AiProposal> proposals) {
    return List.of();
  }

  default void applyProposals(AiAdvisorRequest request, List<AiProposal> selected)
      throws HopException {
    // Chat-only advisors have nothing to apply.
  }

  /**
   * Choices for an inclusion with {@code picker = true}. Called on the UI thread when the user
   * clicks Select…. Must not open SWT. Empty list → the workbench shows a short message and
   * unchecks the inclusion. Id {@code metadata} is workbench-owned and is not routed here.
   */
  default List<AiAdvisorInclusionChoice> listInclusionChoices(
      String inclusionId, AiAdvisorRequest request) {
    return List.of();
  }

  /**
   * One-line summary of an applied proposal for the next user prompt. Default is {@code type:
   * description} or whichever of those is present.
   */
  default String summarizeApplied(AiProposal proposal) {
    if (proposal == null) {
      return "unknown change";
    }
    String type = proposal.getType() == null ? "" : proposal.getType().trim();
    String description = proposal.getDescription() == null ? "" : proposal.getDescription().trim();
    if (type.isEmpty() && description.isEmpty()) {
      return "unknown change";
    }
    if (type.isEmpty()) {
      return description;
    }
    if (description.isEmpty()) {
      return type;
    }
    return type + ": " + description;
  }

  /**
   * Called on the UI thread after {@link #applyProposals} succeeds. Pipeline/workflow advisors may
   * no-op (the workbench refreshes those graphs). Other advisors should mark undo, setChanged, and
   * redraw using {@code request.getAttributes().get(AiAdvisorRequest.ATTR_HOP_GUI)} and {@code
   * request.getArtifact()}.
   */
  default void afterApply(AiAdvisorRequest request, List<AiProposal> applied) {
    // no-op
  }

  /**
   * @return false when this advisor must not be offered (master switch off, missing GUI, …)
   */
  default boolean isAvailable() {
    return true;
  }
}
