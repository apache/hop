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
   */
  AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException;

  /**
   * Parse raw assistant text into advice and optional proposals. Chat-only advisors can return the
   * trimmed text with an empty proposal list.
   */
  default AiAdvisorResponse parseResponse(String raw) {
    AiAdvisorResponse response = new AiAdvisorResponse();
    response.setRawResponse(raw);
    response.setMarkdownAdvice(raw != null ? raw.trim() : "");
    return response;
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
   * @return false when this advisor must not be offered (master switch off, missing GUI, …)
   */
  default boolean isAvailable() {
    return true;
  }
}
