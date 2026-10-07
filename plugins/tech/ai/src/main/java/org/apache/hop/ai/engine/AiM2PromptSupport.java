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

import java.util.List;
import java.util.TreeSet;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

/** Assembles hop_proposals system-prompt supplements and applied-change summaries. */
public final class AiM2PromptSupport {

  static final String PROMPT_ROOT = "/org/apache/hop/ai/prompts/hop-proposals/";

  public static final String ATTR_HOP_GUI = AiAdvisorRequest.ATTR_HOP_GUI;

  private AiM2PromptSupport() {}

  public static String buildSupplement() throws HopException {
    return AiPromptLoader.load(PROMPT_ROOT, "preamble-m2.txt")
        + "\n\n"
        + AiPromptLoader.load(PROMPT_ROOT, "hop-proposals-schema.txt")
        + "\n"
        + savableMetadata();
  }

  /**
   * Which metadata a proposal can save, from the list the review enforces, so the model does not
   * propose a run configuration or a server only to see it blocked, or offers something else.
   */
  static String savableMetadata() throws HopException {
    return AiPromptLoader.load(PROMPT_ROOT, "savable-metadata.txt")
        .replace(
            "{types}",
            String.join(", ", new TreeSet<>(AiMetadataProposalSupport.SAVABLE_TYPE_KEYS)));
  }

  public static void appendAppliedSummaries(StringBuilder prompt, List<String> summaries) {
    if (summaries == null || summaries.isEmpty()) {
      return;
    }
    StringBuilder applied = new StringBuilder();
    for (String summary : summaries) {
      if (!Utils.isEmpty(summary)) {
        applied.append("- ").append(summary).append('\n');
      }
    }
    // The user applied these proposals to the graph since the previous question.
    AiTextUtil.appendSection(prompt, "applied_changes", applied.toString());
  }
}
