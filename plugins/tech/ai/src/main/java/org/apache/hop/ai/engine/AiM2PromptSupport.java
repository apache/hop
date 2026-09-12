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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

/** Assembles hop_proposals system-prompt supplements and applied-change summaries. */
public final class AiM2PromptSupport {

  static final String PROMPT_ROOT = "/org/apache/hop/ai/prompts/hop-proposals/";

  public static final String ATTR_HOP_GUI = "hopGui";

  private AiM2PromptSupport() {}

  public static String buildSupplement() throws HopException {
    return AiPromptLoader.load(PROMPT_ROOT, "preamble-m2.txt")
        + "\n\n"
        + AiPromptLoader.load(PROMPT_ROOT, "hop-proposals-schema.txt");
  }

  public static void appendAppliedSummaries(StringBuilder prompt, List<String> summaries) {
    if (summaries == null || summaries.isEmpty()) {
      return;
    }
    prompt.append("User applied these graph changes since the previous turn:\n");
    for (String summary : summaries) {
      if (!Utils.isEmpty(summary)) {
        prompt.append("- ").append(summary).append('\n');
      }
    }
    prompt.append('\n');
  }
}
