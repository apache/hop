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
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.engine.AiMetadataBackup;

/** One user/assistant exchange in an {@link AiAdvisorSession}. */
@Getter
@Setter
public class AiAdvisorTurn {
  private String userPrompt = "";
  private String assistantAdvice = "";

  /**
   * The answer as the model wrote it, proposal block included. The conversation history replays
   * this: with the block stripped the model learns that its own answers end in an empty example.
   */
  private String rawAnswer;

  private String errorMessage;
  private List<AiProposal> proposals = new ArrayList<>();
  private List<String> appliedSummaries = new ArrayList<>();
  private boolean proposalBlockPresent;
  private String proposalParseError;

  /** When the question was sent, for the seconds counter while waiting. */
  private long startedAtMillis;

  /** Rough size of what was sent, shown while waiting; null when not known. */
  private Integer estimatedPromptTokens;

  /** The provider and model the question went to, shown while waiting. */
  private String providerLabel;

  /**
   * The applied-change summaries this question was sent with, taken off the session once its answer
   * is recorded. Not kept between Hop GUI runs.
   */
  private List<String> sentAppliedSummaries = new ArrayList<>();

  private List<AiMetadataBackup> metadataBackups = new ArrayList<>();
  private Integer inputTokenCount;
  private Integer outputTokenCount;
  private Long durationMs;
}
