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

package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.junit.jupiter.api.Test;

class AiAdvisorSharingSummaryTest {

  @Test
  void questionOnly() {
    assertEquals(
        "Sharing: your question",
        AiAdvisorSessionPane.formatSharingLine("Sharing: ", List.of("your question")));
  }

  @Test
  void questionPlusOptInExtras() {
    assertEquals(
        "Sharing: your question, graph structure, check results",
        AiAdvisorSessionPane.formatSharingLine(
            "Sharing: ", List.of("your question", "graph structure", "check results")));
  }

  @Test
  void fullXmlIsBlockedUntilTheGlobalOptionAllowsIt() {
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    boolean original = config.isAllowSendFullXml();
    try {
      config.setAllowSendFullXml(false);
      assertTrue(AiAdvisorSessionPane.isBlockedByConfig(AiAdvisorInclusions.XML));
      assertFalse(AiAdvisorSessionPane.isBlockedByConfig(AiAdvisorInclusions.LOGS));

      config.setAllowSendFullXml(true);
      assertFalse(AiAdvisorSessionPane.isBlockedByConfig(AiAdvisorInclusions.XML));
    } finally {
      config.setAllowSendFullXml(original);
    }
  }

  @Test
  void upAndDownBrowseTheQuestionsNewestFirst() {
    org.apache.hop.ai.session.AiAdvisorSession session =
        new org.apache.hop.ai.session.AiAdvisorSession();
    for (String question : List.of("first", "second", "second", "third")) {
      org.apache.hop.ai.session.AiAdvisorTurn turn = new org.apache.hop.ai.session.AiAdvisorTurn();
      turn.setUserPrompt(question);
      session.addTurn(turn);
    }
    assertEquals(
        List.of("third", "second", "first"), AiAdvisorSessionPane.earlierQuestions(session));
  }

  @Test
  void everyBasicItemIsExplained() {
    assertTrue(AiAdvisorSessionPane.explainBasic("graph structure").contains("hops"));
    assertTrue(AiAdvisorSessionPane.explainBasic("pipeline-advisor.md").contains("AI plugin"));
    assertTrue(AiAdvisorSessionPane.explainBasic("my-notes.md").contains("Context files"));
  }
}
