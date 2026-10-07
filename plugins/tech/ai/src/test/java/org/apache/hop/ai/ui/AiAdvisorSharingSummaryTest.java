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
import org.apache.hop.ai.config.AiRequestOnClose;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
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

  @Test
  void turningLogsOffHoldsUntilTheNextRun() {
    AiAdvisorSession session = new AiAdvisorSession();
    String[] run = {"run-1"};
    session.setRunIdSupplier(() -> run[0]);
    session.getUserChosenInclusions().add(AiAdvisorInclusions.LOGS);
    session.setLogChoiceRunId("run-1");

    AiAdvisorSessionPane.forgetLogChoiceOfEarlierRun(session);
    assertTrue(
        session.getUserChosenInclusions().contains(AiAdvisorInclusions.LOGS),
        "the choice holds for the run it was made for");

    run[0] = "run-2";
    AiAdvisorSessionPane.forgetLogChoiceOfEarlierRun(session);
    assertFalse(
        session.getUserChosenInclusions().contains(AiAdvisorInclusions.LOGS),
        "a new run brings a new log, which switches on again");
  }

  @Test
  void aChoiceMadeBeforeAnyRunEndsWithTheFirstRun() {
    AiAdvisorSession session = new AiAdvisorSession();
    String[] run = {null};
    session.setRunIdSupplier(() -> run[0]);
    session.getUserChosenInclusions().add(AiAdvisorInclusions.LOGS);

    AiAdvisorSessionPane.forgetLogChoiceOfEarlierRun(session);
    assertTrue(session.getUserChosenInclusions().contains(AiAdvisorInclusions.LOGS));

    run[0] = "run-1";
    AiAdvisorSessionPane.forgetLogChoiceOfEarlierRun(session);
    assertFalse(session.getUserChosenInclusions().contains(AiAdvisorInclusions.LOGS));
  }

  @Test
  void theTranscriptKeepsFourLinesInALowPane() {
    int line = 16;
    int trim = 4;
    // Plenty of room: three lines minimum.
    assertEquals(3 * line + trim, AiAdvisorSessionPane.promptHeight(line, line, trim, 800, 700));
    // A low dock: the question field gives way so the transcript keeps four lines.
    assertEquals(100 - 4 * line, AiAdvisorSessionPane.promptHeight(line, line, trim, 200, 100));
    // Very low: the question field keeps one line.
    assertEquals(line + trim, AiAdvisorSessionPane.promptHeight(line, line, trim, 90, 40));
  }

  @Test
  void closingAWindowCancelsTheQuestionOnlyWhenConfigured() {
    AiAdvisorSession waiting = waitingSession();
    assertFalse(
        AiAdvisorSessionPane.cancelOnClose(
            List.of(waiting), AiRequestOnClose.FINISH_IN_BACKGROUND, false));
    assertTrue(waiting.isWorking(), "by default the question finishes in the background");

    assertFalse(
        AiAdvisorSessionPane.cancelOnClose(List.of(waiting), AiRequestOnClose.CANCEL, true));
    assertTrue(waiting.isWorking(), "moving the assistant with Float or Dock is not a close");

    assertTrue(
        AiAdvisorSessionPane.cancelOnClose(List.of(waiting), AiRequestOnClose.CANCEL, false));
    assertFalse(waiting.isWorking());
    assertTrue(waiting.isCancelled());
    assertFalse(waiting.getTurns().get(0).getErrorMessage().isEmpty(), "the turn says so");
  }

  private static AiAdvisorSession waitingSession() {
    AiAdvisorSession session = new AiAdvisorSession();
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt("Why did it fail?");
    session.addTurn(turn);
    session.setWorking(true);
    return session;
  }
}
