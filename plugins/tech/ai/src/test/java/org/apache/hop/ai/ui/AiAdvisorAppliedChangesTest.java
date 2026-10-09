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

import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.engine.AiUserException;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.junit.jupiter.api.Test;

/**
 * Changes applied after an answer are told to the model with the next question that is answered.
 */
class AiAdvisorAppliedChangesTest {

  @Test
  void aFailedQuestionKeepsTheAppliedChangesForTheRetry() {
    AiAdvisorSession session = new AiAdvisorSession();
    session.getPendingAppliedSummaries().add("ADD_TRANSFORM: Check (Dummy)");
    AiAdvisorTurn turn = sent(session);

    AiAdvisorSessionPane.recordResult(
        session, turn, null, new AiUserException("The question is too large"), false);
    assertEquals(List.of("ADD_TRANSFORM: Check (Dummy)"), session.getPendingAppliedSummaries());

    AiAdvisorTurn cancelled = sent(session);
    AiAdvisorSessionPane.recordResult(session, cancelled, null, null, true);
    assertEquals(List.of("ADD_TRANSFORM: Check (Dummy)"), session.getPendingAppliedSummaries());
  }

  @Test
  void anAnswerTakesOnlyWhatItWasSentWith() {
    AiAdvisorSession session = new AiAdvisorSession();
    session.getPendingAppliedSummaries().add("ADD_TRANSFORM: Check (Dummy)");
    AiAdvisorTurn turn = sent(session);
    // Applied from an earlier answer while this question was waiting.
    session.getPendingAppliedSummaries().add("DELETE_TRANSFORM: Old");

    AiAdvisorResponse response = new AiAdvisorResponse();
    response.setMarkdownAdvice("Done.");
    AiAdvisorSessionPane.recordResult(session, turn, response, null, false);

    assertEquals(List.of("DELETE_TRANSFORM: Old"), session.getPendingAppliedSummaries());
  }

  /** A question as Send leaves it: on the session, with what it was sent with. */
  private static AiAdvisorTurn sent(AiAdvisorSession session) {
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt("And now?");
    turn.setSentAppliedSummaries(List.copyOf(session.getPendingAppliedSummaries()));
    session.addTurn(turn);
    session.setWorking(true);
    return turn;
  }
}
