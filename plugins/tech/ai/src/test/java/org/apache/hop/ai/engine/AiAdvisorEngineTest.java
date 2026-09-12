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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import java.util.List;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.junit.jupiter.api.Test;

class AiAdvisorEngineTest {

  @Test
  void historySkipsFailedTurnsSoRolesAlternate() {
    AiAdvisorSession session = new AiAdvisorSession();
    AiAdvisorTurn failed = new AiAdvisorTurn();
    failed.setUserPrompt("first");
    failed.setErrorMessage("timeout");
    session.addTurn(failed);
    AiAdvisorTurn ok = new AiAdvisorTurn();
    ok.setUserPrompt("retry");
    ok.setAssistantAdvice("here is the answer");
    session.addTurn(ok);
    AiAdvisorTurn current = new AiAdvisorTurn();
    current.setUserPrompt("follow up");
    session.addTurn(current);

    List<ChatMessage> history = AiAdvisorEngine.historyFrom(session);
    assertEquals(2, history.size());
    assertTrue(history.get(0) instanceof UserMessage);
    assertTrue(history.get(1) instanceof AiMessage);
    assertTrue(AiAdvisorEngine.hasSuccessfulPriorTurn(session));
  }

  @Test
  void followUpIsFalseWhenNoPriorAdvice() {
    AiAdvisorSession session = new AiAdvisorSession();
    AiAdvisorTurn failed = new AiAdvisorTurn();
    failed.setUserPrompt("first");
    failed.setErrorMessage("timeout");
    session.addTurn(failed);
    AiAdvisorTurn current = new AiAdvisorTurn();
    current.setUserPrompt("retry");
    session.addTurn(current);
    assertFalse(AiAdvisorEngine.hasSuccessfulPriorTurn(session));
    assertTrue(AiAdvisorEngine.historyFrom(session).isEmpty());
  }
}
