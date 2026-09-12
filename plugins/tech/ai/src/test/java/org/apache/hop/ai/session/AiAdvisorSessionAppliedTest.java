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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiProposal;
import org.junit.jupiter.api.Test;

class AiAdvisorSessionAppliedTest {

  @Test
  void recordAppliedIsConsumedOnce() {
    AiAdvisorSession session = new AiAdvisorSession();
    AiAdvisorTurn turn = new AiAdvisorTurn();
    session.addTurn(turn);
    AiProposal proposal = new AiProposal();
    proposal.setType("ADD_TRANSFORM");
    proposal.setParameters(Map.of("name", "Check", "transformPluginId", "Dummy"));

    session.recordApplied(turn, List.of(proposal));
    assertEquals(1, turn.getAppliedSummaries().size());
    assertTrue(turn.getAppliedSummaries().get(0).contains("Check"));

    List<String> first = session.consumePendingAppliedSummaries();
    assertEquals(1, first.size());
    assertTrue(session.consumePendingAppliedSummaries().isEmpty());
  }
}
