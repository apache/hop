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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.engine.AiMetadataBackup;
import org.junit.jupiter.api.Test;

class AiAdvisorSessionArchiveTest {

  @Test
  void aSessionSurvivesTheRoundTrip() {
    AiAdvisorSession session = new AiAdvisorSession();
    session.setTitle("orders");
    session.setAdvisorPluginId("pipeline-advisor");
    session.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    session.setProviderName("ollama");
    session.setArtifact(new Object());
    session.setArtifactFilename("/project/orders.hpl");
    session.setFocusNodeName("Read orders");
    session.getInclusions().put("logs", true);
    session.getUserChosenInclusions().add("logs");
    session.getMetadataSelections().add(new AiAdvisorMetadataSelection("rdbms", "sales"));
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt("Why did it fail?");
    turn.setAssistantAdvice("The table is missing.");
    turn.setRawAnswer("The table is missing.\n```hop_proposals\n{}\n```");
    turn.setInputTokenCount(6000);
    turn.setDurationMs(4200L);
    AiProposal proposal = new AiProposal();
    proposal.setType("ADD_TRANSFORM");
    proposal.getParameters().put("name", "Check");
    turn.getProposals().add(proposal);
    turn.getAppliedSummaries().add("ADD_TRANSFORM: Check");
    turn.getMetadataBackups()
        .add(
            new AiMetadataBackup(
                "rdbms", "sales", "{\"name\":\"sales\"}", "{\"name\":\"sales2\"}"));
    turn.getMetadataBackups().add(new AiMetadataBackup("rdbms", "new-one", null, "{}"));
    session.addTurn(turn);

    Map<String, Object> map = AiAdvisorSessionArchive.toMap(session);
    AiAdvisorSession restored = AiAdvisorSessionArchive.fromMap(map);

    assertEquals("orders", restored.getTitle());
    assertEquals(AiAdvisorLocations.PIPELINE_GRAPH, restored.getLocation());
    assertEquals("ollama", restored.getProviderName());
    assertNull(restored.getArtifact(), "the file is found again when it is opened");
    assertEquals("/project/orders.hpl", restored.getArtifactFilename());
    assertEquals("Read orders", restored.getFocusNodeName());
    assertTrue(restored.getInclusions().get("logs"));
    assertTrue(restored.getUserChosenInclusions().contains("logs"));
    assertEquals("sales", restored.getMetadataSelections().get(0).getName());
    AiAdvisorTurn back = restored.getTurns().get(0);
    assertEquals("Why did it fail?", back.getUserPrompt());
    assertEquals(turn.getRawAnswer(), back.getRawAnswer());
    assertEquals(6000, back.getInputTokenCount());
    assertEquals(4200L, back.getDurationMs());
    assertEquals("Check", back.getProposals().get(0).getParameters().get("name"));
    assertEquals("ADD_TRANSFORM: Check", back.getAppliedSummaries().get(0));
    assertEquals(turn.getMetadataBackups(), back.getMetadataBackups(), "undo survives a restart");
  }

  @Test
  void aSavedSessionFindsItsFileWhenItIsOpenedAgain() {
    AiAdvisorSession saved = new AiAdvisorSession();
    saved.setAdvisorPluginId("pipeline-advisor");
    saved.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    saved.setArtifactFilename("/project/orders.hpl");
    AiAdvisorSession restored =
        AiAdvisorSessionArchive.fromMap(AiAdvisorSessionArchive.toMap(saved));

    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    store.add(restored);
    org.apache.hop.ai.advisor.AiAdvisorOpenRequest request =
        new org.apache.hop.ai.advisor.AiAdvisorOpenRequest();
    request.setAdvisorPluginId("pipeline-advisor");
    request.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    request.setArtifact((org.apache.hop.core.file.IHasFilename) () -> "/project/orders.hpl");
    assertEquals(restored, store.open(request));
  }
}
