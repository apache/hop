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
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.ai.ui.AiAdvisorSessionPane;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
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

  @Test
  void toRequestCopiesSessionAttributesAndInclusionSelections() {
    AiAdvisorSession session = new AiAdvisorSession();
    session.setLocation("data-vault-graph");
    session.getAttributes().put("modelId", "sales");
    session.getInclusionSelections().put("catalog", List.of("SRC_ORDERS", "SRC_CUSTOMER"));
    session.getInclusions().put("catalog", true);
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt("hub grain?");
    session.addTurn(turn);

    AiAdvisorRequest request = AiAdvisorEngine.toRequest(session, null, null, "log");
    assertEquals("data-vault-graph", request.getLocation());
    assertEquals("sales", request.getAttributes().get("modelId"));
    assertEquals(List.of("SRC_ORDERS", "SRC_CUSTOMER"), request.selectedInclusionIds("catalog"));
    assertTrue(request.inclusionEnabled("catalog"));
    assertEquals("log", request.getLogExcerpt());
    assertEquals("hub grain?", request.getUserPrompt());
    request.getAttributes().put("mutated", true);
    request.getInclusionSelections().get("catalog").add("SRC_EXTRA");
    assertFalse(session.getAttributes().containsKey("mutated"));
    assertEquals(
        List.of("SRC_ORDERS", "SRC_CUSTOMER"), session.getInclusionSelections().get("catalog"));
  }

  @Test
  void previewShowsTheNextQuestionWithoutConsumingAnything() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("orders");
    AiAdvisorSession session = new AiAdvisorSession();
    session.setArtifact(pipelineMeta);
    AiAdvisorTurn answered = new AiAdvisorTurn();
    answered.setUserPrompt("What does it do?");
    answered.setAssistantAdvice("It copies orders.");
    session.addTurn(answered);
    session.getPendingAppliedSummaries().add("ADD_TRANSFORM: Check (Dummy)");

    String preview =
        AiAdvisorEngine.preview(
            session, new PipelineAiAdvisor(), new Variables(), null, null, "Why did it fail?");

    assertTrue(preview.contains("<question>\nWhy did it fail?\n</question>"), preview);
    assertTrue(preview.contains("<applied_changes>"), preview);
    assertTrue(preview.contains("1 earlier question(s)"), preview);
    assertTrue(preview.contains("Answer in the language of the user's question."), preview);
    assertEquals(1, session.getPendingAppliedSummaries().size(), "a preview must not consume");
    assertEquals(1, session.getTurns().size(), "a preview must not add a turn");
  }

  @Test
  void deletesAndReplacementsAreOptIn() {
    List<AiProposal> proposals =
        List.of(
            proposal("DELETE_TRANSFORM"), proposal("ADD_TRANSFORM"), proposal("REPLACE_ACTION"));
    List<AiProposalValidation> validations =
        List.of(new AiProposalValidation(), new AiProposalValidation(), new AiProposalValidation());
    AiAdvisorSessionPane.markOptIn(proposals, validations);
    assertTrue(validations.get(0).isOptIn());
    assertFalse(validations.get(1).isOptIn());
    assertTrue(validations.get(2).isOptIn());
  }

  @Test
  void settingsChangesAndHighRiskProposalsAreOptIn() {
    AiProposal highRiskAdd = proposal("ADD_TRANSFORM");
    highRiskAdd.setRiskLevel("HIGH");
    AiProposal lowRiskAdd = proposal("ADD_TRANSFORM");
    lowRiskAdd.setRiskLevel("LOW");
    List<AiProposal> proposals =
        List.of(
            proposal("CONFIGURE_TRANSFORM"), proposal("CONFIGURE_ACTION"), highRiskAdd, lowRiskAdd);
    List<AiProposalValidation> validations =
        List.of(
            new AiProposalValidation(),
            new AiProposalValidation(),
            new AiProposalValidation(),
            new AiProposalValidation());
    AiAdvisorSessionPane.markOptIn(proposals, validations);
    assertTrue(validations.get(0).isOptIn());
    assertTrue(validations.get(1).isOptIn());
    assertTrue(validations.get(2).isOptIn());
    assertFalse(validations.get(3).isOptIn());
  }

  private static AiProposal proposal(String type) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    return proposal;
  }

  @Test
  void historyReplaysTheAnswerWithACleanProposalBlock() {
    // Without the block a small model learns to end with an empty example; with its original,
    // broken block it repeats the mistakes. It gets the proposals as they were read.
    AiAdvisorSession session = new AiAdvisorSession();
    AiAdvisorTurn earlier = new AiAdvisorTurn();
    earlier.setUserPrompt("Add a Dummy");
    earlier.setAssistantAdvice("Here is the proposal:");
    earlier.setRawAnswer(
        "Here is the proposal:\n```hop_proposals\n{\"proposals\":[{\"type\":\"A|B\"}]}\n```");
    AiProposal add = proposal("ADD_TRANSFORM");
    add.getParameters().put("transformPluginId", "Dummy");
    earlier.getProposals().add(add);
    session.addTurn(earlier);
    AiAdvisorTurn current = new AiAdvisorTurn();
    current.setUserPrompt("And another one");
    session.addTurn(current);

    List<ChatMessage> history = AiAdvisorEngine.historyFrom(session);
    String answer = ((AiMessage) history.get(1)).text();
    assertTrue(answer.contains("```hop_proposals"), answer);
    assertTrue(answer.contains("\"transformPluginId\":\"Dummy\""), answer);
    assertFalse(answer.contains("A|B"), answer);
  }

  @Test
  void answersThatTalkAboutProposalsAreNoticed() {
    assertTrue(AiAdvisorEngine.mentionsProposals("Add a new ADD_TRANSFORM proposal for Dummy."));
    assertTrue(AiAdvisorEngine.mentionsProposals("see the hop_proposals block"));
    assertTrue(AiAdvisorEngine.mentionsProposals("- **transformPluginId**: `Dummy`"));
    assertFalse(AiAdvisorEngine.mentionsProposals("This pipeline reads orders."));
  }
}
