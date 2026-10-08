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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.AnthropicProvider;
import org.apache.hop.ai.providers.MistralProvider;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class AiProposalSchemaTest {

  @Test
  void theSchemaIsStrict() {
    JsonSchema schema = AiProposalSchema.schema();
    JsonObjectSchema root = (JsonObjectSchema) schema.rootElement();
    assertEquals(List.of("answer", "proposals"), root.required());
    assertEquals(Boolean.FALSE, root.additionalProperties());
    assertTrue(schema.name().matches("[A-Za-z0-9_-]+"), "OpenAI accepts only these characters");
  }

  @Test
  void theTypesAreAnEnumOfAllProposalTypes() {
    String json = AiProposalSchema.schema().toString();
    for (AiProposalTypes type : AiProposalTypes.values()) {
      assertTrue(json.contains(type.name()), type.name());
    }
    assertNotNull(JsonEnumSchema.class);
  }

  @Test
  void aStructuredAnswerBecomesTheUsualText() {
    String structured =
        """
        {"answer": "Add a **Dummy** after Output.",
         "proposals": [
           {"id": "1", "description": "Add Dummy", "riskLevel": "LOW", "type": "ADD_TRANSFORM",
            "parameters": [{"name": "transformPluginId", "value": "Dummy"},
                           {"name": "name", "value": "Dummy"},
                           {"name": "config", "value": "{\\"a\\":1}"}]}]}
        """;
    String text = AiProposalSchema.toAnswerText(structured);
    assertTrue(text.startsWith("Add a **Dummy** after Output."), text);
    assertTrue(text.contains("```hop_proposals"), text);

    AiAdvisorResponse response = new PipelineAiAdvisor().parseResponse(text);
    assertNull(response.getProposalParseError());
    assertEquals(1, response.getProposals().size());
    AiProposal proposal = response.getProposals().get(0);
    assertEquals("ADD_TRANSFORM", proposal.getType());
    assertEquals("Dummy", proposal.getParameters().get("transformPluginId"));
    assertEquals("{\"a\":1}", proposal.getParameters().get("config"));
  }

  @Test
  void proposalsRepeatedInTheAnswerAreLeftOut() {
    String structured =
        """
        {"answer": "Add Notify.\\n\\n```json\\n{\\"proposals\\": [{\\"type\\": \\"DELETE_ACTION\\"}]}\\n```\\n\\nSQL:\\n```sql\\nSELECT 1\\n```",
         "proposals": [
           {"id": "1", "description": "Add Notify", "riskLevel": "LOW", "type": "ADD_ACTION",
            "parameters": [{"name": "actionPluginId", "value": "DUMMY"},
                           {"name": "name", "value": "Notify"}]}]}
        """;
    String text = AiProposalSchema.toAnswerText(structured);
    assertFalse(text.contains("DELETE_ACTION"), text);
    assertTrue(text.contains("```sql\nSELECT 1\n```"), "other code blocks stay: " + text);
    AiAdvisorResponse response = new PipelineAiAdvisor().parseResponse(text);
    assertEquals(1, response.getProposals().size());
    assertEquals("ADD_ACTION", response.getProposals().get(0).getType());
  }

  @Test
  void anAnswerWithoutProposalsHasNoBlock() {
    assertEquals(
        "Nothing to change.",
        AiProposalSchema.toAnswerText("{\"answer\": \"Nothing to change.\", \"proposals\": []}"));
  }

  @Test
  void textThatIsNotTheObjectIsLeftAsItIs() {
    String text = "Plain answer\n```hop_proposals\n{\"proposals\": []}\n```";
    assertEquals(text, AiProposalSchema.toAnswerText(text));
  }

  @Test
  void proposalsOutsideTheSchemaAreReported() {
    AiProposal good = proposal("ADD_TRANSFORM", "LOW");
    AiProposal unknownType = proposal("ADD_THING", "LOW");
    AiProposal badRisk = proposal("ADD_TRANSFORM", "SEVERE");
    AiProposal noParameters = new AiProposal();
    noParameters.setType("DELETE_TRANSFORM");

    assertNull(AiProposalSchema.check(List.of(good)));
    String problems = AiProposalSchema.check(List.of(good, unknownType, badRisk, noParameters));
    assertNotNull(problems);
    assertFalse(problems.contains("proposal 1:"), problems);
    assertTrue(problems.contains("proposal 2: type 'ADD_THING'"), problems);
    assertTrue(problems.contains("proposal 3: riskLevel 'SEVERE'"), problems);
    assertTrue(problems.contains("proposal 4: it has no parameters"), problems);
  }

  @Test
  void theAnswerFormatIsAddedToTheInstructions() throws Exception {
    String system = AiAdvisorEngine.structuredSystemPrompt("You are a Hop assistant.");
    assertTrue(system.startsWith("You are a Hop assistant."));
    assertTrue(system.contains("\"answer\""), system);
  }

  @Test
  void anthropicAndMistralCanBeHeldToASchema() throws Exception {
    AiProvider anthropic = new AiProvider();
    anthropic.setName("claude");
    AnthropicProvider anthropicBackend = new AnthropicProvider();
    anthropicBackend.setPluginId("anthropic");
    anthropic.setProvider(anthropicBackend);
    anthropic.setApiKey("key");
    assertTrue(
        AiChatModelFactory.createChatModel(anthropic, "some-model", new Variables())
            .supportedCapabilities()
            .contains(Capability.RESPONSE_FORMAT_JSON_SCHEMA));

    AiProvider mistral = new AiProvider();
    mistral.setName("mistral");
    MistralProvider mistralBackend = new MistralProvider();
    mistralBackend.setPluginId("mistral");
    mistral.setProvider(mistralBackend);
    mistral.setApiKey("key");
    assertTrue(
        AiChatModelFactory.createChatModel(mistral, "some-model", new Variables())
            .supportedCapabilities()
            .contains(Capability.RESPONSE_FORMAT_JSON_SCHEMA));
  }

  @Test
  void onlyTheHopAssistantsAreHeldToTheSchema() {
    assertTrue(AiAdvisorEngine.usesHopProposalSchema(new PipelineAiAdvisor()));
    assertTrue(
        AiAdvisorEngine.usesHopProposalSchema(
            new org.apache.hop.ai.advisors.workflow.WorkflowAiAdvisor()));
    // Another plugin's assistant, with proposal types of its own.
    assertFalse(AiAdvisorEngine.usesHopProposalSchema(null));
  }

  @Test
  void onlyARefusedSchemaIsAskedAgainWithoutIt() {
    assertTrue(
        AiAdvisorEngine.schemaRefused(
            new org.apache.hop.core.exception.HopException(
                "AI request failed",
                new dev.langchain4j.exception.InvalidRequestException("format not supported"))));
    assertTrue(
        AiAdvisorEngine.schemaRefused(
            new dev.langchain4j.exception.UnsupportedFeatureException("json schema")));
    assertFalse(
        AiAdvisorEngine.schemaRefused(
            new org.apache.hop.core.exception.HopException(
                "AI request failed",
                new dev.langchain4j.exception.TimeoutException("request timed out"))));
    assertFalse(
        AiAdvisorEngine.schemaRefused(
            new org.apache.hop.core.exception.HopException(
                "AI request failed",
                new dev.langchain4j.exception.AuthenticationException("401"))));
    assertFalse(
        AiAdvisorEngine.schemaRefused(new dev.langchain4j.exception.RateLimitException("429")));
  }

  @Test
  void anEmptyRepairWithdrawsTheProposals() {
    AiAdvisorResponse empty = new AiAdvisorResponse();
    empty.setProposals(new java.util.ArrayList<>());
    assertTrue(AiAdvisorEngine.withdrawn(empty), "the model confirms it meant no change");

    AiAdvisorResponse failed = new AiAdvisorResponse();
    failed.setProposalParseError("still broken");
    assertFalse(AiAdvisorEngine.withdrawn(failed));

    AiAdvisorResponse repaired = new AiAdvisorResponse();
    repaired.setProposals(new java.util.ArrayList<>(List.of(proposal("ADD_TRANSFORM", "LOW"))));
    assertFalse(AiAdvisorEngine.withdrawn(repaired));
  }

  private static AiProposal proposal(String type, String risk) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setRiskLevel(risk);
    proposal.getParameters().put("transformName", "Dummy");
    return proposal;
  }
}
