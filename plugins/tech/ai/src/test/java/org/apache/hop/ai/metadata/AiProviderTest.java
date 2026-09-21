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

package org.apache.hop.ai.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.junit.jupiter.api.Test;

class AiProviderTest {

  @Test
  void aProviderWithoutRolesBehavesExactlyAsBefore() {
    // The compatibility guarantee: anything serialized before roles existed has no models entry,
    // so CHAT still comes from modelName and no other role resolves.
    AiProvider provider = new AiProvider();
    provider.setModelName("gpt-4o-mini");

    assertTrue(provider.getModels().isEmpty());
    assertEquals("gpt-4o-mini", provider.resolveModelName(AiModelRole.CHAT));
    assertEquals("", provider.resolveModelName(AiModelRole.EMBEDDING));
  }

  @Test
  void eachRoleResolvesItsOwnModel() {
    AiProvider provider = new AiProvider();
    provider.setModelName("gpt-4o-mini");
    provider.setModels(
        List.of(
            new AiProviderModel(AiModelRole.CHAT, "llama3.2"),
            new AiProviderModel(AiModelRole.EMBEDDING, "nomic-embed-text"),
            new AiProviderModel(AiModelRole.SCORING, "bge-reranker-v2-m3")));

    assertEquals("llama3.2", provider.resolveModelName(AiModelRole.CHAT));
    assertEquals("nomic-embed-text", provider.resolveModelName(AiModelRole.EMBEDDING));
    assertEquals("bge-reranker-v2-m3", provider.resolveModelName(AiModelRole.SCORING));
    assertEquals("", provider.resolveModelName(AiModelRole.IMAGE));
  }

  @Test
  void theChatModelIsNotAFallbackForTheOtherRoles() {
    // Embedding a text with a chat model fails at the provider with a confusing message. An empty
    // result here lets the caller say what is actually wrong.
    AiProvider provider = new AiProvider();
    provider.setModelName("gpt-4o-mini");
    provider.setModels(List.of(new AiProviderModel(AiModelRole.SCORING, "bge-reranker-v2-m3")));

    assertEquals("", provider.resolveModelName(AiModelRole.EMBEDDING));
  }

  @Test
  void anEmptyModelNameOnARoleDoesNotCount() {
    AiProvider provider = new AiProvider();
    provider.setModelName("gpt-4o-mini");
    provider.setModels(List.of(new AiProviderModel(AiModelRole.CHAT, "")));

    assertEquals("gpt-4o-mini", provider.resolveModelName(AiModelRole.CHAT));
  }

  @Test
  void copyConstructorDeepCopiesTheModels() {
    AiProvider source = new AiProvider();
    source.setModels(List.of(new AiProviderModel(AiModelRole.EMBEDDING, "nomic-embed-text")));

    AiProvider copy = new AiProvider(source);
    copy.getModels().get(0).setModelName("changed");

    assertEquals("nomic-embed-text", source.resolveModelName(AiModelRole.EMBEDDING));
  }

  @Test
  void theRoleEnumReadsBackFromItsDisplayedText() {
    // Generated dialogs fill an enum combo with toString() and read it back with Enum.valueOf.
    for (AiModelRole role : AiModelRole.values()) {
      assertEquals(role.name(), role.toString());
    }
  }

  @Test
  void copyConstructorClonesProvider() {
    AiProvider source = new AiProvider();
    source.setName("prod-openai");
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    backend.setPluginName("OpenAI");
    source.setProvider(backend);
    source.setApiKey("secret");
    source.setModelName("gpt-4o-mini");

    AiProvider copy = new AiProvider(source);
    assertEquals("prod-openai", copy.getName());
    assertEquals("secret", copy.getApiKey());
    assertEquals("gpt-4o-mini", copy.getModelName());
    assertEquals("openai", copy.getPluginId());
    assertEquals("OpenAI", copy.getPluginName());
  }

  @Test
  void applyProviderDefaultsFillsEmptyFields() {
    AiProvider provider = new AiProvider();
    OllamaProvider backend = new OllamaProvider();
    backend.setPluginId("ollama");
    provider.setProvider(backend);
    provider.applyProviderDefaults();
    assertEquals("http://localhost:11434", provider.getBaseUrl());
    assertEquals("llama3.2", provider.getModelName());
    assertFalse(provider.getProvider().requiresApiKey());
  }

  @Test
  void modelNameChoicesIncludeCurrentAndDefault() {
    AiProvider provider = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    provider.setProvider(backend);
    provider.setModelName("gpt-4o");
    List<String> choices = provider.getModelNameChoices(null, null);
    assertTrue(choices.contains("gpt-4o"));
    assertTrue(choices.contains("gpt-4o-mini"));
  }
}
