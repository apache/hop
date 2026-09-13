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
