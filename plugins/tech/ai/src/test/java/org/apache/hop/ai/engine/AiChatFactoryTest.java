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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.AnthropicProvider;
import org.apache.hop.ai.providers.GrokProvider;
import org.apache.hop.ai.providers.HuggingFaceProvider;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.junit.jupiter.api.Test;

class AiChatFactoryTest {

  @Test
  void mapsOpenAiCompatibleFields() throws Exception {
    AiProvider provider = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    backend.setPluginName("OpenAI");
    provider.setProvider(backend);
    provider.setBaseUrl("https://api.openai.com/v1");
    provider.setApiKey("sk-test");
    provider.setModelName("gpt-4o-mini");
    provider.setTemperature("0.2");
    provider.setTimeoutSeconds("45");

    LanguageModelChatMeta meta = AiChatFactory.toLanguageModelChatMeta(provider, new Variables());
    assertEquals("OPEN_AI", meta.getModelType());
    assertEquals("https://api.openai.com/v1", meta.getOpenAiBaseUrl());
    assertEquals("sk-test", meta.getOpenAiApiKey());
    assertEquals("gpt-4o-mini", meta.getOpenAiModelName());
    assertEquals(0.2, meta.getOpenAiTemperature());
    assertEquals(45, meta.getOpenAiTimeout());
  }

  @Test
  void mapsGrokOntoOpenAi() throws Exception {
    AiProvider provider = new AiProvider();
    GrokProvider backend = new GrokProvider();
    backend.setPluginId("grok");
    backend.setPluginName("Grok (xAI)");
    provider.setProvider(backend);
    provider.setApiKey("xai-key");

    LanguageModelChatMeta meta = AiChatFactory.toLanguageModelChatMeta(provider, new Variables());
    assertEquals("OPEN_AI", meta.getModelType());
    assertEquals("https://api.x.ai/v1", meta.getOpenAiBaseUrl());
    assertEquals("grok-4", meta.getOpenAiModelName());
    assertEquals("xai-key", meta.getOpenAiApiKey());
  }

  @Test
  void mapsAnthropicAndOllama() throws Exception {
    AiProvider anthropic = new AiProvider();
    AnthropicProvider anthropicBackend = new AnthropicProvider();
    anthropicBackend.setPluginId("anthropic");
    anthropic.setProvider(anthropicBackend);
    anthropic.setApiKey("claude-key");
    anthropic.setModelName("claude-3-5-sonnet-20241022");
    LanguageModelChatMeta anthropicMeta =
        AiChatFactory.toLanguageModelChatMeta(anthropic, new Variables());
    assertEquals("ANTHROPIC", anthropicMeta.getModelType());
    assertEquals("claude-key", anthropicMeta.getAnthropicApiKey());

    AiProvider ollama = new AiProvider();
    OllamaProvider ollamaBackend = new OllamaProvider();
    ollamaBackend.setPluginId("ollama");
    ollama.setProvider(ollamaBackend);
    LanguageModelChatMeta ollamaMeta =
        AiChatFactory.toLanguageModelChatMeta(ollama, new Variables());
    assertEquals("OLLAMA", ollamaMeta.getModelType());
    assertEquals("http://localhost:11434", ollamaMeta.getOllamaImageEndpoint());
    assertEquals("llama3.2", ollamaMeta.getOllamaModelName());
  }

  @Test
  void huggingFaceDedicatedEndpointFallsBackToBaseUrl() throws Exception {
    AiProvider provider = new AiProvider();
    HuggingFaceProvider backend = new HuggingFaceProvider();
    backend.setPluginId("hugging-face");
    provider.setProvider(backend);
    provider.setApiKey("hf-token");
    provider.setBaseUrl("https://xyz.endpoints.huggingface.cloud");
    provider.setModelName("");

    LanguageModelChatMeta meta = AiChatFactory.toLanguageModelChatMeta(provider, new Variables());
    assertEquals("HUGGING_FACE", meta.getModelType());
    assertEquals("hf-token", meta.getHuggingFaceAccessToken());
    assertEquals("https://xyz.endpoints.huggingface.cloud", meta.getHuggingFaceModelId());
  }

  @Test
  void huggingFaceModelNameWinsOverBaseUrl() throws Exception {
    AiProvider provider = new AiProvider();
    HuggingFaceProvider backend = new HuggingFaceProvider();
    backend.setPluginId("hugging-face");
    provider.setProvider(backend);
    provider.setApiKey("hf-token");
    provider.setBaseUrl("https://xyz.endpoints.huggingface.cloud");
    provider.setModelName("meta-llama/Llama-3.3-70B-Instruct");

    LanguageModelChatMeta meta = AiChatFactory.toLanguageModelChatMeta(provider, new Variables());
    assertEquals("meta-llama/Llama-3.3-70B-Instruct", meta.getHuggingFaceModelId());
  }

  @Test
  void resolvesVariables() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("AI_KEY", "from-var");
    AiProvider provider = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    provider.setProvider(backend);
    provider.setApiKey("${AI_KEY}");
    LanguageModelChatMeta meta = AiChatFactory.toLanguageModelChatMeta(provider, variables);
    assertEquals("from-var", meta.getOpenAiApiKey());
  }

  @Test
  void validateRequiresApiKey() {
    AiProvider provider = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    provider.setProvider(backend);
    HopException e = assertThrows(HopException.class, () -> AiChatFactory.validate(provider));
    assertTrue(e.getMessage().contains("API key"));
  }

  @Test
  void overlayNamedProviderAppliesConnectionAndKeepsInlineFallbacks() throws Exception {
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    AiProvider provider = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    backend.setPluginName("OpenAI");
    provider.setName("prod-openai");
    provider.setProvider(backend);
    provider.setBaseUrl("https://api.openai.com/v1");
    provider.setApiKey("sk-prod");
    provider.setModelName("gpt-4o");
    provider.setTemperature("0.1");
    provider.setTimeoutSeconds("");
    metadata.getSerializer(AiProvider.class).save(provider);

    LanguageModelChatMeta source = new LanguageModelChatMeta();
    source.setDefault();
    source.setOpenAiApiKey("inline-key");
    source.setOpenAiModelName("gpt-inline");
    source.setOpenAiTemperature(0.7);
    source.setOpenAiTimeout(90);
    source.setOpenAiMaxRetries(7);
    source.setMock(true);
    source.setInputField("prompt");
    source.setAiProviderName("prod-openai");

    LanguageModelChatMeta overlaid =
        AiChatFactory.overlayNamedProvider(source, "prod-openai", new Variables(), metadata);

    assertNotSame(source, overlaid);
    assertEquals("inline-key", source.getOpenAiApiKey());
    assertEquals("gpt-inline", source.getOpenAiModelName());
    assertEquals("OPEN_AI", overlaid.getModelType());
    assertEquals("https://api.openai.com/v1", overlaid.getOpenAiBaseUrl());
    assertEquals("sk-prod", overlaid.getOpenAiApiKey());
    assertEquals("gpt-4o", overlaid.getOpenAiModelName());
    assertEquals(0.1, overlaid.getOpenAiTemperature());
    assertEquals(90, overlaid.getOpenAiTimeout());
    assertEquals(7, overlaid.getOpenAiMaxRetries());
    assertTrue(overlaid.isMock());
    assertEquals("prompt", overlaid.getInputField());
  }

  @Test
  void overlayNamedProviderKeepsInlineWhenProviderFieldsEmpty() throws Exception {
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    AiProvider provider = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    provider.setName("partial");
    provider.setProvider(backend);
    provider.setApiKey("");
    provider.setModelName("");
    provider.setTemperature("");
    provider.setTimeoutSeconds("");
    metadata.getSerializer(AiProvider.class).save(provider);

    LanguageModelChatMeta source = new LanguageModelChatMeta();
    source.setDefault();
    source.setOpenAiBaseUrl("https://inline.example/v1");
    source.setOpenAiApiKey("inline-key");
    source.setOpenAiModelName("gpt-inline");
    source.setOpenAiTemperature(0.9);
    source.setOpenAiTimeout(12);

    LanguageModelChatMeta overlaid =
        AiChatFactory.overlayNamedProvider(source, "partial", new Variables(), metadata);

    assertEquals("OPEN_AI", overlaid.getModelType());
    assertEquals("https://inline.example/v1", overlaid.getOpenAiBaseUrl());
    assertEquals("inline-key", overlaid.getOpenAiApiKey());
    assertEquals("gpt-inline", overlaid.getOpenAiModelName());
    assertEquals(0.9, overlaid.getOpenAiTemperature());
    assertEquals(12, overlaid.getOpenAiTimeout());
  }

  @Test
  void overlayNamedProviderBlankNameReturnsSource() throws Exception {
    LanguageModelChatMeta source = new LanguageModelChatMeta();
    source.setDefault();
    assertSame(source, AiChatFactory.overlayNamedProvider(source, "  ", new Variables(), null));
    assertSame(source, AiChatFactory.overlayNamedProvider(source, "", new Variables(), null));
  }

  @Test
  void overlayNamedProviderMissingObjectThrows() {
    LanguageModelChatMeta source = new LanguageModelChatMeta();
    source.setDefault();
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                AiChatFactory.overlayNamedProvider(
                    source, "missing", new Variables(), new MemoryMetadataProvider()));
    assertTrue(e.getMessage().contains("missing"));
  }

  @Test
  void parseHelpers() {
    assertEquals(0.3, AiChatFactory.parseTemperature(null));
    assertEquals(0.3, AiChatFactory.parseTemperature("nope"));
    assertEquals(0.7, AiChatFactory.parseTemperature("0.7"));
    assertEquals(60, AiChatFactory.parseTimeout("60"));
  }
}
