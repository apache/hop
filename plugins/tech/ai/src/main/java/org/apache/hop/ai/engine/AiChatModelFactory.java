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

import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.mistralai.MistralAiChatModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.apache.hop.ai.metadata.AiModelRole;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Builds a langchain4j chat model from an {@link AiProvider}.
 *
 * <p>{@link AiChatFactory} converts a provider into the Language Model Chat transform's own
 * metadata, which is what that transform needs. A transform that wants to drive the model itself,
 * to constrain it with a schema or to run a tool loop, needs the model object instead, and that is
 * what this hands back.
 */
public final class AiChatModelFactory {

  private static final Class<?> PKG = AiChatModelFactory.class;

  private AiChatModelFactory() {}

  /**
   * Loads the named provider and builds its chat model.
   *
   * @param providerName the {@code AiProvider} to use
   * @param modelName a model that overrides the provider's, or empty to use the provider's own
   * @param variables used to resolve the provider's fields
   * @param metadataProvider where the provider is loaded from
   * @throws HopException when the provider is missing, incomplete, or serves no chat model
   */
  public static ChatModel createChatModel(
      String providerName,
      String modelName,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    AiProvider provider = AiProviderLoader.load(providerName, metadataProvider);
    return createChatModel(provider, modelName, variables);
  }

  public static ChatModel createChatModel(
      AiProvider provider, String modelName, IVariables variables) throws HopException {
    // of() rejects a null or half configured provider, so there is one guard, not two.
    AiProviderSettings settings = AiProviderSettings.of(provider, variables);

    String model = Utils.isEmpty(modelName) ? chatModelName(provider, variables) : modelName;
    if (Utils.isEmpty(model)) {
      model = settings.backend().getDefaultModelName();
    }
    if (Utils.isEmpty(model)) {
      throw new AiUserException(
          BaseMessages.getString(PKG, "AiChatModelFactory.NoModel", provider.getName()));
    }

    return switch (settings.type()) {
      case "OLLAMA" -> ollamaModel(settings, model);
      case "OPEN_AI" -> openAiModel(settings, model);
      case "ANTHROPIC" -> anthropicModel(settings, model);
      case "MISTRAL" -> mistralModel(settings, model);
      default ->
          throw new HopException(
              BaseMessages.getString(PKG, "AiChatModelFactory.UnsupportedType", settings.type()));
    };
  }

  /**
   * The schema as a response format, or null when the model does not accept one. Callers then put
   * the schema in the prompt and check the answer on the way back.
   */
  public static ResponseFormat responseFormatFor(ChatModel model, JsonSchema schema) {
    return model.supportedCapabilities().contains(Capability.RESPONSE_FORMAT_JSON_SCHEMA)
        ? ResponseFormat.builder().type(ResponseFormatType.JSON).jsonSchema(schema).build()
        : null;
  }

  /**
   * The provider's chat model: its {@link AiModelRole#CHAT} entry, falling back to the plain model
   * name, which is what a provider configured before roles existed carries.
   */
  private static String chatModelName(AiProvider provider, IVariables variables) {
    return variables.resolve(provider.resolveModelName(AiModelRole.CHAT));
  }

  /*
   * supportedCapabilities() is not a probe of the provider: langchain4j reports back only what the
   * builder was given. Ollama, OpenAI itself, Anthropic and Mistral declare JSON schema support.
   * Other OpenAI compatible endpoints (Gemini, Grok, custom servers) differ in which schema
   * keywords they accept, so they declare nothing and get the fields in the prompt instead.
   */
  private static ChatModel ollamaModel(AiProviderSettings settings, String model) {
    OllamaChatModel.OllamaChatModelBuilder builder =
        OllamaChatModel.builder()
            .baseUrl(settings.baseUrl())
            .modelName(model)
            .supportedCapabilities(Capability.RESPONSE_FORMAT_JSON_SCHEMA);
    if (settings.timeout() != null) {
      builder.timeout(settings.timeout());
    }
    if (settings.temperature() != null) {
      builder.temperature(settings.temperature());
    }
    if (settings.contextSize() != null) {
      builder.numCtx(settings.contextSize());
    }
    if (settings.maxOutputTokens() != null) {
      builder.numPredict(settings.maxOutputTokens());
    }
    if (settings.think() != null) {
      builder.think(settings.think());
    }
    // Only drops the separate thinking field. With think unset, the model and the Ollama version
    // decide whether the answer carries <think> tags, as before this option existed.
    builder.returnThinking(false);
    return builder.build();
  }

  /** Anthropic holds the model to a schema with its structured outputs. It requires a limit. */
  private static ChatModel anthropicModel(AiProviderSettings settings, String model) {
    AnthropicChatModel.AnthropicChatModelBuilder builder =
        AnthropicChatModel.builder()
            .modelName(model)
            .apiKey(settings.apiKey())
            .supportedCapabilities(Capability.RESPONSE_FORMAT_JSON_SCHEMA)
            .maxTokens(
                settings.maxOutputTokens() != null
                    ? settings.maxOutputTokens()
                    : AiProviderSettings.DEFAULT_ANTHROPIC_MAX_OUTPUT_TOKENS);
    if (!Utils.isEmpty(settings.baseUrl())) {
      builder.baseUrl(settings.baseUrl());
    }
    if (settings.timeout() != null) {
      builder.timeout(settings.timeout());
    }
    if (settings.temperature() != null) {
      builder.temperature(settings.temperature());
    }
    return builder.build();
  }

  private static ChatModel mistralModel(AiProviderSettings settings, String model) {
    MistralAiChatModel.MistralAiChatModelBuilder builder =
        MistralAiChatModel.builder()
            .modelName(model)
            .apiKey(settings.apiKey())
            .supportedCapabilities(Capability.RESPONSE_FORMAT_JSON_SCHEMA);
    if (!Utils.isEmpty(settings.baseUrl())) {
      builder.baseUrl(settings.baseUrl());
    }
    if (settings.timeout() != null) {
      builder.timeout(settings.timeout());
    }
    if (settings.temperature() != null) {
      builder.temperature(settings.temperature());
    }
    if (settings.maxOutputTokens() != null) {
      builder.maxTokens(settings.maxOutputTokens());
    }
    return builder.build();
  }

  private static ChatModel openAiModel(AiProviderSettings settings, String model) {
    OpenAiChatModel.OpenAiChatModelBuilder builder = OpenAiChatModel.builder().modelName(model);
    if (settings.backend() instanceof OpenAiProvider) {
      // Without strict, OpenAI drops additionalProperties and does not hold the model to the
      // schema.
      builder.supportedCapabilities(Capability.RESPONSE_FORMAT_JSON_SCHEMA).strictJsonSchema(true);
    }
    if (!Utils.isEmpty(settings.baseUrl())) {
      builder.baseUrl(settings.baseUrl());
    }
    if (!Utils.isEmpty(settings.apiKey())) {
      builder.apiKey(settings.apiKey());
    }
    if (settings.timeout() != null) {
      builder.timeout(settings.timeout());
    }
    if (settings.temperature() != null) {
      builder.temperature(settings.temperature());
    }
    if (settings.maxOutputTokens() != null) {
      builder.maxTokens(settings.maxOutputTokens());
    }
    return builder.build();
  }
}
