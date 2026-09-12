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

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelFacade;

/** Maps {@link AiProvider} metadata onto Language Model Chat and runs a completion. */
public final class AiChatFactory {

  private static final String HEALTH_SYSTEM =
      "You are a health-check endpoint. Reply with exactly OK and nothing else.";
  private static final String HEALTH_USER = "health";

  private AiChatFactory() {}

  public static LanguageModelChatMeta toLanguageModelChatMeta(
      AiProvider provider, IVariables variables) throws HopException {
    if (provider == null) {
      throw new HopException("AI provider metadata is missing");
    }
    IAiProvider backend = provider.getProvider();
    if (backend == null) {
      throw new HopException("AI provider type is not set on '" + provider.getName() + "'");
    }
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setDefault();
    meta.setMock(false);
    applyConnection(meta, provider, variables, true);
    return meta;
  }

  /**
   * Clone {@code source} and overlay connection fields from the named AI Provider. Empty provider
   * fields leave the transform's inline values in place.
   */
  public static LanguageModelChatMeta overlayNamedProvider(
      LanguageModelChatMeta source,
      String providerName,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    if (source == null) {
      throw new HopException("Language Model Chat metadata is missing");
    }
    if (providerName == null || providerName.isBlank()) {
      return source;
    }
    if (metadataProvider == null) {
      throw new HopException(
          "No metadata provider is available to load AI Provider '" + providerName + "'");
    }
    String name = resolve(variables, providerName);
    AiProvider provider = metadataProvider.getSerializer(AiProvider.class).load(name);
    if (provider == null) {
      throw new HopException("AI provider '" + name + "' was not found.");
    }
    LanguageModelChatMeta copy = (LanguageModelChatMeta) source.clone();
    applyConnection(copy, provider, variables, false);
    return copy;
  }

  /**
   * Copy connection fields from the provider onto {@code meta}. When {@code useProviderDefaults} is
   * true, empty provider fields fall back to the plugin defaults. When false, empty provider fields
   * leave the existing values on {@code meta} in place.
   */
  private static void applyConnection(
      LanguageModelChatMeta meta,
      AiProvider provider,
      IVariables variables,
      boolean useProviderDefaults)
      throws HopException {
    IAiProvider backend = provider.getProvider();
    if (backend == null) {
      throw new HopException("AI provider type is not set on '" + provider.getName() + "'");
    }
    meta.setModelType(backend.getHopModelType());

    String baseUrl = resolve(variables, provider.getBaseUrl());
    if (Utils.isEmpty(baseUrl) && useProviderDefaults) {
      baseUrl = backend.getDefaultBaseUrl();
    }
    String modelName = resolve(variables, provider.getModelName());
    if (Utils.isEmpty(modelName) && useProviderDefaults) {
      modelName = backend.getDefaultModelName();
    }
    String apiKey = resolve(variables, provider.getApiKey());
    boolean hasTemperature = !Utils.isEmpty(provider.getTemperature());
    double temperature = parseTemperature(resolve(variables, provider.getTemperature()));
    Integer timeout = parseTimeout(resolve(variables, provider.getTimeoutSeconds()));
    boolean applyTemperature = useProviderDefaults || hasTemperature;

    String hopType = backend.getHopModelType();
    if ("ANTHROPIC".equals(hopType)) {
      if (!Utils.isEmpty(apiKey)) {
        meta.setAnthropicApiKey(apiKey);
      }
      if (!Utils.isEmpty(modelName)) {
        meta.setAnthropicModelName(modelName);
      }
      if (applyTemperature) {
        meta.setAnthropicTemperature(temperature);
      }
      if (!Utils.isEmpty(baseUrl)) {
        meta.setAnthropicBaseUrl(baseUrl);
      }
      if (timeout != null) {
        meta.setAnthropicTimeout(timeout);
      }
    } else if ("OLLAMA".equals(hopType)) {
      if (!Utils.isEmpty(baseUrl)) {
        meta.setOllamaImageEndpoint(baseUrl);
      }
      if (!Utils.isEmpty(modelName)) {
        meta.setOllamaModelName(modelName);
      }
      if (applyTemperature) {
        meta.setOllamaTemperature(temperature);
      }
      if (timeout != null) {
        meta.setOllamaTimeout(timeout);
      }
    } else if ("MISTRAL".equals(hopType)) {
      if (!Utils.isEmpty(baseUrl)) {
        meta.setMistralBaseUrl(baseUrl);
      }
      if (!Utils.isEmpty(apiKey)) {
        meta.setMistralApiKey(apiKey);
      }
      if (!Utils.isEmpty(modelName)) {
        meta.setMistralModelName(modelName);
      }
      if (applyTemperature) {
        meta.setMistralTemperature(temperature);
      }
      if (timeout != null) {
        meta.setMistralTimeout(timeout);
      }
    } else if ("HUGGING_FACE".equals(hopType)) {
      if (!Utils.isEmpty(apiKey)) {
        meta.setHuggingFaceAccessToken(apiKey);
      }
      // Dedicated endpoints are often entered as Base URL; Language Model Chat stores both the
      // router model id and a dedicated URL in huggingFaceModelId.
      String hfResource = !Utils.isEmpty(modelName) ? modelName : baseUrl;
      if (!Utils.isEmpty(hfResource)) {
        meta.setHuggingFaceModelId(hfResource);
      }
      if (applyTemperature) {
        meta.setHuggingFaceTemperature(temperature);
      }
      if (timeout != null) {
        meta.setHuggingFaceTimeout(timeout);
      }
    } else {
      if (!Utils.isEmpty(baseUrl)) {
        meta.setOpenAiBaseUrl(baseUrl);
      }
      if (!Utils.isEmpty(apiKey)) {
        meta.setOpenAiApiKey(apiKey);
      }
      if (!Utils.isEmpty(modelName)) {
        meta.setOpenAiModelName(modelName);
      }
      if (applyTemperature) {
        meta.setOpenAiTemperature(temperature);
      }
      if (timeout != null) {
        meta.setOpenAiTimeout(timeout);
      }
    }
  }

  public static String generate(
      AiProvider provider,
      IVariables variables,
      String systemPrompt,
      String userPrompt,
      List<ChatMessage> conversationHistory)
      throws HopException {
    validate(provider);
    LanguageModelChatMeta meta = toLanguageModelChatMeta(provider, variables);
    LanguageModelFacade facade = new LanguageModelFacade(variables, meta);
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new SystemMessage(systemPrompt));
    if (conversationHistory != null) {
      messages.addAll(conversationHistory);
    }
    messages.add(new UserMessage(userPrompt));
    try {
      ChatResponse response = facade.chat(messages);
      return response != null && response.aiMessage() != null ? response.aiMessage().text() : "";
    } catch (Exception e) {
      throw new HopException("AI request failed: " + e.getMessage(), e);
    } catch (Error e) {
      // ServiceConfigurationError (langchain4j SPI) is an Error, not an Exception.
      throw new HopException("AI request failed: " + e.getMessage(), e);
    }
  }

  public static String healthCheck(AiProvider provider, IVariables variables) throws HopException {
    String response = generate(provider, variables, HEALTH_SYSTEM, HEALTH_USER, null);
    if (Utils.isEmpty(response)) {
      throw new HopException("The AI provider returned an empty response.");
    }
    String model =
        Utils.isEmpty(provider.getModelName())
            ? provider.getProvider().getDefaultModelName()
            : provider.getModelName();
    return "Connected to "
        + provider.getPluginName()
        + " (model: "
        + model
        + "). Response: "
        + response.trim();
  }

  public static void validate(AiProvider provider) throws HopException {
    if (provider == null) {
      throw new HopException("AI provider metadata is missing");
    }
    if (!AiLanguageModelAvailability.isAvailable()) {
      throw new HopException(
          "The Hop Language Model Chat plugin is not installed. Add hop-transform-languagemodelchat to your Hop assembly.");
    }
    IAiProvider backend = provider.getProvider();
    if (backend == null) {
      throw new HopException("Please select an AI provider type.");
    }
    if (backend.requiresApiKey() && Utils.isEmpty(provider.getApiKey())) {
      throw new HopException("Please configure an API key for the AI provider.");
    }
  }

  static double parseTemperature(String temperature) {
    if (Utils.isEmpty(temperature)) {
      return 0.3;
    }
    try {
      return Double.parseDouble(temperature.trim());
    } catch (NumberFormatException e) {
      return 0.3;
    }
  }

  static Integer parseTimeout(String timeoutSeconds) {
    if (Utils.isEmpty(timeoutSeconds)) {
      return null;
    }
    try {
      return Integer.parseInt(timeoutSeconds.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null) {
      return "";
    }
    return variables != null ? variables.resolve(value) : value;
  }
}
