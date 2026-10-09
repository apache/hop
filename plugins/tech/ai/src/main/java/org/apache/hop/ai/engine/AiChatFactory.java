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
import dev.langchain4j.model.chat.Capability;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.TokenUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.ai.metadata.AiModelRole;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelFacade;

/** Maps {@link AiProvider} metadata onto Language Model Chat and runs a completion. */
public final class AiChatFactory {

  private static final String HEALTH_SYSTEM =
      "You are a health-check endpoint. Reply with exactly OK and nothing else.";
  private static final String HEALTH_USER = "health";

  private static final Class<?> PKG = AiChatFactory.class;

  private AiChatFactory() {}

  static String missingTypeMessage(AiProvider provider) {
    return BaseMessages.getString(PKG, "AiChatFactory.MissingType", provider.getName());
  }

  public static LanguageModelChatMeta toLanguageModelChatMeta(
      AiProvider provider, IVariables variables) throws HopException {
    if (provider == null) {
      throw new AiUserException(BaseMessages.getString(PKG, "AiChatFactory.NoProvider"));
    }
    if (!provider.hasProviderType()) {
      throw new AiUserException(missingTypeMessage(provider));
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
      throw new HopException(BaseMessages.getString(PKG, "AiChatFactory.NoChatMeta"));
    }
    if (providerName == null || providerName.isBlank()) {
      return source;
    }
    if (metadataProvider == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "AiChatFactory.NoMetadataProvider", providerName));
    }
    String name = resolve(variables, providerName);
    AiProvider provider = metadataProvider.getSerializer(AiProvider.class).load(name);
    if (provider == null) {
      throw new AiUserException(
          BaseMessages.getString(PKG, "AiChatFactory.ProviderNotFound", name));
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
    if (!provider.hasProviderType()) {
      throw new AiUserException(missingTypeMessage(provider));
    }
    IAiProvider backend = provider.getProvider();
    meta.setModelType(backend.getHopModelType());

    String baseUrl = resolve(variables, provider.getBaseUrl());
    if (Utils.isEmpty(baseUrl) && useProviderDefaults) {
      baseUrl = backend.getDefaultBaseUrl();
    }
    String modelName = resolve(variables, provider.resolveModelName(AiModelRole.CHAT));
    if (Utils.isEmpty(modelName) && useProviderDefaults) {
      modelName = backend.getDefaultModelName();
    }
    String apiKey = resolve(variables, provider.getApiKey());
    boolean hasTemperature = !Utils.isEmpty(provider.getTemperature());
    double temperature = parseTemperature(resolve(variables, provider.getTemperature()));
    Integer timeout = parseTimeout(resolve(variables, provider.getTimeoutSeconds()));
    Integer contextSize = AiProviderSettings.contextSize(provider, variables);
    Integer maxOutputTokens = AiProviderSettings.maxOutputTokens(provider, variables);
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
      if (maxOutputTokens != null) {
        meta.setAnthropicMaxTokens(maxOutputTokens);
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
      if (contextSize != null) {
        meta.setOllamaNumCtx(contextSize);
      }
      if (maxOutputTokens != null) {
        meta.setOllamaNumPredict(maxOutputTokens);
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
      if (maxOutputTokens != null) {
        meta.setMistralMaxTokens(maxOutputTokens);
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
      if (maxOutputTokens != null) {
        meta.setHuggingFaceMaxNewTokens(maxOutputTokens);
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
      if (maxOutputTokens != null) {
        meta.setOpenAiMaxTokens(maxOutputTokens);
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
    return generateResult(provider, variables, systemPrompt, userPrompt, conversationHistory)
        .getText();
  }

  public static AiChatResult generateResult(
      AiProvider provider,
      IVariables variables,
      String systemPrompt,
      String userPrompt,
      List<ChatMessage> conversationHistory)
      throws HopException {
    return generateResult(
        provider, variables, systemPrompt, userPrompt, conversationHistory, false);
  }

  /**
   * @param jsonOnly ask the provider to answer with valid JSON only, where it supports that (Ollama
   *     and OpenAI). Small models write broken JSON in free text far more often.
   */
  public static AiChatResult generateResult(
      AiProvider provider,
      IVariables variables,
      String systemPrompt,
      String userPrompt,
      List<ChatMessage> conversationHistory,
      boolean jsonOnly)
      throws HopException {
    validate(provider);
    LanguageModelChatMeta meta = toLanguageModelChatMeta(provider, variables);
    if (jsonOnly) {
      if ("OLLAMA".equals(provider.getHopModelType())) {
        meta.setOllamaFormat("json");
      } else if ("MISTRAL".equals(provider.getHopModelType())) {
        meta.setMistralResponseFormat("json_object");
      } else if (provider.getProvider() instanceof OpenAiProvider) {
        meta.setOpenAiResponseFormat("json_object");
      }
    }
    LanguageModelFacade facade = new LanguageModelFacade(variables, meta);
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new SystemMessage(systemPrompt));
    if (conversationHistory != null) {
      messages.addAll(conversationHistory);
    }
    messages.add(new UserMessage(userPrompt));
    long started = System.nanoTime();
    try {
      ChatResponse response = facade.chat(messages);
      long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      String text =
          response != null && response.aiMessage() != null ? response.aiMessage().text() : "";
      TokenUsage usage = response != null ? response.tokenUsage() : null;
      return new AiChatResult(
          text == null ? "" : text,
          positive(usage != null ? usage.inputTokenCount() : null),
          positive(usage != null ? usage.outputTokenCount() : null),
          durationMs);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "AiChatFactory.RequestFailed", e.getMessage()), e);
    } catch (Error e) {
      // ServiceConfigurationError (langchain4j SPI) is an Error, not an Exception.
      throw new HopException(
          BaseMessages.getString(PKG, "AiChatFactory.RequestFailed", e.getMessage()), e);
    }
  }

  /**
   * Ask with the answer held to a JSON schema. The model is built by {@link AiChatModelFactory},
   * which knows which provider types accept a schema.
   *
   * @return the answer, or null when this provider type cannot hold a model to a schema; the caller
   *     then asks without one
   */
  public static AiChatResult generateStructured(
      AiProvider provider,
      IVariables variables,
      String systemPrompt,
      String userPrompt,
      List<ChatMessage> conversationHistory,
      JsonSchema schema)
      throws HopException {
    validate(provider);
    ChatModel model;
    try {
      model = AiChatModelFactory.createChatModel(provider, "", variables);
    } catch (HopException e) {
      // A provider type the factory cannot drive, such as Hugging Face.
      return null;
    }
    if (!model.supportedCapabilities().contains(Capability.RESPONSE_FORMAT_JSON_SCHEMA)) {
      return null;
    }
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new SystemMessage(systemPrompt));
    if (conversationHistory != null) {
      messages.addAll(conversationHistory);
    }
    messages.add(new UserMessage(userPrompt));
    ChatRequest request =
        ChatRequest.builder()
            .messages(messages)
            .responseFormat(
                ResponseFormat.builder().type(ResponseFormatType.JSON).jsonSchema(schema).build())
            .build();
    long started = System.nanoTime();
    try {
      ChatResponse response = model.chat(request);
      long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      String text =
          response != null && response.aiMessage() != null ? response.aiMessage().text() : "";
      TokenUsage usage = response != null ? response.tokenUsage() : null;
      return new AiChatResult(
          text == null ? "" : text,
          positive(usage != null ? usage.inputTokenCount() : null),
          positive(usage != null ? usage.outputTokenCount() : null),
          durationMs);
    } catch (Exception | Error e) {
      throw new HopException(
          BaseMessages.getString(PKG, "AiChatFactory.RequestFailed", e.getMessage()), e);
    }
  }

  static Integer positive(Integer value) {
    if (value == null || value < 0) {
      return null;
    }
    return value;
  }

  public static String healthCheck(AiProvider provider, IVariables variables) throws HopException {
    String response = generate(provider, variables, HEALTH_SYSTEM, HEALTH_USER, null);
    if (Utils.isEmpty(response)) {
      throw new HopException(BaseMessages.getString(PKG, "AiChatFactory.EmptyResponse"));
    }
    // The model that answered: a CHAT row in Models per role replaces Model name.
    String model = resolve(variables, provider.resolveModelName(AiModelRole.CHAT));
    if (Utils.isEmpty(model)) {
      model = provider.getProvider().getDefaultModelName();
    }
    return BaseMessages.getString(
        PKG, "AiChatFactory.Connected", provider.getPluginName(), model, response.trim());
  }

  public static void validate(AiProvider provider) throws HopException {
    if (provider == null) {
      throw new AiUserException(BaseMessages.getString(PKG, "AiChatFactory.NoProvider"));
    }
    if (!AiLanguageModelAvailability.isAvailable()) {
      throw new AiUserException(BaseMessages.getString(PKG, "AiChatFactory.NoChatPlugin"));
    }
    if (!provider.hasProviderType()) {
      throw new AiUserException(missingTypeMessage(provider));
    }
    IAiProvider backend = provider.getProvider();
    if (backend.requiresApiKey() && Utils.isEmpty(provider.getApiKey())) {
      throw new AiUserException(
          BaseMessages.getString(PKG, "AiChatFactory.NoApiKey", provider.getName()));
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
