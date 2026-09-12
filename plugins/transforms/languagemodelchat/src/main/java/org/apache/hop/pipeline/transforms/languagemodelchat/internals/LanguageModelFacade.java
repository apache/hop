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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals;

import static com.squareup.moshi.Types.newParameterizedType;
import static dev.langchain4j.data.message.UserMessage.userMessage;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.Message.toChatMessages;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.http.client.jdk.JdkHttpClientBuilder;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.mistralai.MistralAiChatModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.huggingface.HuggingFaceChatModel;

public class LanguageModelFacade {

  private final LanguageModelChatMeta meta;
  private final JsonAdapter<List<Message>> inputJsonAdapter;
  private final JsonAdapter<List<BaseMessage>> outputJsonAdapter;
  private final LanguageModel lm;
  private final IVariables variables;
  private ChatModel model;

  public LanguageModelFacade(IVariables variables, LanguageModelChatMeta meta) {
    this.variables = variables;
    this.meta = (LanguageModelChatMeta) meta.clone();
    this.lm = new LanguageModel(meta);
    inputJsonAdapter =
        new Moshi.Builder().build().adapter(newParameterizedType(List.class, Message.class));
    outputJsonAdapter =
        new Moshi.Builder().build().adapter(newParameterizedType(List.class, BaseMessage.class));
  }

  private void createModel() {
    switch (lm.getType()) {
      case OPEN_AI:
        model = createOpenAiModel();
        break;
      case ANTHROPIC:
        model = createAnthropicModel();
        break;
      case OLLAMA:
        model = createOllamaModel();
        break;
      case MISTRAL:
        model = createMistralModel();
        break;
      case HUGGING_FACE:
        model = createHuggingFaceModel();
        break;
    }
  }

  private String resolve(String param) {
    return variables.resolve(trimToNull(param));
  }

  private ChatModel createOpenAiModel() {

    String baseUrl = resolve(meta.getOpenAiBaseUrl());
    String apiKey = trimToNull(resolve(meta.getOpenAiApiKey()));
    apiKey = isBlank(apiKey) ? trimToNull(getenv(meta.getOpenAiApiKey())) : apiKey;
    String organizationId = resolve(meta.getOpenAiOrganizationId());
    String modelName = resolve(meta.getOpenAiModelName());
    String responseFormat = resolve(meta.getOpenAiResponseFormat());
    String user = resolve(meta.getOpenAiUser());
    Double temperature = meta.getOpenAiTemperature();
    Double topP = meta.getOpenAiTopP();
    Double presencePenalty = meta.getOpenAiPresencePenalty();
    Double frequencyPenalty = meta.getOpenAiFrequencyPenalty();
    Integer maxTokens = meta.getOpenAiMaxTokens();
    Integer seed = meta.getOpenAiSeed();
    Integer timeout = meta.getOpenAiTimeout();
    Integer maxRetries = meta.getOpenAiMaxRetries();
    boolean logRequests = meta.isOpenAiLogRequests();
    boolean logResponses = meta.isOpenAiLogResponses();

    OpenAiChatModel.OpenAiChatModelBuilder builder =
        OpenAiChatModel.builder()
            .baseUrl(baseUrl)
            .apiKey(apiKey)
            .organizationId(organizationId)
            .modelName(modelName)
            .temperature(temperature)
            .topP(topP)
            .maxTokens(maxTokens)
            .presencePenalty(presencePenalty)
            .frequencyPenalty(frequencyPenalty)
            .responseFormat(toResponseFormat(responseFormat))
            .seed(seed)
            .user(user)
            .timeout(timeout == null ? null : ofSeconds(timeout))
            .maxRetries(maxRetries)
            .logRequests(logRequests)
            .logResponses(logResponses);

    if (meta.isOpenAiUseProxy()) {
      builder.httpClientBuilder(
          new JdkHttpClientBuilder()
              .httpClientBuilder(
                  HttpClient.newBuilder()
                      .proxy(
                          ProxySelector.of(
                              new InetSocketAddress(
                                  meta.getOpenAiProxyHost(), meta.getOpenAiProxyPort())))));
    }

    return builder.build();
  }

  private ChatModel createHuggingFaceModel() {
    String modelResource = resolve(meta.getHuggingFaceModelId());

    String accessToken = trimToNull(resolve(meta.getHuggingFaceAccessToken()));
    accessToken =
        isBlank(accessToken) ? trimToNull(getenv(meta.getHuggingFaceAccessToken())) : accessToken;
    Integer timeout = meta.getHuggingFaceTimeout();
    Double temperature = meta.getHuggingFaceTemperature();
    Integer maxNewTokens = meta.getHuggingFaceMaxNewTokens();

    boolean returnFullText = meta.isHuggingFaceReturnFullText();
    boolean waitForModel = meta.isHuggingFaceWaitForModel();

    return HuggingFaceChatModel.builder()
        .accessToken(accessToken)
        .modelResource(modelResource)
        .timeout(timeout == null ? null : ofSeconds(timeout))
        .temperature(temperature)
        .maxNewTokens(maxNewTokens)
        .returnFullText(returnFullText)
        .waitForModel(waitForModel)
        .build();
  }

  private ChatModel createOllamaModel() {
    String modelName = resolve(meta.getOllamaModelName());

    if (isBlank(modelName)) {
      modelName = "phi3";
    }

    String imageEndpoint = resolve(meta.getOllamaImageEndpoint());

    if (isBlank(imageEndpoint)) {
      switch (modelName) {
        case "mistral", "llama3", "llama2", "codellama", "phi", "orca-mini", "tinyllama" ->
            imageEndpoint = format("langchain4j/ollama-%s:latest", modelName);
        default -> imageEndpoint = "ollama/ollama";
      }
    }

    Double temperature = meta.getOllamaTemperature();
    Double topP = meta.getOllamaTopP();
    Integer topK = meta.getOllamaTopK();
    Double repeatPenalty = meta.getOllamaRepeatPenalty();
    Integer seed = meta.getOllamaSeed();
    Integer numPredict = meta.getOllamaNumPredict();
    Integer numCtx = meta.getOllamaNumCtx();
    String format = meta.getOllamaFormat();
    Integer timeout = meta.getOllamaTimeout();
    Integer maxRetries = meta.getOllamaMaxRetries();

    return OllamaChatModel.builder()
        .baseUrl(imageEndpoint)
        .modelName(modelName)
        .temperature(temperature)
        .topP(topP)
        .topK(topK)
        .repeatPenalty(repeatPenalty)
        .seed(seed)
        .numPredict(numPredict)
        .numCtx(numCtx)
        .responseFormat(toResponseFormat(format))
        .timeout(timeout == null ? null : ofSeconds(timeout))
        .maxRetries(maxRetries)
        .build();
  }

  private ChatModel createAnthropicModel() {

    String baseUrl = resolve(meta.getAnthropicBaseUrl());
    String apiKey = trimToNull(resolve(meta.getAnthropicApiKey()));
    apiKey = isBlank(apiKey) ? trimToNull(getenv(meta.getAnthropicApiKey())) : apiKey;
    String version = resolve(meta.getAnthropicVersion());
    String modelName = resolve(meta.getAnthropicModelName());
    Double temperature = meta.getAnthropicTemperature();
    Double topP = meta.getAnthropicTopP();
    Integer topK = meta.getAnthropicTopK();
    Integer maxTokens = meta.getAnthropicMaxTokens();
    Integer timeout = meta.getAnthropicTimeout();
    Integer maxRetries = meta.getAnthropicMaxRetries();

    boolean logRequests = meta.isAnthropicLogRequests();
    boolean logResponses = meta.isAnthropicLogResponses();

    return AnthropicChatModel.builder()
        .baseUrl(baseUrl)
        .apiKey(apiKey)
        .version(version)
        .modelName(modelName)
        .temperature(temperature)
        .topP(topP)
        .topK(topK)
        .maxTokens(maxTokens)
        .timeout(timeout == null ? null : ofSeconds(timeout))
        .maxRetries(maxRetries)
        .logRequests(logRequests)
        .logResponses(logResponses)
        .build();
  }

  private ChatModel createMistralModel() {
    String baseUrl = resolve(meta.getMistralBaseUrl());
    String apiKey = trimToNull(resolve(meta.getMistralApiKey()));
    apiKey = isBlank(apiKey) ? trimToNull(getenv(meta.getMistralApiKey())) : apiKey;
    String modelName = resolve(meta.getMistralModelName());
    Double temperature = meta.getMistralTemperature();
    Double topP = meta.getMistralTopP();
    Integer maxTokens = meta.getMistralMaxTokens();
    Integer randomSeed = meta.getMistralRandomSeed();
    String responseFormat = resolve(meta.getMistralResponseFormat());
    Integer timeout = meta.getMistralTimeout();
    Integer maxRetries = meta.getMistralMaxRetries();

    boolean safePrompt = meta.isMistralSafePrompt();
    boolean logRequests = meta.isMistralLogRequests();
    boolean logResponses = meta.isMistralLogResponses();

    return MistralAiChatModel.builder()
        .baseUrl(baseUrl)
        .apiKey(apiKey)
        .modelName(modelName)
        .temperature(temperature)
        .topP(topP)
        .maxTokens(maxTokens)
        .safePrompt(safePrompt)
        .randomSeed(randomSeed)
        .responseFormat(toResponseFormat(responseFormat))
        .timeout(timeout == null ? null : ofSeconds(timeout))
        .logRequests(logRequests)
        .logResponses(logResponses)
        .maxRetries(maxRetries)
        .build();
  }

  /**
   * The providers take a response format object instead of the free format string the dialogs ask
   * for. Anything mentioning json asks for json, anything else for text. Open AI does read a string
   * as well, but only recognises "json_object" there and drops every other value.
   */
  private static ResponseFormat toResponseFormat(String format) {
    String value = trimToNull(format);
    if (value == null) {
      return null;
    }
    return containsIgnoreCase(value, "json") ? ResponseFormat.JSON : ResponseFormat.TEXT;
  }

  @SuppressWarnings("java:S131")
  public String messagesToOutput(List<ChatMessage> chat, String assistant) {
    // TODO fetch these from the meta, hard code for now
    String systemRoleName = "system";
    String userRoleName = "user";
    String assistantRoleName = "assistant";

    List<BaseMessage> messages = new ArrayList<>();
    for (ChatMessage c : chat) {
      String text = ChatMessages.text(c);
      switch (c.type()) {
        case SYSTEM -> messages.add(new BaseMessage(systemRoleName, text));
        case USER -> messages.add(new BaseMessage(userRoleName, text));
        case AI -> messages.add(new BaseMessage(assistantRoleName, text));
      }
    }

    messages.add(new BaseMessage(assistantRoleName, assistant));
    return outputJsonAdapter.toJson(messages);
  }

  public List<Message> readMessagesJson(String json) throws HopValueException {
    try {
      return inputJsonAdapter.fromJson(json);
    } catch (Exception e) {
      throw new HopValueException(e);
    }
  }

  public List<ChatMessage> inputToChatMessages(String input) throws HopValueException {
    input = trimToNull(input);
    notNull(input, "Message is not set");
    List<ChatMessage> messageList = new ArrayList<>();
    if (meta.isInputChatJson()) {
      List<Message> messages = readMessagesJson(input);
      isTrue(!messages.isEmpty(), "No messages were read from the json: " + input);
      List<ChatMessage> chatMessages = toChatMessages(messages);
      isTrue(!chatMessages.isEmpty(), "No valid chat messages were read from the json: " + input);
      messageList.addAll(chatMessages);
    } else {
      messageList.add(userMessage(input));
    }

    return messageList;
  }

  public ChatResponse chat(List<ChatMessage> messages) {
    return model().chat(messages);
  }

  public ChatModel model() {
    if (model == null) {
      createModel();
    }
    return model;
  }
}
