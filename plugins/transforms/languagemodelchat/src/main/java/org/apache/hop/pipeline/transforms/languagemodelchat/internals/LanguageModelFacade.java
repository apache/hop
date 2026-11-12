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
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta.DEFAULT_ANTHROPIC_API_KEY;
import static org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta.DEFAULT_HUGGING_FACE_ACCESS_TOKEN;
import static org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta.DEFAULT_MISTRAL_API_KEY;
import static org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta.DEFAULT_OPENAI_API_KEY;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.huggingface.HuggingFaceChatModel;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.huggingface.HuggingFaceClient;

public class LanguageModelFacade {

  private static final Class<?> PKG = LanguageModelChatMeta.class;

  /**
   * The key field of each provider: the variable it defaults to, and the domains its own endpoints
   * live on. The environment variable of that name is only sent to those domains.
   */
  enum ApiKeyProvider {
    OPEN_AI(DEFAULT_OPENAI_API_KEY, "openai.com"),
    MISTRAL(DEFAULT_MISTRAL_API_KEY, "mistral.ai"),
    ANTHROPIC(DEFAULT_ANTHROPIC_API_KEY, "anthropic.com"),
    HUGGING_FACE(DEFAULT_HUGGING_FACE_ACCESS_TOKEN, "huggingface.co", "huggingface.cloud");

    private final String defaultKey;
    private final String variableName;
    private final List<String> domains;

    ApiKeyProvider(String defaultKey, String... domains) {
      this.defaultKey = defaultKey;
      this.variableName = StringUtil.getVariableName(defaultKey);
      this.domains = List.of(domains);
    }

    String variableName() {
      return variableName;
    }

    /**
     * Whether a request to this endpoint goes to the provider itself: over https to one of its
     * domains, or to its default endpoint when no endpoint is set.
     */
    boolean ownsEndpoint(String endpoint) {
      if (endpoint == null) {
        return true;
      }
      String host = host(endpoint);
      if (host == null || !endpoint.regionMatches(true, 0, "https://", 0, 8)) {
        return false;
      }
      return domains.stream().anyMatch(d -> host.equals(d) || host.endsWith("." + d));
    }
  }

  private final LanguageModelChatMeta meta;
  private final JsonAdapter<List<Message>> inputJsonAdapter;
  private final JsonAdapter<List<BaseMessage>> outputJsonAdapter;
  private final LanguageModel lm;
  private final IVariables variables;
  private final UnaryOperator<String> environment;
  private final ILogChannel log;
  private ChatModel model;

  public LanguageModelFacade(IVariables variables, LanguageModelChatMeta meta) {
    this(variables, meta, LogChannel.GENERAL);
  }

  /**
   * @param log where to report an API key that is not sent
   */
  public LanguageModelFacade(IVariables variables, LanguageModelChatMeta meta, ILogChannel log) {
    this(variables, meta, System::getenv, log);
  }

  /** With the environment lookup handed in, so tests need not set real environment variables. */
  LanguageModelFacade(
      IVariables variables,
      LanguageModelChatMeta meta,
      UnaryOperator<String> environment,
      ILogChannel log) {
    this.variables = variables;
    this.environment = environment;
    this.log = log;
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

  /**
   * The API key to send, or null to send none.
   *
   * <p>A key field defaults to the provider's own variable, such as {@code ${OPENAI_API_KEY}}.
   * Pipelines written before the default became a variable hold the bare name, which reads the same
   * way. The field is resolved against the Hop variables. When no Hop variable defines the
   * provider's own variable, the operating system environment variable of that name is used, since
   * the environment is only imported into Hop variables on request. That environment variable is
   * only sent to the provider's own endpoint: a base URL that points elsewhere, such as an
   * OpenAI-compatible server, gets no key rather than the provider's. Any other variable that
   * nothing defines sends no key, rather than its own text.
   *
   * @param field the key field as stored in the transform
   * @param provider the provider whose variable the field defaults to
   * @param endpoint the resolved base URL the request goes to, or null for the provider's default
   */
  String resolveApiKey(String field, ApiKeyProvider provider, String endpoint) {
    String value = trimToNull(field);
    if (value == null) {
      return null;
    }
    if (value.equals(provider.variableName())) {
      value = provider.defaultKey;
    }
    String resolved = trimToNull(variables.resolve(value));
    if (resolved != null && !StringUtil.containsVariableToken(resolved)) {
      return resolved;
    }
    if (!value.equals(provider.defaultKey)) {
      if (resolved != null) {
        // Never log the value itself: it may be a key that happens to contain a delimiter.
        log.logBasic(BaseMessages.getString(PKG, "LanguageModelFacade.ApiKey.Unresolved"));
      }
      return null;
    }
    String fromEnvironment = trimToNull(environment.apply(provider.variableName()));
    if (fromEnvironment == null) {
      return null;
    }
    if (!provider.ownsEndpoint(endpoint)) {
      String host = host(endpoint);
      log.logBasic(
          BaseMessages.getString(
              PKG,
              "LanguageModelFacade.ApiKey.EnvironmentKeyNotSent",
              provider.variableName(),
              host == null ? endpoint : host));
      return null;
    }
    return fromEnvironment;
  }

  private static String host(String url) {
    try {
      String host = new URI(url).getHost();
      return host == null ? null : host.toLowerCase(Locale.ROOT);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  private ChatModel createOpenAiModel() {

    String baseUrl = resolve(meta.getOpenAiBaseUrl());
    String apiKey = resolveApiKey(meta.getOpenAiApiKey(), ApiKeyProvider.OPEN_AI, baseUrl);
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
            .logResponses(logResponses)
            .httpClientBuilder(openAiHttpClientBuilder());

    return builder.build();
  }

  private ChatModel createHuggingFaceModel() {
    String modelResource = resolve(meta.getHuggingFaceModelId());

    // A model id goes to the Hugging Face router, a URL to that dedicated endpoint.
    String accessToken =
        resolveApiKey(
            meta.getHuggingFaceAccessToken(),
            ApiKeyProvider.HUGGING_FACE,
            HuggingFaceClient.isDedicatedEndpoint(modelResource) ? modelResource : null);
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
    Boolean think = OllamaThink.think(meta.getOllamaThink());
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
        // Null leaves think unset, so the model and the Ollama version decide, and the answer may
        // carry <think> tags as before. returnThinking(false) only drops the separate field.
        .think(think)
        .returnThinking(false)
        .timeout(timeout == null ? null : ofSeconds(timeout))
        .maxRetries(maxRetries)
        .httpClientBuilder(jdkHttpClientBuilder())
        .build();
  }

  private ChatModel createAnthropicModel() {

    String baseUrl = resolve(meta.getAnthropicBaseUrl());
    String apiKey = resolveApiKey(meta.getAnthropicApiKey(), ApiKeyProvider.ANTHROPIC, baseUrl);
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
        .httpClientBuilder(jdkHttpClientBuilder())
        .build();
  }

  private ChatModel createMistralModel() {
    String baseUrl = resolve(meta.getMistralBaseUrl());
    String apiKey = resolveApiKey(meta.getMistralApiKey(), ApiKeyProvider.MISTRAL, baseUrl);
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
        .httpClientBuilder(jdkHttpClientBuilder())
        .build();
  }

  /**
   * Construct the JDK client here instead of going through langchain4j's ServiceLoader. Hop's
   * plugin classloader is child-first while ServiceLoader still sees parent META-INF/services, so
   * the factory class and HttpClientBuilderFactory can come from different loaders ("not a
   * subtype").
   */
  private static JdkHttpClientBuilder jdkHttpClientBuilder() {
    return new JdkHttpClientBuilder();
  }

  private JdkHttpClientBuilder openAiHttpClientBuilder() {
    JdkHttpClientBuilder builder = jdkHttpClientBuilder();
    if (meta.isOpenAiUseProxy()) {
      builder.httpClientBuilder(
          HttpClient.newBuilder()
              .proxy(
                  ProxySelector.of(
                      new InetSocketAddress(
                          meta.getOpenAiProxyHost(), meta.getOpenAiProxyPort()))));
    }
    return builder;
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
