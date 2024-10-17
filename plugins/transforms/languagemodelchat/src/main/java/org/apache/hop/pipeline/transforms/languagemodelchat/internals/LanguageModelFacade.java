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
import static java.lang.System.getenv;
import static java.net.Proxy.Type.HTTP;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.Message.toChatMessages;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.huggingface.DedicatedEndpointHuggingFaceChatModel;
import dev.langchain4j.model.huggingface.HuggingFaceChatModel;
import dev.langchain4j.model.mistralai.MistralAiChatModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.output.Response;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;

public class LanguageModelFacade {

  private final LanguageModelChatMeta meta;
  // private final MessageDigest digest = getSha3_512Digest();
  // private final MessageDigest fastDigest = DigestUtils.getMd5Digest();
  private final JsonAdapter<List<Message>> inputJsonAdapter;
  private final JsonAdapter<List<BaseMessage>> outputJsonAdapter;
  private final LanguageModel lm;
  // private final IRowMeta rowMeta;
  // private final Object[] inputRow;
  private final IVariables variables;
  private ChatLanguageModel model;

  //  Map<String, GenericContainer<? extends Container>> containers;
  // private byte[][] state;

  public LanguageModelFacade(IVariables variables, LanguageModelChatMeta meta) {
    this.variables = variables;
    this.meta = (LanguageModelChatMeta) meta.clone();
    //  this.containers = containers;
    this.lm = new LanguageModel(meta);
    //   this.rowMeta = rowMeta;
    //   this.inputRow = inputRow;
    inputJsonAdapter =
        new Moshi.Builder().build().adapter(newParameterizedType(List.class, Message.class));
    outputJsonAdapter =
        new Moshi.Builder().build().adapter(newParameterizedType(List.class, BaseMessage.class));

    // checkState();
  }

  /*
     public void checkState() {
         if (model == null || state == null || state.length == 0 || state[0].length == 0) {
             createModel();
         } else {
             byte[] data = getBytes(meta.getXml(), UTF_8);
             // Check fast, less accurate digest first
             byte[] s0 = fastDigest.digest(data);

             if (!Arrays.equals(this.state[0], s0)) {
                 // Check accurate digest to rule out a collision
                 byte[] s1 = digest.digest(data);
                 if (!Arrays.equals(this.state[1], s1)) {
                     createModel();
                     this.state[0] = s0;
                     this.state[1] = s1;
                 }
             }
         }
     }

  */

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
    // byte[] data = getBytes(meta.getXml(), UTF_8);
    // this.state = new byte[2][0];
    //  this.state[0] = fastDigest.digest(data);
    // this.state[1] = digest.digest(data);
  }

  /*
      private String stringParam(String field) {
          field = meta.getOutputFieldNamePrefix() + field;
          int idx = this.rowMeta.indexOfValue(field);

          return idx >= 0 ? (String) inputRow[idx] : null;
      }

      private Integer intParam(String field) {
          field = meta.getOutputFieldNamePrefix() + field;
          int idx = this.rowMeta.indexOfValue(field);

          return idx >= 0 ? ((Number) inputRow[idx]).intValue() : null;
      }

      private Double doubleParam(String field) {
          field = meta.getOutputFieldNamePrefix() + field;
          int idx = this.rowMeta.indexOfValue(field);

          return idx >= 0 ? ((Number) inputRow[idx]).doubleValue() : null;
      }
  */
  private String resolve(String param) {
    return variables.resolve(trimToNull(param));
  }

  private ChatLanguageModel createOpenAiModel() {

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
    /*
            baseUrl = baseUrl != null ? baseUrl : stringParam("baseUrl");
            apiKey = apiKey != null ? apiKey : stringParam("apiKey");
            organizationId = organizationId != null ? organizationId : stringParam("organizationId");
            modelName = modelName != null ? modelName : stringParam("modelName");
            responseFormat = responseFormat != null ? responseFormat : stringParam("responseFormat");
            user = user != null ? user : stringParam("user");

            temperature = temperature != null ? temperature : doubleParam("temperature");
            topP = topP != null ? topP : doubleParam("topP");
            presencePenalty = presencePenalty != null ? presencePenalty : doubleParam("presencePenalty");
            frequencyPenalty = frequencyPenalty != null ? frequencyPenalty : doubleParam("frequencyPenalty");

            maxTokens = maxTokens != null ? maxTokens : intParam("maxTokens");
            seed = seed != null ? seed : intParam("seed");
            timeout = timeout != null ? timeout : intParam("timeout");
            maxRetries = maxRetries != null ? maxRetries : intParam("maxRetries");

    */
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
            .responseFormat(responseFormat)
            .seed(seed)
            .user(user)
            .timeout(timeout == null ? null : ofSeconds(timeout))
            .maxRetries(maxRetries)
            .logRequests(logRequests)
            .logResponses(logResponses);

    if (meta.isOpenAiUseProxy()) {
      builder.proxy(
          new Proxy(
              HTTP, new InetSocketAddress(meta.getOpenAiProxyHost(), meta.getOpenAiProxyPort())));
    }

    return builder.build();
  }

  private ChatLanguageModel createHuggingFaceModel() {
    String modelResource = resolve(meta.getHuggingFaceModelId());
    boolean dedicated =
        startsWithIgnoreCase(modelResource, "http://")
            || startsWithIgnoreCase(modelResource, "https://");

    String accessToken = trimToNull(resolve(meta.getHuggingFaceAccessToken()));
    accessToken =
        isBlank(accessToken) ? trimToNull(getenv(meta.getHuggingFaceAccessToken())) : accessToken;
    Integer timeout = meta.getHuggingFaceTimeout();
    Double temperature = meta.getHuggingFaceTemperature();
    Integer maxNewTokens = meta.getHuggingFaceMaxNewTokens();

    boolean returnFullText = meta.isHuggingFaceReturnFullText();
    boolean waitForModel = meta.isHuggingFaceWaitForModel();

    if (dedicated) {
      return DedicatedEndpointHuggingFaceChatModel.builder()
          .accessToken(accessToken)
          .endpointUrl(modelResource)
          .timeout(timeout == null ? null : ofSeconds(timeout))
          .temperature(temperature)
          .maxNewTokens(maxNewTokens)
          .returnFullText(returnFullText)
          .waitForModel(waitForModel)
          .build();
    } else {
      return HuggingFaceChatModel.builder()
          .accessToken(accessToken)
          .modelId(modelResource)
          .timeout(timeout == null ? null : ofSeconds(timeout))
          .temperature(temperature)
          .maxNewTokens(maxNewTokens)
          .returnFullText(returnFullText)
          .waitForModel(waitForModel)
          .build();
    }
  }

  private ChatLanguageModel createOllamaModel() {
    String modelName = resolve(meta.getOllamaModelName());

    //  modelName = modelName != null ? modelName : stringParam("modelName");

    if (isBlank(modelName)) {
      modelName = "phi3";
    }

    String imageEndpoint = resolve(meta.getOllamaImageEndpoint());
    //  imageEndpoint = imageEndpoint != null ? imageEndpoint : stringParam("imageEndpoint");

    if (isBlank(imageEndpoint)) {
      switch (modelName) {
        case "mistral":
        case "llama3":
        case "llama2":
        case "codellama":
        case "phi":
        case "orca-mini":
        case "tinyllama":
          imageEndpoint = String.format("langchain4j/ollama-%s:latest", modelName);
          break;
        default:
          imageEndpoint = "ollama/ollama";
      }
    }

    boolean url =
        startsWithIgnoreCase(imageEndpoint, "http://")
            || startsWithIgnoreCase(imageEndpoint, "https://");
    String baseUrl = imageEndpoint;
    /*
    if (url) {
        baseUrl = imageEndpoint;
    } else {
        int port = 11434;
        String key = String.format("%s:%s", imageEndpoint, port);
        OllamaContainer container = (OllamaContainer) containers.get(key);
        if (container == null) {
            try {
                container = new OllamaContainer(DockerImageName.parse(imageEndpoint)
                        .asCompatibleSubstituteFor("ollama/ollama"))
                        .withExposedPorts(port)
                        .waitingFor(Wait.forHttp("/"));
                containers.put(key, container);
                container.start();
                container.execInContainer("ollama", "pull", modelName);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        baseUrl = String.format("http://%s:%d", container.getHost(), container.getFirstMappedPort());
    }

     */

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
        .baseUrl(baseUrl)
        .modelName(modelName)
        .temperature(temperature)
        .topP(topP)
        .topK(topK)
        .repeatPenalty(repeatPenalty)
        .seed(seed)
        .numPredict(numPredict)
        .numCtx(numCtx)
        .format(format)
        .timeout(timeout == null ? null : ofSeconds(timeout))
        .maxRetries(maxRetries)
        .build();
  }

  private ChatLanguageModel createAnthropicModel() {

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

  private ChatLanguageModel createMistralModel() {
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
        .responseFormat(responseFormat)
        .timeout(timeout == null ? null : ofSeconds(timeout))
        .logRequests(logRequests)
        .logResponses(logResponses)
        .maxRetries(maxRetries)
        .build();
  }

  public String messagesToOutput(List<ChatMessage> chat, String assistant)
      throws HopValueException {
    // TODO fetch these from the meta, hard code for now
    String systemRoleName = "system";
    String userRoleName = "user";
    String assistantRoleName = "assistant";

    List<BaseMessage> messages = new ArrayList<>();
    for (ChatMessage c : chat) {
      String text = c.text();
      switch (c.type()) {
        case SYSTEM:
          messages.add(new BaseMessage(systemRoleName, text));
          break;
        case USER:
          messages.add(new BaseMessage(userRoleName, text));
          break;
        case AI:
          messages.add(new BaseMessage(assistantRoleName, text));
          break;
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

  public Response<AiMessage> generate(List<ChatMessage> messages) {
    return model().generate(messages);
  }

  public ChatLanguageModel model() {
    // checkState();
    if (model == null) {
      createModel();
    }
    return model;
  }
}
