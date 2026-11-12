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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import dev.langchain4j.data.message.UserMessage;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelFacade.ApiKeyProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Which API key is sent, see issue #9385. The key fields default to the provider's variable, such
 * as {@code ${OPENAI_API_KEY}}, which is read from the Hop variables and then from the environment.
 * The environment variable only goes to the provider's own endpoint. The environment is handed to
 * the facade, so no real environment variable is needed.
 */
class LanguageModelFacadeApiKeyTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private final IVariables variables = new Variables();
  private final Map<String, String> environment = new HashMap<>();
  private final AtomicReference<String> receivedKey = new AtomicReference<>();
  private final List<String> logged = new ArrayList<>();
  private final LogChannel log =
      new LogChannel("LanguageModelFacadeApiKeyTest") {
        @Override
        public void logBasic(String message) {
          logged.add(message);
        }
      };

  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  void startServer() throws IOException {
    // 127.0.0.1 on both sides: "localhost" can bind IPv4 and connect IPv6 first.
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  // ------------------------------------------------------------------ resolveApiKey

  @Test
  void theDefaultIsReadFromTheEnvironment() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    assertEquals("sk-from-env", resolve("${OPENAI_API_KEY}"));
  }

  @Test
  void aHopVariableComesBeforeTheEnvironment() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    variables.setVariable("OPENAI_API_KEY", "sk-from-hop");
    assertEquals("sk-from-hop", resolve("${OPENAI_API_KEY}"));
  }

  @Test
  void theBareNameOlderPipelinesStoredReadsTheVariable() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    assertEquals("sk-from-env", resolve("OPENAI_API_KEY"));

    variables.setVariable("OPENAI_API_KEY", "sk-from-hop");
    assertEquals("sk-from-hop", resolve("OPENAI_API_KEY"));
  }

  @Test
  void aKeyIsSentAsItStands() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    assertEquals("sk-typed-in", resolve("sk-typed-in"));
    assertEquals("sk-typed-in", resolve("  sk-typed-in  "));
  }

  @Test
  void anotherVariableIsResolvedFromHopOnly() {
    variables.setVariable("MY_KEY", "sk-mine");
    assertEquals("sk-mine", resolve("${MY_KEY}"));

    // The environment is only read for the provider's own variable.
    environment.put("OTHER_KEY", "sk-other");
    assertNull(resolve("${OTHER_KEY}"));
  }

  @Test
  void anUnsetVariableSendsNoKey() {
    assertNull(resolve("${OPENAI_API_KEY}"));
    assertNull(resolve("OPENAI_API_KEY"));
    assertNull(resolve("${NOT_SET}"));
    assertNull(resolve("prefix-${NOT_SET}"));
    assertNull(resolve("%%NOT_SET%%"));
    assertNull(resolve("prefix-%%NOT_SET%%"));
  }

  @Test
  void aDiscardedKeyIsLoggedWithoutItsValue() {
    assertNull(resolve("sk-secret-${NOT_SET}"));
    assertEquals(1, logged.size(), logged.toString());
    assertFalse(logged.get(0).contains("sk-secret"), logged.get(0));
  }

  @Test
  void anEmptyFieldSendsNoKey() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    assertNull(resolve(null));
    assertNull(resolve(""));
    assertNull(resolve("   "));
  }

  @Test
  void anEmptyHopVariableFallsBackToTheEnvironment() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    variables.setVariable("OPENAI_API_KEY", "");
    assertEquals("sk-from-env", resolve("${OPENAI_API_KEY}"));
  }

  // ------------------------------------------------------------------ which endpoint gets it

  @Test
  void theEnvironmentKeyGoesToTheProvidersOwnEndpoint() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    assertEquals("sk-from-env", resolve("${OPENAI_API_KEY}", null));
    assertEquals("sk-from-env", resolve("${OPENAI_API_KEY}", "https://api.openai.com/v1"));
    assertEquals("sk-from-env", resolve("${OPENAI_API_KEY}", "HTTPS://API.OPENAI.COM/v1"));
    assertEquals("sk-from-env", resolve("OPENAI_API_KEY", "https://api.openai.com/v1"));
  }

  /** An OpenAI-compatible third party or local server must not get the real OpenAI key. */
  @Test
  void theEnvironmentKeyDoesNotGoToAnotherEndpoint() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    for (String endpoint :
        List.of(
            "https://api.groq.com/openai/v1",
            "http://localhost:1234/v1",
            "http://127.0.0.1:8000/v1",
            "http://api.openai.com/v1",
            "https://api.openai.com.example.org/v1",
            "https://api.openai.com@example.org/v1",
            "https://example.org/api.openai.com/v1",
            "https://notopenai.com/v1",
            "${BASE_URL}/v1",
            "not a url")) {
      assertNull(resolve("${OPENAI_API_KEY}", endpoint), endpoint);
      assertNull(resolve("OPENAI_API_KEY", endpoint), endpoint);
    }
  }

  @Test
  void aWithheldEnvironmentKeyIsLoggedWithoutItsValue() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    assertNull(resolve("${OPENAI_API_KEY}", "https://api.groq.com/openai/v1"));

    assertEquals(1, logged.size(), logged.toString());
    String message = logged.get(0);
    assertTrue(message.contains("OPENAI_API_KEY"), message);
    assertTrue(message.contains("api.groq.com"), message);
    assertFalse(message.contains("sk-from-env"), message);
  }

  @Test
  void aHopVariableOrATypedKeyGoesToAnyEndpoint() {
    environment.put("OPENAI_API_KEY", "sk-from-env");
    String endpoint = "http://localhost:1234/v1";
    assertEquals("sk-typed-in", resolve("sk-typed-in", endpoint));

    variables.setVariable("OPENAI_API_KEY", "sk-from-hop");
    assertEquals("sk-from-hop", resolve("${OPENAI_API_KEY}", endpoint));
    assertEquals("sk-from-hop", resolve("OPENAI_API_KEY", endpoint));
    assertTrue(logged.isEmpty(), logged.toString());
  }

  @Test
  void eachProviderOwnsItsOwnDomains() {
    assertTrue(ApiKeyProvider.MISTRAL.ownsEndpoint("https://api.mistral.ai/v1"));
    assertFalse(ApiKeyProvider.MISTRAL.ownsEndpoint("https://api.openai.com/v1"));

    assertTrue(ApiKeyProvider.ANTHROPIC.ownsEndpoint("https://api.anthropic.com/v1/"));
    assertFalse(ApiKeyProvider.ANTHROPIC.ownsEndpoint("https://anthropic.example.org/v1/"));

    assertTrue(ApiKeyProvider.HUGGING_FACE.ownsEndpoint(null));
    assertTrue(
        ApiKeyProvider.HUGGING_FACE.ownsEndpoint(
            "https://router.huggingface.co/hf-inference/models/meta-llama/Llama-3.3-70B-Instruct"));
    assertTrue(
        ApiKeyProvider.HUGGING_FACE.ownsEndpoint(
            "https://abc123.us-east-1.aws.endpoints.huggingface.cloud"));
    assertFalse(ApiKeyProvider.HUGGING_FACE.ownsEndpoint("https://someone.hf.space/generate"));
  }

  /** The variable each provider falls back to is the one its key field defaults to. */
  @Test
  void eachProviderReadsTheVariableItsFieldDefaultsTo() {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    assertEquals(meta.getOpenAiApiKey(), "${" + ApiKeyProvider.OPEN_AI.variableName() + "}");
    assertEquals(meta.getMistralApiKey(), "${" + ApiKeyProvider.MISTRAL.variableName() + "}");
    assertEquals(meta.getAnthropicApiKey(), "${" + ApiKeyProvider.ANTHROPIC.variableName() + "}");
    assertEquals(
        meta.getHuggingFaceAccessToken(), "${" + ApiKeyProvider.HUGGING_FACE.variableName() + "}");
  }

  // ------------------------------------------------------------------ the key on the wire

  /** The stub is not openai.com, so the OPENAI_API_KEY environment variable stays home. */
  @Test
  void openAiSendsNoEnvironmentKeyToAnotherHost() throws Exception {
    respondWith(
        "/v1/chat/completions",
        "Authorization",
        """
        {"id":"chat-1","object":"chat.completion","created":1,"model":"gpt-test",
         "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}
        """);
    environment.put("OPENAI_API_KEY", "sk-openai");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.OPEN_AI.code());
    meta.setOpenAiBaseUrl(baseUrl + "/v1");
    meta.setOpenAiMaxRetries(0);

    chat(meta);
    assertNull(receivedKey.get());
  }

  @Test
  void openAiSendsAHopVariableByDefault() throws Exception {
    respondWith(
        "/v1/chat/completions",
        "Authorization",
        """
        {"id":"chat-1","object":"chat.completion","created":1,"model":"gpt-test",
         "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}
        """);
    environment.put("OPENAI_API_KEY", "sk-openai-env");
    variables.setVariable("OPENAI_API_KEY", "sk-openai");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.OPEN_AI.code());
    meta.setOpenAiBaseUrl(baseUrl + "/v1");
    meta.setOpenAiMaxRetries(0);

    chat(meta);
    assertEquals("Bearer sk-openai", receivedKey.get());
  }

  @Test
  void openAiSendsNoKeyForAnUnsetVariable() throws Exception {
    respondWith(
        "/v1/chat/completions",
        "Authorization",
        """
        {"id":"chat-1","object":"chat.completion","created":1,"model":"gpt-test",
         "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}
        """);

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.OPEN_AI.code());
    meta.setOpenAiBaseUrl(baseUrl + "/v1");
    meta.setOpenAiApiKey("${NOT_SET}");
    meta.setOpenAiMaxRetries(0);

    chat(meta);
    assertNull(receivedKey.get());
  }

  @Test
  void mistralSendsAHopVariableByDefault() throws Exception {
    respondWith(
        "/v1/chat/completions",
        "Authorization",
        """
        {"id":"chat-1","object":"chat.completion","created":1,"model":"mistral-test",
         "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}
        """);
    variables.setVariable("MISTRAL_API_KEY", "sk-mistral");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.MISTRAL.code());
    meta.setMistralBaseUrl(baseUrl + "/v1/");
    meta.setMistralMaxRetries(0);

    chat(meta);
    assertEquals("Bearer sk-mistral", receivedKey.get());
  }

  @Test
  void anthropicSendsAHopVariableByDefault() throws Exception {
    respondWith(
        "/v1/messages",
        "x-api-key",
        """
        {"id":"msg-1","type":"message","role":"assistant","model":"claude-test",
         "content":[{"type":"text","text":"ok"}],
         "stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}
        """);
    variables.setVariable("ANTHROPIC_API_KEY", "sk-anthropic");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.ANTHROPIC.code());
    meta.setAnthropicBaseUrl(baseUrl + "/v1/");
    meta.setAnthropicMaxRetries(0);

    chat(meta);
    assertEquals("sk-anthropic", receivedKey.get());
  }

  @Test
  void huggingFaceSendsAHopVariableByDefault() throws Exception {
    respondWith("/", "Authorization", """
        [{"generated_text":"ok"}]
        """);
    variables.setVariable("HF_ACCESS_TOKEN", "hf-token");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.HUGGING_FACE.code());
    meta.setHuggingFaceModelId(baseUrl + "/");

    chat(meta);
    assertEquals("Bearer hf-token", receivedKey.get());
  }

  /**
   * Only OpenAI can send no key. The Anthropic, Mistral and Hugging Face clients refuse to start
   * without one, so every call fails; the key tooltips and the transform page say so. The
   * environment variables are set, but the stub is not the providers' own endpoint.
   */
  @Test
  void anthropicMistralAndHuggingFaceRefuseToRunWithoutAKey() {
    environment.put("ANTHROPIC_API_KEY", "sk-anthropic");
    environment.put("MISTRAL_API_KEY", "sk-mistral");
    environment.put("HF_ACCESS_TOKEN", "hf-token");

    LanguageModelChatMeta anthropic = new LanguageModelChatMeta();
    anthropic.setModelType(ModelType.ANTHROPIC.code());
    anthropic.setAnthropicBaseUrl(baseUrl + "/v1/");

    LanguageModelChatMeta mistral = new LanguageModelChatMeta();
    mistral.setModelType(ModelType.MISTRAL.code());
    mistral.setMistralBaseUrl(baseUrl + "/v1/");

    LanguageModelChatMeta huggingFace = new LanguageModelChatMeta();
    huggingFace.setModelType(ModelType.HUGGING_FACE.code());
    huggingFace.setHuggingFaceModelId(baseUrl + "/");

    for (LanguageModelChatMeta meta : List.of(anthropic, mistral, huggingFace)) {
      assertThrows(IllegalArgumentException.class, () -> chat(meta), meta.getModelType());
    }
    assertNull(receivedKey.get());
  }

  private String resolve(String field) {
    return resolve(field, null);
  }

  private String resolve(String field, String endpoint) {
    LanguageModelFacade facade =
        new LanguageModelFacade(variables, new LanguageModelChatMeta(), environment::get, log);
    return facade.resolveApiKey(field, ApiKeyProvider.OPEN_AI, endpoint);
  }

  private void respondWith(String path, String keyHeader, String body) {
    server.createContext(
        path,
        exchange -> {
          try (InputStream in = exchange.getRequestBody()) {
            in.readAllBytes();
          }
          receivedKey.set(exchange.getRequestHeaders().getFirst(keyHeader));
          byte[] bytes = body.getBytes(UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
  }

  private void chat(LanguageModelChatMeta meta) {
    LanguageModelFacade facade = new LanguageModelFacade(variables, meta, environment::get, log);
    facade.chat(List.of(UserMessage.userMessage("Which key did you receive?")));
  }
}
