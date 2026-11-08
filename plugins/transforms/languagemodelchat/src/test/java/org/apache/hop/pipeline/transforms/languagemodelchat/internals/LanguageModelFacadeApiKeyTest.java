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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sun.net.httpserver.HttpServer;
import dev.langchain4j.data.message.UserMessage;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Which API key is sent, see issue #9385. The key fields default to the provider's variable, such
 * as {@code ${OPENAI_API_KEY}}, which is read from the Hop variables and then from the environment.
 * The environment is handed to the facade, so no real environment variable is needed.
 */
class LanguageModelFacadeApiKeyTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private final IVariables variables = new Variables();
  private final Map<String, String> environment = new HashMap<>();
  private final AtomicReference<String> receivedKey = new AtomicReference<>();

  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    baseUrl = "http://localhost:" + server.getAddress().getPort();
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

  // ------------------------------------------------------------------ the key on the wire

  @Test
  void openAiSendsTheEnvironmentKeyByDefault() throws Exception {
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
  void mistralSendsTheEnvironmentKeyByDefault() throws Exception {
    respondWith(
        "/v1/chat/completions",
        "Authorization",
        """
        {"id":"chat-1","object":"chat.completion","created":1,"model":"mistral-test",
         "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}
        """);
    environment.put("MISTRAL_API_KEY", "sk-mistral");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.MISTRAL.code());
    meta.setMistralBaseUrl(baseUrl + "/v1/");
    meta.setMistralMaxRetries(0);

    chat(meta);
    assertEquals("Bearer sk-mistral", receivedKey.get());
  }

  @Test
  void anthropicSendsTheEnvironmentKeyByDefault() throws Exception {
    respondWith(
        "/v1/messages",
        "x-api-key",
        """
        {"id":"msg-1","type":"message","role":"assistant","model":"claude-test",
         "content":[{"type":"text","text":"ok"}],
         "stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}
        """);
    environment.put("ANTHROPIC_API_KEY", "sk-anthropic");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.ANTHROPIC.code());
    meta.setAnthropicBaseUrl(baseUrl + "/v1/");
    meta.setAnthropicMaxRetries(0);

    chat(meta);
    assertEquals("sk-anthropic", receivedKey.get());
  }

  @Test
  void huggingFaceSendsTheEnvironmentTokenByDefault() throws Exception {
    respondWith("/", "Authorization", """
        [{"generated_text":"ok"}]
        """);
    environment.put("HF_ACCESS_TOKEN", "hf-token");

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.HUGGING_FACE.code());
    meta.setHuggingFaceModelId(baseUrl + "/");

    chat(meta);
    assertEquals("Bearer hf-token", receivedKey.get());
  }

  private String resolve(String field) {
    LanguageModelFacade facade =
        new LanguageModelFacade(variables, new LanguageModelChatMeta(), environment::get);
    return facade.resolveApiKey(field, LanguageModelChatMeta.OPENAI_API_KEY_VARIABLE);
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
    LanguageModelFacade facade = new LanguageModelFacade(variables, meta, environment::get);
    facade.chat(List.of(UserMessage.userMessage("Which key did you receive?")));
  }
}
