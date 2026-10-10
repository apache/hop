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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.ollama.OllamaChatRequestParameters;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The model Extract graph, Structured extract and the AI Assistant use, against a stub Ollama, so
 * the Thinking option is checked in the request that is actually sent.
 */
class AiChatModelFactoryTest {

  /** A thinking model answers with its reasoning in a field of its own. */
  private static final String THINKING_ANSWER =
      """
      {"model":"qwen3","created_at":"2026-10-10T00:00:00Z",
       "message":{"role":"assistant","content":"OK","thinking":"The user wants OK."},
       "done":true,"done_reason":"stop","prompt_eval_count":11,"eval_count":22}
      """;

  private final AtomicReference<String> lastRequestBody = new AtomicReference<>();
  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/api/chat",
        exchange -> {
          try (InputStream in = exchange.getRequestBody()) {
            lastRequestBody.set(new String(in.readAllBytes(), UTF_8));
          }
          byte[] bytes = THINKING_ANSWER.getBytes(UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    baseUrl = "http://localhost:" + server.getAddress().getPort();
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void thinkingDefaultSendsNoThink() throws Exception {
    ChatModel model = model("");

    assertNull(((OllamaChatRequestParameters) model.defaultRequestParameters()).think());
    model.chat("Reply with OK");
    assertFalse(compactRequestBody().contains("\"think\""), lastRequestBody.get());
  }

  @Test
  void thinkingOffSendsThinkFalse() throws Exception {
    ChatModel model = model("Off");

    assertEquals(
        Boolean.FALSE, ((OllamaChatRequestParameters) model.defaultRequestParameters()).think());
    model.chat("Reply with OK");
    assertTrue(compactRequestBody().contains("\"think\":false"), lastRequestBody.get());
  }

  @Test
  void thinkingOnSendsThinkTrue() throws Exception {
    model("On").chat("Reply with OK");

    assertTrue(compactRequestBody().contains("\"think\":true"), lastRequestBody.get());
  }

  @Test
  void thinkingCanBeAVariable() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("AI_THINKING", "off");
    AiProvider provider = provider("${AI_THINKING}");

    AiChatModelFactory.createChatModel(provider, "", variables).chat("Reply with OK");

    assertTrue(compactRequestBody().contains("\"think\":false"), lastRequestBody.get());
  }

  @Test
  void theReasoningNeverReachesTheAnswer() throws Exception {
    for (String thinking : new String[] {"", "Off", "On"}) {
      ChatResponse response =
          model(thinking)
              .chat(ChatRequest.builder().messages(UserMessage.from("Reply with OK")).build());
      assertEquals("OK", response.aiMessage().text(), thinking);
      assertNull(response.aiMessage().thinking(), thinking);
    }
  }

  private ChatModel model(String thinking) throws Exception {
    return AiChatModelFactory.createChatModel(provider(thinking), "", new Variables());
  }

  private AiProvider provider(String thinking) {
    AiProvider provider = new AiProvider();
    provider.setName("ollama-test");
    OllamaProvider backend = new OllamaProvider();
    backend.setPluginId("ollama");
    provider.setProvider(backend);
    provider.setBaseUrl(baseUrl);
    provider.setModelName("qwen3");
    provider.setThinking(thinking);
    return provider;
  }

  /** Request payloads are pretty printed, so they are matched without their whitespace. */
  private String compactRequestBody() {
    return lastRequestBody.get().replaceAll("\\s+", "");
  }
}
