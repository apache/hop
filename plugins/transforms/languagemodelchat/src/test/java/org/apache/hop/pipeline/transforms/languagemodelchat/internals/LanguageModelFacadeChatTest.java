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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
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
 * Drives the providers against a stub http server, so the request serialization, the http call and
 * the response parsing are all covered without reaching out to a provider.
 */
class LanguageModelFacadeChatTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private final IVariables variables = new Variables();
  private final AtomicReference<String> lastRequestBody = new AtomicReference<>();

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

  /** Provider payloads are pretty printed, so requests are matched without their whitespace. */
  private String compactRequestBody() {
    return lastRequestBody.get().replaceAll("\\s+", "");
  }

  private void respondWith(String path, String body) {
    server.createContext(
        path,
        exchange -> {
          try (InputStream in = exchange.getRequestBody()) {
            lastRequestBody.set(new String(in.readAllBytes(), UTF_8));
          }
          byte[] bytes = body.getBytes(UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
  }

  @Test
  void openAi() throws Exception {
    respondWith(
        "/v1/chat/completions",
        """
        {"id":"chat-1","object":"chat.completion","created":1,"model":"gpt-test",
         "choices":[{"index":0,"message":{"role":"assistant","content":"four"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}
        """);

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.OPEN_AI.code());
    meta.setOpenAiBaseUrl(baseUrl + "/v1");
    meta.setOpenAiApiKey("test-key");
    meta.setOpenAiModelName("gpt-test");
    meta.setOpenAiResponseFormat("json");

    ChatResponse response = chat(meta, "two plus two");

    assertEquals("four", response.aiMessage().text());
    assertEquals(11, response.tokenUsage().inputTokenCount());
    assertEquals(22, response.tokenUsage().outputTokenCount());
    assertEquals(33, response.tokenUsage().totalTokenCount());
    assertTrue(compactRequestBody().contains("twoplustwo"), lastRequestBody.get());
    assertTrue(
        compactRequestBody().contains("\"response_format\":{\"type\":\"json_object\"}"),
        lastRequestBody.get());
  }

  @Test
  void ollama() throws Exception {
    respondWith(
        "/api/chat",
        """
        {"model":"phi3","created_at":"2026-09-11T00:00:00Z",
         "message":{"role":"assistant","content":"four"},
         "done":true,"done_reason":"stop","prompt_eval_count":11,"eval_count":22}
        """);

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.OLLAMA.code());
    meta.setOllamaImageEndpoint(baseUrl);
    meta.setOllamaModelName("phi3");
    meta.setOllamaFormat("json");

    ChatResponse response = chat(meta, "two plus two");

    assertEquals("four", response.aiMessage().text());
    assertEquals(11, response.tokenUsage().inputTokenCount());
    assertTrue(compactRequestBody().contains("\"format\":\"json\""), lastRequestBody.get());
  }

  @Test
  void anthropic() throws Exception {
    respondWith(
        "/v1/messages",
        """
        {"id":"msg-1","type":"message","role":"assistant","model":"claude-test",
         "content":[{"type":"text","text":"four"}],
         "stop_reason":"end_turn","usage":{"input_tokens":11,"output_tokens":22}}
        """);

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.ANTHROPIC.code());
    meta.setAnthropicBaseUrl(baseUrl + "/v1/");
    meta.setAnthropicApiKey("test-key");
    meta.setAnthropicModelName("claude-test");

    ChatResponse response = chat(meta, "two plus two");

    assertEquals("four", response.aiMessage().text());
    assertEquals(11, response.tokenUsage().inputTokenCount());
    assertEquals(22, response.tokenUsage().outputTokenCount());
  }

  @Test
  void mistral() throws Exception {
    respondWith(
        "/v1/chat/completions",
        """
        {"id":"chat-1","object":"chat.completion","created":1,"model":"mistral-test",
         "choices":[{"index":0,"message":{"role":"assistant","content":"four"},"finish_reason":"stop"}],
         "usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}
        """);

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.MISTRAL.code());
    meta.setMistralBaseUrl(baseUrl + "/v1/");
    meta.setMistralApiKey("test-key");
    meta.setMistralModelName("mistral-test");
    meta.setMistralResponseFormat("json_object");

    ChatResponse response = chat(meta, "two plus two");

    assertEquals("four", response.aiMessage().text());
    assertEquals(33, response.tokenUsage().totalTokenCount());
    assertTrue(
        compactRequestBody().contains("\"response_format\":{\"type\":\"json_object\"}"),
        lastRequestBody.get());
  }

  @Test
  void huggingFaceDedicatedEndpoint() throws Exception {
    respondWith("/", """
        [{"generated_text":"four"}]
        """);

    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.HUGGING_FACE.code());
    meta.setHuggingFaceModelId(baseUrl + "/");
    meta.setHuggingFaceAccessToken("test-token");
    meta.setHuggingFaceMaxNewTokens(10);

    ChatResponse response = chat(meta, "two plus two");

    assertEquals("four", response.aiMessage().text());
    assertTrue(compactRequestBody().contains("\"max_new_tokens\":10"), lastRequestBody.get());
    assertTrue(compactRequestBody().contains("\"wait_for_model\":true"), lastRequestBody.get());
  }

  private ChatResponse chat(LanguageModelChatMeta meta, String message) throws Exception {
    LanguageModelFacade facade = new LanguageModelFacade(variables, meta);
    return facade.chat(List.of(dev.langchain4j.data.message.UserMessage.userMessage(message)));
  }
}
