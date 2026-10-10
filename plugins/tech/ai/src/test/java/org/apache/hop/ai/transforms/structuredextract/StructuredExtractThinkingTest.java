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

package org.apache.hop.ai.transforms.structuredextract;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ai.engine.AiChatModelFactory;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The provider's Thinking option on the request Structured extract actually sends: built by its own
 * request builder and held to a JSON schema, against a stub Ollama. The integration test cannot see
 * the flag, because Structured extract reports no token counts.
 */
class StructuredExtractThinkingTest {

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
          byte[] bytes =
              """
              {"model":"qwen3","created_at":"2026-10-10T00:00:00Z",
               "message":{"role":"assistant","content":"{\\"severity\\":\\"high\\"}"},
               "done":true,"done_reason":"stop","prompt_eval_count":11,"eval_count":5}
              """
                  .getBytes(UTF_8);
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
  void thinkingOffIsSentAlongsideTheSchema() throws Exception {
    String body = send("OFF");

    assertTrue(body.contains("\"think\":false"), lastRequestBody.get());
    assertTrue(body.contains("\"format\":{"), lastRequestBody.get());
    assertTrue(body.contains("\"severity\""), lastRequestBody.get());
  }

  @Test
  void thinkingOnIsSentAlongsideTheSchema() throws Exception {
    String body = send("ON");

    assertTrue(body.contains("\"think\":true"), lastRequestBody.get());
    assertTrue(body.contains("\"format\":{"), lastRequestBody.get());
  }

  @Test
  void thinkingDefaultSendsTheSchemaWithoutThink() throws Exception {
    String body = send("");

    assertFalse(body.contains("\"think\""), lastRequestBody.get());
    assertTrue(body.contains("\"format\":{"), lastRequestBody.get());
  }

  /** Sends one row's request the way Structured extract does, and returns the compacted body. */
  private String send(String thinking) throws Exception {
    AiProvider provider = new AiProvider();
    provider.setName("ollama-test");
    OllamaProvider backend = new OllamaProvider();
    backend.setPluginId("ollama");
    provider.setProvider(backend);
    provider.setBaseUrl(baseUrl);
    provider.setModelName("qwen3");
    provider.setThinking(thinking);

    JsonSchema schema =
        ExtractionSchema.build(
            List.of(new StructuredExtractField("severity", "String", "how urgent", true)),
            "Structured extract");
    ChatModel model = AiChatModelFactory.createChatModel(provider, "", new Variables());
    ResponseFormat format = StructuredExtract.responseFormatFor(model, schema);
    ChatRequest request = StructuredExtract.buildRequest("system", "a ticket", format);

    assertEquals("{\"severity\":\"high\"}", model.chat(request).aiMessage().text());
    return lastRequestBody.get().replaceAll("\\s+", "");
  }
}
