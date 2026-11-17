/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.client.RestOAuth2TokenProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #9550: the REST transform on an OAuth 2 connection gets a new token when the server rejects
 * the cached one with a 401, and sends the request once more. Run against a server in this JVM with
 * a real client, because what matters is the sequence of requests that reaches the server.
 */
class RestOAuth2TokenRejectedTest {

  private HttpServer server;
  private String baseUrl;

  private final AtomicInteger tokensIssued = new AtomicInteger();
  private final AtomicInteger token1Accepted = new AtomicInteger();
  private final List<String> apiRequests = new CopyOnWriteArrayList<>();
  private volatile int token1Uses;
  private volatile boolean rejectEverything;

  @BeforeEach
  void startServer() throws IOException {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
    RestOAuth2TokenProvider.clearCache();
    tokensIssued.set(0);
    token1Accepted.set(0);
    apiRequests.clear();
    token1Uses = 2;
    rejectEverything = false;

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/token", this::issueToken);
    server.createContext("/workflows", exchange -> api(exchange, "{\"ok\":true}"));
    server.createContext("/stream", exchange -> api(exchange, "{\"n\":1}\n{\"n\":2}\n"));
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
    RestOAuth2TokenProvider.clearCache();
  }

  @Test
  void rowsAfterTheServerRejectsTheTokenStillSucceed() throws Exception {
    // Token-1 is good for two requests: rows 3 and 4 used to come back 401.
    CollectingRest rest = build("/workflows", false);

    List<Long> statuses = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      statuses.add(status(rest.callRest(new Object[] {"row-" + i})));
    }

    assertEquals(List.of(200L, 200L, 200L, 200L), statuses);
    assertEquals(2, tokensIssued.get());
    assertEquals(
        List.of(
            "Bearer token-1",
            "Bearer token-1",
            "Bearer token-1",
            "Bearer token-2",
            "Bearer token-2"),
        apiRequests);
  }

  @Test
  void aSecond401IsReportedAsBefore() throws Exception {
    CollectingRest rest = build("/workflows", false);
    rejectEverything = true;

    assertEquals(401L, status(rest.callRest(new Object[] {"row"})));
    assertEquals(2, apiRequests.size(), "one more try, not a loop");
  }

  @Test
  void a401InTheRetryStatusCodesGetsTheNewTokenFirst() throws Exception {
    // Adding 401 to the retry codes used to resend the same rejected token every time.
    CollectingRest rest = build("/workflows", false);
    rest.meta().setRetryMethods(new ArrayList<>(List.of(RestMeta.HTTP_METHOD_GET)));
    rest.meta().setRetryStatusCodes(new ArrayList<>(List.of("401")));
    rest.meta().setRetryTimes(3);
    rest.meta().setRetryDelayMs(1L);
    token1Uses = 0;

    assertEquals(200L, status(rest.callRest(new Object[] {"row"})));
    assertEquals(List.of("Bearer token-1", "Bearer token-2"), apiRequests);
  }

  @Test
  void aStreamingRequestRecoversBeforeAnyRecordIsEmitted() throws Exception {
    CollectingRest rest = build("/stream", true);
    token1Uses = 0;

    rest.callRest(new Object[] {"row"});

    assertEquals(2, rest.emitted.size());
    assertEquals(List.of("Bearer token-1", "Bearer token-2"), apiRequests);
  }

  @Test
  void aStreamingRequestStillFailsOnASecond401() {
    CollectingRest rest = build("/stream", true);
    rejectEverything = true;

    assertThrows(HopException.class, () -> rest.callRest(new Object[] {"row"}));
    assertEquals(2, apiRequests.size());
    assertEquals(0, rest.emitted.size());
  }

  private static Long status(Object[] row) {
    // Input field, result, status code.
    return (Long) row[2];
  }

  private CollectingRest build(String path, boolean streaming) {
    String url = baseUrl + path;

    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setDefault();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl(url);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("status");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = url;
    data.resultFieldName = "result";
    data.resultCodeFieldName = "status";
    if (streaming) {
      meta.setStreamingEnabled(true);
      meta.setStreamingFormat(RestStreamingFormat.NDJSON);
      data.streaming = true;
      data.streamingFormat = RestStreamingFormat.NDJSON;
    }

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = inputRowMeta;
    data.outputRowMeta = inputRowMeta.clone();

    CollectingRest rest =
        new CollectingRest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(new MemoryMetadataProvider());

    // The connection is normally loaded from metadata in init().
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setAuthType(RestConnection.OAUTH2);
    connection.setOauth2TokenUrl(baseUrl + "/token");
    connection.setOauth2ClientId("hop");
    connection.setOauth2ClientSecret("secret");
    try {
      Field field = Rest.class.getDeclaredField("connection");
      field.setAccessible(true);
      field.set(rest, connection);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
    return rest;
  }

  /** Collects streamed rows instead of passing them downstream. */
  static class CollectingRest extends Rest {
    final List<Object[]> emitted = new ArrayList<>();

    CollectingRest(
        TransformMeta transformMeta,
        RestMeta meta,
        RestData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    RestMeta meta() {
      return (RestMeta) getMeta();
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) {
      emitted.add(row);
    }
  }

  private void issueToken(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    String token = "token-" + tokensIssued.incrementAndGet();
    respond(
        exchange,
        200,
        "{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}");
  }

  private void api(HttpExchange exchange, String body) throws IOException {
    String authorization = exchange.getRequestHeaders().getFirst("Authorization");
    apiRequests.add(authorization == null ? "-" : authorization);
    boolean accepted;
    if (rejectEverything || authorization == null) {
      accepted = false;
    } else if (authorization.equals("Bearer token-1")) {
      accepted = token1Accepted.incrementAndGet() <= token1Uses;
    } else {
      accepted = authorization.startsWith("Bearer token-");
    }
    if (accepted) {
      respond(exchange, 200, body);
    } else {
      respond(exchange, 401, "{\"error\":\"invalid_token\"}");
    }
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
