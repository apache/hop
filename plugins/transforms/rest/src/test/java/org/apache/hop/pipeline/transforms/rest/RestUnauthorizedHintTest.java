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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.client.RestAuthenticator;
import org.apache.hop.metadata.rest.client.RestOAuth2TokenProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #9550: a REST transform whose connection uses a fixed Bearer token says once, per copy,
 * that a token that expires needs the OAuth 2 authentication type. Once rather than per row,
 * because every row after the first gets the same 401; and not at all for OAuth 2, which renews
 * tokens.
 */
class RestUnauthorizedHintTest {

  private static final String HINT =
      BaseMessages.getString(RestAuthenticator.class, "RestAuthenticator.Hint.BearerUnauthorized");

  private HttpServer server;
  private String baseUrl;
  private final AtomicInteger apiRequests = new AtomicInteger();

  @BeforeAll
  static void initPasswordEncoder() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @BeforeEach
  void startServer() throws IOException {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
    RestOAuth2TokenProvider.clearCache();
    apiRequests.set(0);
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/token", this::issueToken);
    server.createContext(
        "/api",
        exchange -> {
          apiRequests.incrementAndGet();
          respond(exchange, 401, "{\"error\":\"invalid_token\"}");
        });
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
    RestOAuth2TokenProvider.clearCache();
  }

  @Test
  void aRejectedBearerTokenLogsTheHintOncePerCopy() throws Exception {
    LoggingRest copy0 = build(0, bearerConnection());
    LoggingRest copy1 = build(1, bearerConnection());

    for (int i = 0; i < 3; i++) {
      assertEquals(401L, status(copy0.callRest(new Object[] {"row-" + i})));
      assertEquals(401L, status(copy1.callRest(new Object[] {"row-" + i})));
    }

    assertEquals(1, copy0.hintsLogged(), "once for the first copy, not once per row");
    assertEquals(1, copy1.hintsLogged(), "and once for the second copy");
    assertEquals(6, apiRequests.get(), "a fixed token is never sent a second time");
  }

  @Test
  void aRejectedOAuth2TokenLogsNoHint() throws Exception {
    LoggingRest rest = build(0, oauth2Connection());

    for (int i = 0; i < 3; i++) {
      assertEquals(401L, status(rest.callRest(new Object[] {"row-" + i})));
    }

    assertEquals(0, rest.hintsLogged());
    assertTrue(apiRequests.get() > 3, "OAuth 2 tries a new token, which is the whole difference");
  }

  private RestConnection bearerConnection() {
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setAuthType(RestConnection.BEARER);
    connection.setBearerToken("a-fixed-token-that-has-expired");
    return connection;
  }

  private RestConnection oauth2Connection() {
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setAuthType(RestConnection.OAUTH2);
    connection.setOauth2TokenUrl(baseUrl + "/token");
    connection.setOauth2ClientId("hop-hint");
    connection.setOauth2ClientSecret("secret");
    return connection;
  }

  private static Long status(Object[] row) {
    // Input field, result, status code.
    return (Long) row[2];
  }

  private LoggingRest build(int copyNr, RestConnection connection) {
    String url = baseUrl + "/api";

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

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = inputRowMeta;
    data.outputRowMeta = inputRowMeta.clone();

    LoggingRest rest =
        new LoggingRest(transformMeta, meta, data, copyNr, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(new MemoryMetadataProvider());

    // The connection is normally loaded from metadata in init().
    try {
      Field field = Rest.class.getDeclaredField("connection");
      field.setAccessible(true);
      field.set(rest, connection);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
    return rest;
  }

  /** Records what the transform logs at the basic level. */
  static class LoggingRest extends Rest {
    final List<String> basicLog = new ArrayList<>();

    LoggingRest(
        TransformMeta transformMeta,
        RestMeta meta,
        RestData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public void logBasic(String message) {
      basicLog.add(message);
      super.logBasic(message);
    }

    long hintsLogged() {
      return basicLog.stream().filter(HINT::equals).count();
    }
  }

  private void issueToken(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    String token = "token-" + System.nanoTime();
    respond(
        exchange,
        200,
        "{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}");
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
