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

package org.apache.hop.metadata.rest.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #9550: the client built for a REST connection sends a request once more with a new OAuth 2
 * token when the server rejects the one it carried. It is in the client, so every caller that
 * builds its requests itself (HTTP client, HTTP POST, the HTTP action) gets it.
 */
class RestOAuth2RenewalExecTest {

  private static final String BODY = "{\"name\":\"a body that has to arrive twice\"}";

  private HttpServer server;
  private String baseUrl;
  private final AtomicInteger tokensIssued = new AtomicInteger();

  /** "authorization body" of every API request, in order. */
  private final List<String> apiRequests = new CopyOnWriteArrayList<>();

  @BeforeEach
  void startServer() throws IOException {
    RestOAuth2TokenProvider.clearCache();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/token", this::issueToken);
    server.createContext("/api", this::api);
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
    RestOAuth2TokenProvider.clearCache();
  }

  @Test
  void aRejectedTokenIsReplacedAndTheBodySentAgain() throws Exception {
    RestClientSettings settings = settings();
    RestAuthenticator authenticator = new RestAuthenticator(settings);

    int status;
    try (CloseableHttpClient client =
        RestClientFactory.createClient(settings, authenticator, null)) {
      status = post(client, authenticator, new StringEntity(BODY, ContentType.APPLICATION_JSON));
    }

    assertEquals(200, status);
    assertEquals(List.of("Bearer token-1 " + BODY, "Bearer token-2 " + BODY), apiRequests);
  }

  @Test
  void aBodyThatCannotBeReadTwiceIsNotResentButTheTokenIsDropped() throws Exception {
    RestClientSettings settings = settings();
    RestAuthenticator authenticator = new RestAuthenticator(settings);

    int first;
    int second;
    try (CloseableHttpClient client =
        RestClientFactory.createClient(settings, authenticator, null)) {
      first = post(client, authenticator, streamed());
      second = post(client, authenticator, streamed());
    }

    assertEquals(401, first);
    assertEquals(200, second);
    assertEquals(List.of("Bearer token-1 " + BODY, "Bearer token-2 " + BODY), apiRequests);
  }

  @Test
  void aClientWithoutTheAuthenticatorLeavesThe401Alone() throws Exception {
    RestClientSettings settings = settings();
    RestAuthenticator authenticator = new RestAuthenticator(settings);

    int status;
    try (CloseableHttpClient client = RestClientFactory.createClient(settings)) {
      status = post(client, authenticator, new StringEntity(BODY, ContentType.APPLICATION_JSON));
    }

    assertEquals(401, status);
    assertEquals(1, apiRequests.size());
  }

  private int post(CloseableHttpClient client, RestAuthenticator authenticator, HttpEntity body)
      throws IOException {
    String url = baseUrl + "/api";
    HttpPost post = new HttpPost(url);
    authenticator.applyTo(post, url);
    post.setEntity(body);
    return client.execute(post, response -> response.getCode());
  }

  private static HttpEntity streamed() {
    byte[] bytes = BODY.getBytes(StandardCharsets.UTF_8);
    return new InputStreamEntity(
        new ByteArrayInputStream(bytes), bytes.length, ContentType.APPLICATION_JSON);
  }

  private RestClientSettings settings() {
    RestClientSettings settings = new RestClientSettings();
    settings.setAuthOrigin(baseUrl);
    settings.setAuthType(RestAuthType.OAUTH2);
    settings.setOauth2TokenUrl(baseUrl + "/token");
    settings.setOauth2ClientId("hop-renewal");
    settings.setOauth2ClientSecret("secret");
    return settings;
  }

  private void issueToken(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    String token = "token-" + tokensIssued.incrementAndGet();
    respond(
        exchange,
        200,
        "{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}");
  }

  /** Token-1 is rejected straight away, as if the provider revoked it. */
  private void api(HttpExchange exchange) throws IOException {
    String authorization = exchange.getRequestHeaders().getFirst("Authorization");
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    apiRequests.add(authorization + " " + body);
    if ("Bearer token-1".equals(authorization)) {
      respond(exchange, 401, "{\"error\":\"invalid_token\"}");
    } else {
      respond(exchange, 200, "{\"ok\":true}");
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
