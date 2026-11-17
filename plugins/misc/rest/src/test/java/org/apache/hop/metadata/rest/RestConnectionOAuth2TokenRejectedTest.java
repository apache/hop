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

package org.apache.hop.metadata.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.rest.client.RestAuthenticator;
import org.apache.hop.metadata.rest.client.RestOAuth2TokenProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #9550: a server that stops accepting an OAuth 2 token before its expiry time - revoked,
 * rotated, or replaced because another client asked for a new one - gets a new token and the
 * request once more, instead of a 401 on every request until the cached token expires.
 *
 * <p>The stub is stateful: the token endpoint issues token-1, token-2, ...; the API accepts token-1
 * for {@link #token1Uses} requests and then answers 401 to it, and accepts any later token.
 */
class RestConnectionOAuth2TokenRejectedTest {

  private HttpServer server;
  private String baseUrl;

  private final AtomicInteger tokensIssued = new AtomicInteger();
  private final AtomicInteger token1Accepted = new AtomicInteger();

  /** The Authorization header of every API request, in order. */
  private final List<String> apiRequests = new CopyOnWriteArrayList<>();

  private volatile int token1Uses;
  private volatile boolean alwaysIssueToken1;
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
    alwaysIssueToken1 = false;
    rejectEverything = false;

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/token", this::issueToken);
    server.createContext("/workflows", this::api);
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
    RestOAuth2TokenProvider.clearCache();
  }

  @Test
  void aRejectedTokenIsReplacedOnceAndTheRequestSentAgain() throws Exception {
    RestConnection connection = oauth2Connection();

    for (int i = 0; i < 4; i++) {
      assertTrue(connection.getResponse(baseUrl + "/workflows").contains("\"ok\":true"));
    }

    assertEquals(2, tokensIssued.get(), "one token at the start, one after the 401");
    assertEquals(
        List.of(
            "Bearer token-1",
            "Bearer token-1",
            "Bearer token-1", // 401
            "Bearer token-2",
            "Bearer token-2"),
        apiRequests);
  }

  @Test
  void theConnectionTestRecoversToo() throws Exception {
    RestConnection connection = oauth2Connection();
    connection.setTestUrl(baseUrl + "/workflows");
    token1Uses = 0;

    connection.testConnection();

    assertEquals(2, tokensIssued.get());
    assertEquals(List.of("Bearer token-1", "Bearer token-2"), apiRequests);
  }

  @Test
  void aSecond401IsReportedAsBefore() {
    RestConnection connection = oauth2Connection();
    rejectEverything = true;

    HopException e =
        assertThrows(HopException.class, () -> connection.getResponse(baseUrl + "/workflows"));

    assertTrue(causeContains(e, "401"), e.getMessage());
    assertEquals(2, apiRequests.size(), "one retry, not a loop");
    assertEquals(2, tokensIssued.get());
  }

  @Test
  void theSameTokenHandedBackIsNotSentAgain() {
    RestConnection connection = oauth2Connection();
    alwaysIssueToken1 = true;
    token1Uses = 0;

    assertThrows(HopException.class, () -> connection.getResponse(baseUrl + "/workflows"));

    assertEquals(1, apiRequests.size(), "resending a rejected token gets the same 401");
  }

  @Test
  void anAuthorizationHeaderTheRowSuppliedIsNeverReplaced() throws Exception {
    RestConnection connection = oauth2Connection();
    RestAuthenticator authenticator = new RestAuthenticator(connection.createClientSettings());
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("authorization", "Bearer the-rows-own-token");
    String url = baseUrl + "/workflows";

    authenticator.applyRequestHeaders(headers, url);

    assertFalse(authenticator.refreshAfterUnauthorized(headers, url));
    assertEquals("Bearer the-rows-own-token", headers.get("authorization"));
    assertEquals(0, tokensIssued.get());
  }

  @Test
  void otherAuthenticationTypesAreNotRetried() throws Exception {
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setAuthType(RestConnection.API_KEY);
    connection.setAuthorizationHeaderName("Authorization");
    connection.setAuthorizationHeaderValue("Bearer token-1");
    token1Uses = 0;

    assertThrows(HopException.class, () -> connection.getResponse(baseUrl + "/workflows"));

    assertEquals(1, apiRequests.size());
    assertEquals(0, tokensIssued.get());
  }

  @Test
  void aRejectedBearerTokenSaysWhichAuthenticationTypeRenewsTokens() {
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setAuthType(RestConnection.BEARER);
    connection.setBearerToken("token-1");
    token1Uses = 0;

    HopException e =
        assertThrows(HopException.class, () -> connection.getResponse(baseUrl + "/workflows"));

    assertTrue(causeContains(e, "OAuth 2"), e.getMessage());
    assertEquals(1, apiRequests.size());
  }

  private RestConnection oauth2Connection() {
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setAuthType(RestConnection.OAUTH2);
    connection.setOauth2TokenUrl(baseUrl + "/token");
    connection.setOauth2ClientId("hop");
    connection.setOauth2ClientSecret("secret");
    return connection;
  }

  private static boolean causeContains(Throwable t, String needle) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause.getMessage() != null && cause.getMessage().contains(needle)) {
        return true;
      }
    }
    return false;
  }

  private void issueToken(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    int n = tokensIssued.incrementAndGet();
    String token = alwaysIssueToken1 ? "token-1" : "token-" + n;
    respond(
        exchange,
        200,
        "{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}");
  }

  private void api(HttpExchange exchange) throws IOException {
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
      respond(exchange, 200, "{\"ok\":true}");
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
