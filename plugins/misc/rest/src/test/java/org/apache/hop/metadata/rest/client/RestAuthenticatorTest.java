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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The one place authentication is decided, for both REST connections and the REST transform. */
class RestAuthenticatorTest {

  private static final String ORIGIN = "https://api.example.com";
  private static final String ON_ORIGIN = "https://api.example.com/v1/users";
  private static final String OFF_ORIGIN = "https://someone-elses-host.example.net/v1/users";

  private HttpServer server;
  private String secureUrl;

  /** Authorization header of every request the server saw, in order; null when absent. */
  private final List<String> seenAuthorizationHeaders =
      Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/secure",
        exchange -> {
          String authorization = exchange.getRequestHeaders().getFirst("Authorization");
          seenAuthorizationHeaders.add(authorization);
          if (authorization == null) {
            exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=\"test\"");
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
          }
          byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    secureUrl = "http://localhost:" + server.getAddress().getPort() + "/secure";
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  private static RestClientSettings basic() {
    RestClientSettings settings = new RestClientSettings();
    settings.setAuthType(RestAuthType.BASIC);
    settings.setBasicUsername("user");
    settings.setBasicPassword("password");
    settings.setAuthOrigin(ORIGIN);
    return settings;
  }

  private static Map<String, String> headersFor(RestClientSettings settings, String requestUri) {
    Map<String, String> headers = new LinkedHashMap<>();
    new RestAuthenticator(settings).applyRequestHeaders(headers, requestUri);
    return headers;
  }

  @Test
  void noAuthAddsNothing() {
    assertTrue(headersFor(new RestClientSettings(), ON_ORIGIN).isEmpty());
  }

  @Test
  void preemptiveBasicWritesTheAuthorizationHeader() {
    Map<String, String> headers = headersFor(basic(), ON_ORIGIN);

    assertEquals(
        "Basic "
            + Base64.getEncoder().encodeToString("user:password".getBytes(StandardCharsets.UTF_8)),
        headers.get("Authorization"));
  }

  @Test
  void challengeResponseBasicWritesNoHeader() {
    RestClientSettings settings = basic();
    settings.setBasicPreemptive(false);

    // The 401 is answered from the credentials provider on the client instead.
    assertTrue(headersFor(settings, ON_ORIGIN).isEmpty());
    assertNotNull(new RestAuthenticator(settings).createCredentialsProvider());
  }

  @Test
  void bearerWritesTheAuthorizationHeader() {
    RestClientSettings settings = new RestClientSettings();
    settings.setAuthType(RestAuthType.BEARER);
    settings.setBearerToken("t0ken");
    settings.setAuthOrigin(ORIGIN);

    assertEquals("Bearer t0ken", headersFor(settings, ON_ORIGIN).get("Authorization"));
  }

  @Test
  void apiKeyWritesItsOwnHeaderWithAnOptionalPrefix() {
    RestClientSettings settings = new RestClientSettings();
    settings.setAuthType(RestAuthType.API_KEY);
    settings.setApiKeyHeaderName("X-Api-Key");
    settings.setApiKeyHeaderValue("secret");
    settings.setAuthOrigin(ORIGIN);

    assertEquals("secret", headersFor(settings, ON_ORIGIN).get("X-Api-Key"));

    settings.setApiKeyHeaderPrefix("Token");
    assertEquals("Token secret", headersFor(settings, ON_ORIGIN).get("X-Api-Key"));
  }

  @Test
  void incompleteCredentialsAddNothing() {
    RestClientSettings bearer = new RestClientSettings();
    bearer.setAuthType(RestAuthType.BEARER);
    assertTrue(headersFor(bearer, ON_ORIGIN).isEmpty());

    RestClientSettings apiKey = new RestClientSettings();
    apiKey.setAuthType(RestAuthType.API_KEY);
    apiKey.setApiKeyHeaderName("X-Api-Key");
    assertTrue(headersFor(apiKey, ON_ORIGIN).isEmpty());
  }

  @Test
  void aRowSuppliedHeaderWins() {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("authorization", "Basic cm93OndpbnM=");
    new RestAuthenticator(basic()).applyRequestHeaders(headers, ON_ORIGIN);

    assertEquals("Basic cm93OndpbnM=", headers.get("authorization"));
    assertEquals(1, headers.size());
  }

  @Test
  void aBlankRowHeaderDoesNotWin() {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("Authorization", "  ");
    new RestAuthenticator(basic()).applyRequestHeaders(headers, ON_ORIGIN);

    assertTrue(headers.get("Authorization").startsWith("Basic "));
  }

  @Test
  void credentialsDoNotGoToAnotherHost() {
    // A URL field can point a row at any host while the client stays the same. Credentials
    // configured for the connection's own origin must not follow it there.
    assertTrue(headersFor(basic(), OFF_ORIGIN).isEmpty());
  }

  @Test
  void aDifferentSchemeOrPortIsADifferentOrigin() {
    assertTrue(headersFor(basic(), "http://api.example.com/v1/users").isEmpty());
    assertTrue(headersFor(basic(), "https://api.example.com:8443/v1/users").isEmpty());
  }

  @Test
  void withoutAnOriginTheCredentialsAreAlwaysSent() {
    // A URL field with no base URL to anchor it: there is nothing to check against, so the
    // long-standing behaviour of sending the credentials regardless is kept.
    RestClientSettings settings = basic();
    settings.setAuthOrigin(null);

    assertNotNull(headersFor(settings, OFF_ORIGIN).get("Authorization"));
  }

  @Test
  void preemptiveBasicReachesTheServerOnTheFirstRequest() throws Exception {
    RestClientSettings settings = basic();
    settings.setAuthOrigin(secureUrl);

    get(settings);

    assertEquals(1, seenAuthorizationHeaders.size());
    assertFalse(seenAuthorizationHeaders.contains(null));
  }

  @Test
  void challengeResponseBasicAnswersThe401() throws Exception {
    RestClientSettings settings = basic();
    settings.setAuthOrigin(secureUrl);
    settings.setBasicPreemptive(false);

    get(settings);

    // Unauthenticated first, then a retry carrying the credentials.
    assertEquals(2, seenAuthorizationHeaders.size());
    assertNull(seenAuthorizationHeaders.get(0));
    assertNotNull(seenAuthorizationHeaders.get(1));
  }

  /** GETs the secure URL with whatever authentication the settings describe. */
  private void get(RestClientSettings settings) throws Exception {
    try (CloseableHttpClient client = RestClientFactory.createClient(settings)) {
      HttpUriRequestBase request = new HttpUriRequestBase("GET", URI.create(secureUrl));
      headersFor(settings, secureUrl).forEach(request::addHeader);
      client.execute(
          request,
          response -> {
            assertEquals(200, response.getCode());
            EntityUtils.consume(response.getEntity());
            return null;
          });
    }
  }
}
