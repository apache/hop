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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of the proxy settings a REST connection carries, against a stub proxy and a
 * stub origin server in this JVM.
 *
 * <p>{@link RestProxyRoutePlannerTest} covers the bypass matching in isolation; these tests cover
 * the wiring — that a client the factory builds really does route through the proxy, that the
 * bypass decision is taken per request rather than once per client, and that proxy credentials
 * answer a 407.
 *
 * <p>The stub proxy answers requests itself instead of forwarding them, which is enough: a proxied
 * request is recognisable by its absolute-form request line, so the origin never needs to be
 * involved to prove the request took the proxy route.
 */
class RestProxyRoutingTest {

  private static final String VIA_PROXY = "VIA-PROXY";
  private static final String FROM_ORIGIN = "FROM-ORIGIN";

  private HttpServer proxy;
  private HttpServer origin;

  private final List<String> proxiedRequests = new CopyOnWriteArrayList<>();
  private final List<String> proxyCredentials = new CopyOnWriteArrayList<>();
  private final List<String> originRequests = new CopyOnWriteArrayList<>();

  /** Set before building a client to make the stub proxy demand Basic authentication. */
  private volatile boolean proxyRequiresAuth;

  @BeforeEach
  void startServers() throws IOException {
    proxy = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    proxy.createContext("/", this::handleProxyRequest);
    proxy.start();

    origin = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    origin.createContext(
        "/",
        exchange -> {
          originRequests.add(exchange.getRequestURI().toString());
          respond(exchange, 200, FROM_ORIGIN);
        });
    origin.start();
  }

  @AfterEach
  void stopServers() {
    proxy.stop(0);
    origin.stop(0);
  }

  @Test
  void requestGoesThroughTheConfiguredProxy() throws Exception {
    RestClientSettings settings = proxySettings();

    assertEquals(VIA_PROXY, get(settings, originUrl("127.0.0.1")));

    // An absolute-form request line is what distinguishes a proxied request from a direct one.
    assertEquals(1, proxiedRequests.size());
    assertEquals(originUrl("127.0.0.1"), proxiedRequests.get(0));
    assertTrue(originRequests.isEmpty(), "the origin must not have been contacted directly");
  }

  @Test
  void aBypassedHostIsReachedDirectly() throws Exception {
    RestClientSettings settings = proxySettings();
    settings.setNonProxyHosts("localhost");

    assertEquals(FROM_ORIGIN, get(settings, originUrl("localhost")));

    assertTrue(proxiedRequests.isEmpty(), "a bypassed host must not go through the proxy");
    assertEquals(1, originRequests.size());
  }

  @Test
  void theBypassDecisionIsTakenPerRequestNotPerClient() throws Exception {
    // The REST transform can take its URL from an input field, so a single client serves a target
    // host that changes from row to row. Both requests below share one client on purpose.
    RestClientSettings settings = proxySettings();
    settings.setNonProxyHosts("localhost");

    try (CloseableHttpClient client = RestClientFactory.createClient(settings)) {
      assertEquals(FROM_ORIGIN, get(client, originUrl("localhost")));
      assertEquals(VIA_PROXY, get(client, originUrl("127.0.0.1")));
    }

    assertEquals(List.of(originUrl("127.0.0.1")), proxiedRequests);
    assertEquals(1, originRequests.size());
  }

  @Test
  void proxyCredentialsAnswerA407Challenge() throws Exception {
    proxyRequiresAuth = true;

    RestClientSettings settings = proxySettings();
    settings.setProxyUsername("proxyuser");
    settings.setProxyPassword("proxypass");

    assertEquals(VIA_PROXY, get(settings, originUrl("127.0.0.1")));

    assertEquals(1, proxyCredentials.size());
    assertEquals("proxyuser:proxypass", decodeBasic(proxyCredentials.get(0)));
  }

  @Test
  void withoutProxyCredentialsThe407IsSurfaced() throws Exception {
    proxyRequiresAuth = true;

    RestClientSettings settings = proxySettings();

    assertEquals(407, status(settings, originUrl("127.0.0.1")));
    assertTrue(proxiedRequests.isEmpty(), "the proxy never let the request through");
  }

  @Test
  void credentialsAreScopedToTheProxyAndNotSentToTheOrigin() throws Exception {
    // Proxy credentials belong to the proxy. A bypassed host talks to the origin directly, and
    // must not be handed them.
    proxyRequiresAuth = true;

    RestClientSettings settings = proxySettings();
    settings.setNonProxyHosts("localhost");
    settings.setProxyUsername("proxyuser");
    settings.setProxyPassword("proxypass");

    assertEquals(FROM_ORIGIN, get(settings, originUrl("localhost")));

    assertTrue(proxyCredentials.isEmpty(), "the proxy was not involved at all");
  }

  private void handleProxyRequest(HttpExchange exchange) throws IOException {
    String authorization = exchange.getRequestHeaders().getFirst("Proxy-Authorization");
    if (proxyRequiresAuth && authorization == null) {
      exchange.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=\"hop\"");
      respond(exchange, 407, "proxy authentication required");
      return;
    }
    if (authorization != null) {
      proxyCredentials.add(authorization);
    }
    proxiedRequests.add(exchange.getRequestURI().toString());
    respond(exchange, 200, VIA_PROXY);
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  /** Settings pointing at the stub proxy, with no bypass list. */
  private RestClientSettings proxySettings() {
    RestClientSettings settings = new RestClientSettings();
    settings.setProxyHost("localhost");
    settings.setProxyPort(proxy.getAddress().getPort());
    return settings;
  }

  /** The origin server's URL, under whichever host name the test wants to reach it by. */
  private String originUrl(String host) {
    return "http://" + host + ":" + origin.getAddress().getPort() + "/resource";
  }

  private String get(RestClientSettings settings, String url) throws Exception {
    try (CloseableHttpClient client = RestClientFactory.createClient(settings)) {
      return get(client, url);
    }
  }

  private String get(CloseableHttpClient client, String url) throws Exception {
    return client.execute(new HttpGet(url), response -> EntityUtils.toString(response.getEntity()));
  }

  private int status(RestClientSettings settings, String url) throws Exception {
    try (CloseableHttpClient client = RestClientFactory.createClient(settings)) {
      return client.execute(new HttpGet(url), response -> response.getCode());
    }
  }

  private static String decodeBasic(String authorizationHeader) {
    assertTrue(
        authorizationHeader.startsWith("Basic "),
        "expected Basic proxy credentials but got: " + authorizationHeader);
    return new String(
        Base64.getDecoder().decode(authorizationHeader.substring("Basic ".length())),
        StandardCharsets.UTF_8);
  }
}
