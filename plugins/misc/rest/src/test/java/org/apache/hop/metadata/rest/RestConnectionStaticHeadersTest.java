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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
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
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.rest.client.RestAuthType;
import org.apache.hop.metadata.rest.client.RestAuthenticator;
import org.apache.hop.metadata.rest.client.RestClientSettings;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #9551: headers configured on a REST connection go out on every request made through it, the
 * connection test included, and a header the request already carries wins.
 */
class RestConnectionStaticHeadersTest {

  private static final String USER_HEADER = "x-as-user-email";

  private HttpServer server;
  private String baseUrl;

  /** The x-as-user-email value of every request the server saw, "-" when absent. */
  private final List<String> seen = new CopyOnWriteArrayList<>();

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
    seen.clear();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/workflows", this::requireUserHeader);
    server.start();
    baseUrl = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void aConnectionSavedBeforeHeadersExistedHasNone() throws Exception {
    RestConnection connection = fromJson("{\"base_url\":\"https://api.example.com\"}");

    assertNotNull(connection.getHeaders());
    assertTrue(connection.getHeaders().isEmpty());
    assertTrue(connection.createClientSettings().getStaticHeaders().isEmpty());
  }

  @Test
  void headersSurviveAJsonRoundTrip() throws Exception {
    RestConnection connection = new RestConnection(new Variables());
    connection.setName("ironclad");
    connection.setHeaders(List.of(new RestConnectionHeader(USER_HEADER, "${API_USER}")));

    JSONObject json =
        new JsonMetadataParser<>(RestConnection.class, new MemoryMetadataProvider())
            .getJsonObject(connection);
    RestConnection loaded = fromJson(json.toJSONString());

    assertEquals(1, loaded.getHeaders().size());
    assertEquals(USER_HEADER, loaded.getHeaders().get(0).getName());
    assertEquals("${API_USER}", loaded.getHeaders().get(0).getValue());
  }

  @Test
  void headerValuesAreStoredEncrypted() throws Exception {
    RestConnection connection = new RestConnection(new Variables());
    connection.setName("ironclad");
    connection.setHeaders(List.of(new RestConnectionHeader("x-api-key", "s3cret-key")));

    String json =
        new JsonMetadataParser<>(RestConnection.class, new MemoryMetadataProvider())
            .getJsonObject(connection)
            .toJSONString();
    RestConnection loaded = fromJson(json);

    assertFalse(json.contains("s3cret-key"), json);
    assertTrue(json.contains("Encrypted "), json);
    assertEquals("s3cret-key", loaded.getHeaders().get(0).getValue());
  }

  @Test
  void aValueSavedInPlainTextStillLoads() throws Exception {
    RestConnection connection =
        fromJson("{\"headers\":[{\"name\":\"x-api-key\",\"value\":\"plain-key\"}]}");

    assertEquals("plain-key", connection.getHeaders().get(0).getValue());
  }

  @Test
  void anEmptyHeaderOfTheSameNameIsReplacedNotDoubled() {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("X-Tenant", "");

    authenticator("https://api.example.com")
        .applyRequestHeaders(headers, "https://api.example.com/workflows");

    assertEquals(Map.of(USER_HEADER, "hop@example.com", "x-tenant", "acme"), headers);
  }

  @Test
  void settingsCarryTheResolvedHeaders() throws HopException {
    Variables variables = new Variables();
    variables.setVariable("API_USER", "hop@example.com");
    variables.setVariable("HEADER_NAME", "x-tenant");
    RestConnection connection = new RestConnection(variables);
    connection.setBaseUrl("https://api.example.com");
    connection.setHeaders(
        List.of(
            new RestConnectionHeader(USER_HEADER, "${API_USER}"),
            new RestConnectionHeader("${HEADER_NAME}", "acme"),
            new RestConnectionHeader("", "a row without a name is skipped"),
            new RestConnectionHeader("x-secret", Encr.encryptPasswordIfNotUsingVariables("s3"))));

    Map<String, String> headers = connection.createClientSettings().getStaticHeaders();

    assertEquals(
        Map.of(USER_HEADER, "hop@example.com", "x-tenant", "acme", "x-secret", "s3"), headers);
  }

  @Test
  void aHeaderTheRequestAlreadyCarriesWins() {
    RestAuthenticator authenticator = authenticator("https://api.example.com");
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("X-As-User-Email", "row@example.com");

    authenticator.applyRequestHeaders(headers, "https://api.example.com/workflows");

    assertEquals(Map.of("X-As-User-Email", "row@example.com", "x-tenant", "acme"), headers);
  }

  @Test
  void theAuthenticationHeaderWinsOverAStaticHeaderOfTheSameName() {
    RestClientSettings settings = settings("https://api.example.com");
    settings.setAuthType(RestAuthType.BEARER);
    settings.setBearerToken("the-token");
    settings.getStaticHeaders().put("Authorization", "Bearer static");
    Map<String, String> headers = new LinkedHashMap<>();

    new RestAuthenticator(settings).applyRequestHeaders(headers, "https://api.example.com/x");

    assertEquals("Bearer the-token", headers.get("Authorization"));
  }

  @Test
  void anotherHostGetsNoStaticHeaders() {
    // Same rule as the credentials: a URL from an input field that names another host must not
    // receive what the connection was configured to send to its own.
    Map<String, String> headers = new LinkedHashMap<>();

    authenticator("https://api.example.com")
        .applyRequestHeaders(headers, "https://elsewhere.example.org/workflows");

    assertTrue(headers.isEmpty());
  }

  @Test
  void aRequestObjectKeepsTheHeadersItAlreadyHas() {
    // HTTP client, HTTP POST and the HTTP action write onto the request object directly.
    HttpGet request = new HttpGet("https://api.example.com/workflows");
    request.setHeader("x-tenant", "set-by-the-transform");

    authenticator("https://api.example.com").applyTo(request, "https://api.example.com/workflows");

    assertEquals("set-by-the-transform", request.getFirstHeader("x-tenant").getValue());
    assertEquals("hop@example.com", request.getFirstHeader(USER_HEADER).getValue());
  }

  @Test
  void theConnectionTestSendsTheHeaders() throws Exception {
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setTestUrl(baseUrl + "/workflows");
    connection.setHeaders(List.of(new RestConnectionHeader(USER_HEADER, "hop@example.com")));

    connection.testConnection();

    assertEquals(List.of("hop@example.com"), seen);
  }

  @Test
  void withoutTheHeaderTheSameConnectionTestFails() {
    // Proves the stub really rejects a request without the header, so the test above means
    // something.
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl(baseUrl);
    connection.setTestUrl(baseUrl + "/workflows");

    assertThrows(HopException.class, connection::testConnection);
    assertEquals(List.of("-"), seen);
    assertFalse(seen.contains("hop@example.com"));
  }

  private static RestClientSettings settings(String origin) {
    RestClientSettings settings = new RestClientSettings();
    settings.setAuthOrigin(origin);
    settings.getStaticHeaders().put(USER_HEADER, "hop@example.com");
    settings.getStaticHeaders().put("x-tenant", "acme");
    return settings;
  }

  private static RestAuthenticator authenticator(String origin) {
    return new RestAuthenticator(settings(origin));
  }

  private static RestConnection fromJson(String json) throws Exception {
    JsonParser parser = new JsonFactory().createParser(json);
    parser.nextToken();
    return new JsonMetadataParser<>(RestConnection.class, new MemoryMetadataProvider())
        .loadJsonObject(RestConnection.class, parser);
  }

  private void requireUserHeader(HttpExchange exchange) throws IOException {
    String user = exchange.getRequestHeaders().getFirst(USER_HEADER);
    seen.add(user == null ? "-" : user);
    if (user == null) {
      respond(exchange, 403, "{\"error\":\"x-as-user-email or x-as-user-id is required\"}");
    } else {
      respond(exchange, 200, "{\"user\":\"" + user + "\"}");
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
