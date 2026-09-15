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

package org.apache.hop.core.variables.resolver.vault;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal Kubernetes TokenReview API used by Vault's Kubernetes auth backend in tests.
 * Authenticates JWTs whose {@code sub} claim is {@code system:serviceaccount:<namespace>:<name>}
 * for the bound service account.
 */
final class KubernetesTokenReviewMock implements AutoCloseable {

  private static final Pattern SUB = Pattern.compile("\"sub\"\\s*:\\s*\"([^\"]+)\"");

  private final HttpServer server;

  KubernetesTokenReviewMock() throws IOException {
    server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
    server.createContext("/", this::handle);
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
  }

  int getPort() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    String method = exchange.getRequestMethod();
    byte[] body = readBody(exchange);

    if ("GET".equalsIgnoreCase(method) && path.contains("healthz")) {
      send(exchange, 200, "text/plain", "ok");
      return;
    }

    if ("POST".equalsIgnoreCase(method) && path.contains("tokenreviews")) {
      send(
          exchange, 200, "application/json", tokenReview(new String(body, StandardCharsets.UTF_8)));
      return;
    }

    send(exchange, 200, "application/json", "{\"kind\":\"APIVersions\",\"versions\":[\"v1\"]}");
  }

  private static byte[] readBody(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
    try (InputStream in = exchange.getRequestBody()) {
      return in.readAllBytes();
    }
  }

  private static void send(
      com.sun.net.httpserver.HttpExchange exchange, int status, String contentType, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static String tokenReview(String requestBody) {
    String jwt = jsonString(requestBody, "token");
    String sub = jwtSubject(jwt);
    boolean authenticated = sub.startsWith("system:serviceaccount:");
    String username = authenticated ? sub : "";
    return """
        {
          "apiVersion": "authentication.k8s.io/v1",
          "kind": "TokenReview",
          "status": {
            "authenticated": %s,
            "user": {
              "username": "%s",
              "uid": "1",
              "groups": ["system:serviceaccounts"]
            }
          }
        }
        """
        .formatted(authenticated, username);
  }

  private static String jsonString(String json, String field) {
    Matcher matcher =
        Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
    return matcher.find() ? matcher.group(1) : "";
  }

  static String jwtSubject(String jwt) {
    if (jwt == null || jwt.isBlank()) {
      return "";
    }
    String[] parts = jwt.split("\\.");
    if (parts.length < 2) {
      return "";
    }
    try {
      String payload =
          new String(Base64.getUrlDecoder().decode(padBase64(parts[1])), StandardCharsets.UTF_8);
      Matcher matcher = SUB.matcher(payload);
      return matcher.find() ? matcher.group(1) : "";
    } catch (IllegalArgumentException e) {
      return "";
    }
  }

  private static String padBase64(String value) {
    int remainder = value.length() % 4;
    if (remainder == 0) {
      return value;
    }
    return value + "=".repeat(4 - remainder);
  }
}
