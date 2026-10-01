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

package org.apache.hop.pipeline.transforms.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The transform can take its client, its base URL and its credentials from a REST connection. */
class HttpConnectionTest {

  private static final String CONNECTION = "test-connection";
  private static final String USER = "restuser";
  private static final String PASSWORD = "restpassword";
  private static final String PAYLOAD = "from the origin";

  /** A base URL on a host the tests never reach: the connection's credentials belong to it. */
  private static final String OTHER_ORIGIN = "http://api.example.invalid";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private HttpServer server;
  private final AtomicReference<String> authorization = new AtomicReference<>();
  private final AtomicReference<String> requestPath = new AtomicReference<>();

  @BeforeAll
  static void setupBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    server.createContext("/", this::respond);
    server.start();
  }

  private void respond(HttpExchange exchange) throws IOException {
    List<String> auth = exchange.getRequestHeaders().get("Authorization");
    authorization.set(auth == null ? null : auth.get(0));
    requestPath.set(exchange.getRequestURI().getPath());

    byte[] body = PAYLOAD.getBytes(UTF_8);
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(body);
    }
    exchange.close();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
    authorization.set(null);
    requestPath.set(null);
  }

  @Test
  void aRelativeUrlIsResolvedAgainstTheConnectionBaseUrl() throws Exception {
    HttpMeta meta = meta("v1/items");
    Http http = transform(meta, provider(null, null));

    assertTrue(http.init());
    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals(PAYLOAD, result[0]);
    assertEquals("/v1/items", requestPath.get());
    http.dispose();
  }

  @Test
  void theConnectionCredentialsAuthenticateTheRequest() throws Exception {
    HttpMeta meta = meta("v1/items");
    Http http = transform(meta, provider(USER, PASSWORD));

    assertTrue(http.init());
    http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals(
        "Basic " + Base64.getEncoder().encodeToString((USER + ":" + PASSWORD).getBytes(UTF_8)),
        authorization.get());
    http.dispose();
  }

  @Test
  void anAbsoluteUrlOnAnotherHostGetsNoConnectionCredentials() throws Exception {
    // The credentials belong to the connection's base URL host. An absolute URL naming another
    // host is still called, but without them.
    HttpMeta meta = meta("http://localhost:" + server.getAddress().getPort() + "/elsewhere");
    Http http = transform(meta, provider(USER, PASSWORD, OTHER_ORIGIN));

    assertTrue(http.init());
    http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals("/elsewhere", requestPath.get());
    assertNull(authorization.get(), "the connection credentials went to another host");
    http.dispose();
  }

  @Test
  void withoutAConnectionTheTransformUsesItsOwnFields() throws Exception {
    HttpMeta meta = new HttpMeta();
    meta.setUrl("http://localhost:" + server.getAddress().getPort() + "/direct");
    Http http = transform(meta, new MemoryMetadataProvider());

    assertTrue(http.init());
    http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals("/direct", requestPath.get());
    assertNull(authorization.get());
    http.dispose();
  }

  @Test
  void aMissingConnectionStopsTheTransform() throws Exception {
    HttpMeta meta = meta("v1/items");
    meta.setConnectionName("does-not-exist");

    // Falling back to the transform's own fields would send the request somewhere else,
    // unauthenticated, so init has to fail instead.
    assertFalse(transform(meta, new MemoryMetadataProvider()).init());
  }

  private HttpMeta meta(String url) {
    HttpMeta meta = new HttpMeta();
    meta.setConnectionName(CONNECTION);
    meta.setUrl(url);
    return meta;
  }

  private MemoryMetadataProvider provider(String user, String password) throws Exception {
    return provider(user, password, "http://localhost:" + server.getAddress().getPort());
  }

  private MemoryMetadataProvider provider(String user, String password, String baseUrl)
      throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    RestConnection connection = new RestConnection();
    connection.setName(CONNECTION);
    connection.setBaseUrl(baseUrl);
    if (user != null) {
      connection.setAuthType(RestConnection.BASIC);
      connection.setUsername(user);
      connection.setPassword(password);
    }
    provider.getSerializer(RestConnection.class).save(connection);
    return provider;
  }

  private Http transform(HttpMeta meta, MemoryMetadataProvider provider) {
    HttpData data = new HttpData();
    data.argNrs = new int[0];

    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("HttpConnectionTest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("HttpConnectionTest");
    pipelineMeta.addTransform(transformMeta);

    Http http = new Http(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    http.setMetadataProvider(provider);
    return http;
  }
}
