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

package org.apache.hop.workflow.actions.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The action can take its client, its base URL and its credentials from a REST connection. */
class ActionHttpConnectionTest {

  private static final String USER = "restuser";
  private static final String PASSWORD = "restpassword";
  private static final String PAYLOAD = "{\"reached\":\"origin\"}";

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
    server.createContext(
        "/",
        exchange -> {
          List<String> auth = exchange.getRequestHeaders().get("Authorization");
          authorization.set(auth == null ? null : auth.get(0));
          requestPath.set(exchange.getRequestURI().getPath());

          byte[] body = PAYLOAD.getBytes(UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
    authorization.set(null);
    requestPath.set(null);
  }

  @Test
  void relativeUrlIsResolvedAgainstTheConnectionBaseUrl() throws Exception {
    File target = File.createTempFile("connection", ".tmp");
    target.deleteOnExit();

    ActionHttp action = action(connectionProvider(null, null), target);
    action.setUrl("v1/items");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals("/v1/items", requestPath.get());
    assertEquals(PAYLOAD, FileUtils.readFileToString(target, UTF_8));
  }

  @Test
  void connectionCredentialsAuthenticateTheRequest() throws Exception {
    File target = File.createTempFile("connectionauth", ".tmp");
    target.deleteOnExit();

    ActionHttp action = action(connectionProvider(USER, PASSWORD), target);
    action.setUrl("v1/items");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(
        "Basic " + Base64.getEncoder().encodeToString((USER + ":" + PASSWORD).getBytes(UTF_8)),
        authorization.get());
  }

  @Test
  void anAbsoluteUrlOnAnotherHostGetsNoConnectionCredentials() throws Exception {
    // The credentials belong to the connection's base URL host. An absolute URL naming another
    // host, typed in or taken from a result row, is still called, but without them.
    File target = File.createTempFile("otherhost", ".tmp");
    target.deleteOnExit();

    ActionHttp action = action(connectionProvider(USER, PASSWORD, OTHER_ORIGIN), target);
    action.setUrl("http://localhost:" + server.getAddress().getPort() + "/elsewhere");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals("/elsewhere", requestPath.get());
    assertNull(authorization.get(), "the connection credentials went to another host");
  }

  @Test
  void aMissingConnectionIsAnError() throws Exception {
    File target = File.createTempFile("noconnection", ".tmp");
    target.deleteOnExit();

    ActionHttp action = new ActionHttp();
    action.setParentWorkflow(new LocalWorkflowEngine());
    action.setMetadataProvider(new MemoryMetadataProvider());
    action.setAddFilenameToResult(false);
    action.setConnectionName("does-not-exist");
    action.setUrl("http://localhost:" + server.getAddress().getPort() + "/v1/items");
    action.setTargetFilename(target.getCanonicalPath());

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  private MemoryMetadataProvider connectionProvider(String user, String password) throws Exception {
    return connectionProvider(user, password, "http://localhost:" + server.getAddress().getPort());
  }

  private MemoryMetadataProvider connectionProvider(String user, String password, String baseUrl)
      throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    RestConnection connection = new RestConnection();
    connection.setName("test-connection");
    connection.setBaseUrl(baseUrl);
    if (user != null) {
      connection.setAuthType(RestConnection.BASIC);
      connection.setUsername(user);
      connection.setPassword(password);
    }
    provider.getSerializer(RestConnection.class).save(connection);
    return provider;
  }

  private ActionHttp action(MemoryMetadataProvider provider, File target) throws IOException {
    ActionHttp action = new ActionHttp();
    action.setParentWorkflow(new LocalWorkflowEngine());
    action.setMetadataProvider(provider);
    action.setAddFilenameToResult(false);
    action.setConnectionName("test-connection");
    action.setTargetFilename(target.getCanonicalPath());
    return action;
  }
}
