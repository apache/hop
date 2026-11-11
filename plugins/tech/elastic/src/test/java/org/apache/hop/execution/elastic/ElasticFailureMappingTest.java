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

package org.apache.hop.execution.elastic;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The failure mapping that is added to an existing index before the first write. */
class ElasticFailureMappingTest {

  private HttpServer server;
  private final List<String> requests = new CopyOnWriteArrayList<>();
  private final AtomicInteger status = new AtomicInteger(200);

  @BeforeAll
  static void initLogging() throws HopException {
    // A refusal is logged
    HopClientEnvironment.init();
  }

  @BeforeEach
  void startServer() throws IOException {
    // 127.0.0.1 on both ends: localhost can resolve to another address family than the server's
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI() + " " + body);
          byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status.get(), response.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(response);
          }
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  /** A location for an index of its own: what was dealt with is remembered per index. */
  private ElasticExecutionInfoLocation location() {
    ElasticExecutionInfoLocation location = new ElasticExecutionInfoLocation();
    location.actualUrl =
        "http://"
            + server.getAddress().getAddress().getHostAddress()
            + ":"
            + server.getAddress().getPort()
            + "/";
    location.actualApiKey = "key";
    location.actualIndexName = "hop-" + UUID.randomUUID();
    return location;
  }

  @Test
  void theMappingIsAddedOnceAnIndex() {
    ElasticExecutionInfoLocation location = location();
    location.ensureFailureMapping();
    location.ensureFailureMapping();

    assertEquals(1, requests.size());
    String request = requests.get(0);
    assertTrue(request.startsWith("PUT /" + location.actualIndexName + "/_mapping "), request);
    assertTrue(request.contains("\"failure\""), request);
    assertTrue(request.contains("\"enabled\": false"), request);
  }

  /** The first write creates the index; the next one adds the mapping. */
  @Test
  void aMissingIndexIsTriedAgain() {
    ElasticExecutionInfoLocation location = location();
    status.set(404);
    location.ensureFailureMapping();
    status.set(200);
    location.ensureFailureMapping();
    location.ensureFailureMapping();

    assertEquals(2, requests.size());
  }

  /** An API key without the right to change the mapping, or a failure mapped before. */
  @Test
  void aRefusalIsNotTriedAgain() {
    ElasticExecutionInfoLocation location = location();
    status.set(403);
    location.ensureFailureMapping();
    location.ensureFailureMapping();

    assertEquals(1, requests.size());
  }

  @Test
  void anUnreachableServerDoesNotStopTheWrite() {
    ElasticExecutionInfoLocation location = location();
    server.stop(0);
    assertDoesNotThrow(location::ensureFailureMapping);
  }
}
