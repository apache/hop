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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpServer;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for issue #7621: the connect/read timeouts configured on the REST transform must
 * be honoured when the request goes through a REST connection. Before the fix {@link
 * RestConnection#getInvocationBuilder} never set {@code CONNECT_TIMEOUT}/{@code READ_TIMEOUT}, so a
 * slow or hung server would block the pipeline indefinitely on the connection path.
 */
class RestConnectionTimeoutTest {

  private HttpServer server;
  private String url;
  private String fastUrl;

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    // Withhold the response for far longer than the client read timeout below.
    server.createContext(
        "/slow",
        exchange -> {
          try {
            Thread.sleep(1500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
          try {
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(body);
            }
          } catch (Exception ignore) {
            // The client has already timed out and disconnected.
          } finally {
            exchange.close();
          }
        });
    // Immediate response, used to prove a request still succeeds with unset (negative) timeouts.
    server.createContext(
        "/fast",
        exchange -> {
          byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    url = "http://localhost:" + server.getAddress().getPort() + "/slow";
    fastUrl = "http://localhost:" + server.getAddress().getPort() + "/fast";
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void readTimeoutThroughConnectionIsHonoured() throws Exception {
    RestConnection connection = new RestConnection(new Variables());

    // connectTimeout 2000 ms, readTimeout 300 ms — the server withholds the response for 1500 ms,
    // so the read must time out (a ProcessingException wrapping a SocketTimeoutException).
    Invocation.Builder invocationBuilder =
        connection.getInvocationBuilder(url, null, 8080, 2000, 300);

    assertThrows(ProcessingException.class, () -> invocationBuilder.get(String.class));
  }

  @Test
  void negativeTimeoutsAreIgnored() throws Exception {
    RestConnection connection = new RestConnection(new Variables());

    // Empty transform timeout fields resolve to -1; the connection must not pass a negative timeout
    // to Jersey (which throws "timeouts can't be negative"). The request should simply succeed.
    Invocation.Builder invocationBuilder =
        connection.getInvocationBuilder(fastUrl, null, 8080, -1, -1);

    try (Response response = invocationBuilder.get()) {
      assertEquals(200, response.getStatus());
    }
  }
}
