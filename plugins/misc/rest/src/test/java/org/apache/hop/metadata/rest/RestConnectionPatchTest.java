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

import com.sun.net.httpserver.HttpServer;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for issue #7558: a PATCH request issued through a REST connection must actually
 * reach the server. The invocation builder produced by {@link RestConnection} uses Jersey's default
 * (JDK {@code HttpURLConnection}) connector, which rejects PATCH unless {@link
 * org.glassfish.jersey.client.HttpUrlConnectorProvider#SET_METHOD_WORKAROUND} is enabled. Before
 * the fix the PATCH failed with a {@code ProcessingException} even though the URL was assembled
 * correctly.
 */
class RestConnectionPatchTest {

  private HttpServer server;
  private String url;

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    // Echo the HTTP method back so the test can prove PATCH actually reached the server.
    server.createContext(
        "/objects",
        exchange -> {
          byte[] body = ("method=" + exchange.getRequestMethod()).getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    url = "http://localhost:" + server.getAddress().getPort() + "/objects";
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void patchThroughConnectionReachesServer() throws Exception {
    RestConnection connection = new RestConnection(new Variables());

    Invocation.Builder invocationBuilder = connection.getInvocationBuilder(url);
    try (Response response =
        invocationBuilder.method(
            "PATCH", Entity.entity("{\"name\":\"updated\"}", MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(200, response.getStatus());
      assertEquals("method=PATCH", response.readEntity(String.class));
    }
  }
}
