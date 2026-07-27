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
import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for issue #7621: a POST issued through a REST connection must send a {@code
 * Content-Length} header, just like the standalone URL path does.
 *
 * <p>The connection path uses Jersey's default (JDK {@code HttpURLConnection}) connector. That
 * connector omits {@code Content-Length} for a {@code null} request entity, but sends {@code
 * Content-Length: 0} for an empty (non-null) entity. {@code Rest.executeRequest} therefore
 * normalizes a null body to an empty string; this test locks in the connector behaviour that fix
 * relies on: an empty-body POST through a connection carries {@code Content-Length} and is not sent
 * with chunked transfer encoding.
 */
class RestConnectionContentLengthTest {

  private HttpServer server;
  private String url;

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    // Echo back the request's Content-Length and whether it arrived chunked, so the test can prove
    // the connection path sends Content-Length rather than using chunked transfer encoding.
    server.createContext(
        "/objects",
        exchange -> {
          // Drain the request body before responding.
          exchange.getRequestBody().readAllBytes();
          String contentLength = exchange.getRequestHeaders().getFirst("Content-Length");
          List<String> transferEncoding = exchange.getRequestHeaders().get("Transfer-Encoding");
          boolean chunked =
              transferEncoding != null
                  && transferEncoding.stream().anyMatch(v -> v.equalsIgnoreCase("chunked"));
          byte[] body =
              ("content-length=" + contentLength + ";chunked=" + chunked)
                  .getBytes(StandardCharsets.UTF_8);
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
  void emptyBodyPostThroughConnectionSendsContentLength() throws Exception {
    RestConnection connection = new RestConnection(new Variables());

    Invocation.Builder invocationBuilder = connection.getInvocationBuilder(url);
    try (Response response =
        invocationBuilder.post(Entity.entity("", MediaType.APPLICATION_JSON_TYPE))) {

      assertEquals(200, response.getStatus());
      assertEquals("content-length=0;chunked=false", response.readEntity(String.class));
    }
  }
}
