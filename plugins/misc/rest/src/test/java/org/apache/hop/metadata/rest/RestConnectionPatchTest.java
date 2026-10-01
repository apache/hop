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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.rest.client.RestClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for issue #7558: a PATCH request issued through a REST connection must actually
 * reach the server. It used to fail with a {@code ProcessingException} even though the URL was
 * assembled correctly, because Jersey's default JDK {@code HttpURLConnection} connector rejects
 * PATCH. The connection now runs on the Apache 5 connector, which writes the method token straight
 * into the request line.
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

    assertEquals("method=PATCH", execute(connection, "PATCH", url, "{\"name\":\"updated\"}"));
  }

  /** Issues one request on a client built from the connection, returning the body. */
  private static String execute(RestConnection connection, String method, String url, String body)
      throws Exception {
    try (CloseableHttpClient client =
        RestClientFactory.createClient(connection.createClientSettings())) {
      HttpUriRequestBase request = new HttpUriRequestBase(method, URI.create(url));
      if (body != null) {
        request.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
      }
      return client.execute(
          request,
          response -> {
            assertEquals(200, response.getCode());
            return EntityUtils.toString(response.getEntity());
          });
    }
  }
}
