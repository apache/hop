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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
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
 * Regression test for issue #7621: the connect/read timeouts configured on the REST transform must
 * be honoured when the request goes through a REST connection. Before the fix the connection built
 * its own client and never set the connect or read timeout, so a slow or hung server would block
 * the pipeline indefinitely on the connection path.
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
    // so the read must time out.
    connection.setConnectTimeout("2000");
    connection.setReadTimeout("300");

    assertThrows(SocketTimeoutException.class, () -> execute(connection, "GET", url, null));
  }

  @Test
  void negativeTimeoutsAreIgnored() throws Exception {
    RestConnection connection = new RestConnection(new Variables());

    // Empty timeout fields resolve to -1; the connection must leave the timeout unset rather than
    // pass a negative one through. The request should simply succeed.
    assertEquals("ok", execute(connection, "GET", fastUrl, null));
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
