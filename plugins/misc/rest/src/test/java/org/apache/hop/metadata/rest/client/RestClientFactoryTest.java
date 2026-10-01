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

package org.apache.hop.metadata.rest.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Covers the single client factory shared by the REST connection and the REST transform. */
class RestClientFactoryTest {

  private HttpServer server;
  private String echoMethodUrl;

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    // Echoes the HTTP method it saw, so a custom verb can be proven to reach the wire.
    server.createContext(
        "/echo-method",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] body = ("method=" + exchange.getRequestMethod()).getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    echoMethodUrl = "http://localhost:" + server.getAddress().getPort() + "/echo-method";
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void customMethodTokensReachTheWire() throws Exception {
    // HttpClient5 writes the method token straight into the request line, so an arbitrary verb
    // (issue #4770) needs nothing special — no reflection into HttpURLConnection, and so no
    // --add-opens java.base/java.net.
    try (CloseableHttpClient client = RestClientFactory.createClient(new RestClientSettings())) {
      HttpUriRequestBase request = new HttpUriRequestBase("LIST", URI.create(echoMethodUrl));
      request.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));
      assertEquals("method=LIST", client.execute(request, RestClientFactoryTest::body));
    }
  }

  @Test
  void anUnderscoreHostIsAddressable() throws Exception {
    // java.net.URI cannot parse "mtls_test" as a server authority, but HttpClient5 falls back to
    // its own lenient URIAuthority parser, so the host survives onto the wire. This is what Jersey
    // could not do.
    HttpUriRequestBase request =
        new HttpUriRequestBase("GET", URI.create("http://mtls_test:443/api/whoami"));

    assertNull(URI.create("http://mtls_test:443/api/whoami").getHost());
    assertEquals("mtls_test", request.getAuthority().getHostName());
  }

  @Test
  void oneClientServesManyRequests() throws Exception {
    // A client is bound to a configuration, not to a URL: the REST transform reuses one across
    // rows even when the endpoint comes from an input field.
    try (CloseableHttpClient client = RestClientFactory.createClient(new RestClientSettings())) {
      for (int i = 0; i < 3; i++) {
        HttpUriRequestBase request =
            new HttpUriRequestBase("GET", URI.create(echoMethodUrl + "?row=" + i));
        assertEquals("method=GET", client.execute(request, RestClientFactoryTest::body));
      }
    }
  }

  private static String body(ClassicHttpResponse response) throws IOException, ParseException {
    assertEquals(200, response.getCode());
    return EntityUtils.toString(response.getEntity());
  }
}
