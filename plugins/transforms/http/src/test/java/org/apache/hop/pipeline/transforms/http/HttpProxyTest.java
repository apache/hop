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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The proxy and the target server each keep their own credentials.
 *
 * <p>Both used to be served by one wildcard {@link org.apache.hc.client5.http.auth.AuthScope}, so
 * the web server's user name and password were offered to whichever of the two asked first --
 * including a proxy returning 407 (issue #3440).
 */
class HttpProxyTest {

  private static final String PROXY_USER = "proxyuser";
  private static final String PROXY_PASSWORD = "proxypassword";
  private static final String SERVER_USER = "serveruser";
  private static final String SERVER_PASSWORD = "serverpassword";
  private static final String PROXIED_PAYLOAD = "through the proxy";
  private static final String ORIGIN_PAYLOAD = "straight from the origin";

  /** A host the test must never resolve: everything for it has to go through the proxy. */
  private static final String UNRESOLVABLE_TARGET = "http://target.invalid/data";

  private HttpServer proxy;
  private HttpServer origin;
  private final AtomicReference<String> proxyAuthorization = new AtomicReference<>();
  private final AtomicReference<String> serverAuthorization = new AtomicReference<>();

  @BeforeEach
  void startServers() throws IOException {
    proxy = HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    proxy.createContext(
        "/",
        exchange -> {
          List<String> proxyAuth = exchange.getRequestHeaders().get("Proxy-Authorization");
          List<String> serverAuth = exchange.getRequestHeaders().get("Authorization");
          proxyAuthorization.set(proxyAuth == null ? null : proxyAuth.get(0));
          serverAuthorization.set(serverAuth == null ? null : serverAuth.get(0));

          if (proxyAuth == null) {
            exchange.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=\"hop-test\"");
            exchange.sendResponseHeaders(407, -1);
            exchange.close();
            return;
          }
          respond(exchange, PROXIED_PAYLOAD);
        });
    proxy.start();

    origin = HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    origin.createContext("/", exchange -> respond(exchange, ORIGIN_PAYLOAD));
    origin.start();
  }

  private static void respond(com.sun.net.httpserver.HttpExchange exchange, String payload)
      throws IOException {
    byte[] body = payload.getBytes(UTF_8);
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(body);
    }
    exchange.close();
  }

  @AfterEach
  void stopServers() {
    proxy.stop(0);
    origin.stop(0);
    proxyAuthorization.set(null);
    serverAuthorization.set(null);
  }

  @Test
  void proxyCredentialsAnswerTheProxyChallenge() throws Exception {
    Http http =
        transform(
            UNRESOLVABLE_TARGET,
            data -> {
              data.realProxyUsername = PROXY_USER;
              data.realProxyPassword = PROXY_PASSWORD;
            });

    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals(PROXIED_PAYLOAD, result[0]);
    assertEquals(basic(PROXY_USER, PROXY_PASSWORD), proxyAuthorization.get());
  }

  @Test
  void serverCredentialsAreNeverOfferedToTheProxy() throws Exception {
    Http http =
        transform(
            UNRESOLVABLE_TARGET,
            data -> {
              data.realHttpLogin = SERVER_USER;
              data.realHttpPassword = SERVER_PASSWORD;
            });

    // The proxy challenges and no proxy credentials are configured, so the 407 stands: the web
    // server's credentials are not handed to the proxy to get past it.
    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals("", result[0]);
    assertNull(proxyAuthorization.get(), "the server's credentials were sent to the proxy");
  }

  @Test
  void bypassedHostIsReachedWithoutTheProxy() throws Exception {
    Http http =
        transform(
            "http://localhost:" + origin.getAddress().getPort() + "/data",
            data -> data.realNonProxyHosts = "localhost|127.*");

    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals(ORIGIN_PAYLOAD, result[0]);
    assertNull(proxyAuthorization.get(), "the request went through the proxy after all");
  }

  private Http transform(String url, java.util.function.Consumer<HttpData> configure) {
    HttpMeta meta = new HttpMeta();
    HttpData data = new HttpData();
    data.realUrl = url;
    data.argNrs = new int[0];
    data.realProxyHost = "localhost";
    data.realProxyPort = proxy.getAddress().getPort();
    configure.accept(data);

    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("HttpProxyTest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("HttpProxyTest");
    pipelineMeta.addTransform(transformMeta);
    return new Http(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
  }

  private static String basic(String user, String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(UTF_8));
  }
}
