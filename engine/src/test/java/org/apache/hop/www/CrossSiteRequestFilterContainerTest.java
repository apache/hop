/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.security.CrossSitePolicy;
import org.eclipse.jetty.ee11.servlet.FilterHolder;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Runs {@link CrossSiteRequestFilter} in a real servlet container, wired the way the Hop Web {@code
 * web.xml} wires it: mapped on {@code /hop/*} ahead of the filters that authenticate. The
 * mock-based test cannot see container header handling or filter ordering, which is where a
 * deployment actually goes wrong.
 */
class CrossSiteRequestFilterContainerTest {

  private Server server;
  private int port;

  /** Set when the request got past the cross-site filter, i.e. as far as auth and the servlet. */
  private final AtomicBoolean downstreamReached = new AtomicBoolean();

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  @Test
  void crossSiteRequestNeverReachesAuthenticationOrTheServlet() throws Exception {
    start(CrossSitePolicy.SAME_SITE);

    assertEquals(403, statusFor("cross-site"));
    assertFalse(
        downstreamReached.get(), "the request must be refused before authentication runs at all");
  }

  @Test
  void allowedRequestsReachTheServlet() throws Exception {
    start(CrossSitePolicy.SAME_SITE);

    for (String site : new String[] {null, "none", "same-origin", "same-site"}) {
      downstreamReached.set(false);
      assertEquals(200, statusFor(site), "Sec-Fetch-Site: " + site);
      assertTrue(downstreamReached.get(), "Sec-Fetch-Site: " + site);
    }
  }

  @Test
  void duplicatedHeaderIsRejected() throws Exception {
    start(CrossSitePolicy.SAME_SITE);

    // Two contradictory values, as a proxy on the way in might produce. There is no safe reading.
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/hop/status"))
            .timeout(Duration.ofSeconds(30))
            .header(CrossSiteRequestHandler.SEC_FETCH_SITE, "same-origin")
            .header(CrossSiteRequestHandler.SEC_FETCH_SITE, "cross-site")
            .GET()
            .build();
    try (HttpClient client = HttpClient.newHttpClient()) {
      assertEquals(403, client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
    }
    assertFalse(downstreamReached.get());
  }

  @Test
  void offPolicyLetsCrossSiteThrough() throws Exception {
    start(CrossSitePolicy.OFF);

    assertEquals(200, statusFor("cross-site"));
    assertTrue(downstreamReached.get());
  }

  private void start(CrossSitePolicy policy) throws Exception {
    port = freePort();
    server = new Server();
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(port);
    connector.setHost("localhost");
    server.setConnectors(new org.eclipse.jetty.server.Connector[] {connector});

    ServletContextHandler context = new ServletContextHandler("/", ServletContextHandler.SESSIONS);

    FilterHolder crossSite = new FilterHolder(CrossSiteRequestFilter.class);
    crossSite.setInitParameter(CrossSiteRequestFilter.INIT_PARAM_POLICY, policy.getCode());
    context.addFilter(crossSite, "/hop/*", EnumSet.of(DispatcherType.REQUEST));

    // Stands in for HopBasicAuthFilter / HopOidcAuthFilter, which web.xml maps after this one.
    context.addFilter(
        new FilterHolder(markerFilter()), "/hop/*", EnumSet.of(DispatcherType.REQUEST));

    context.addServlet(new ServletHolder(okServlet()), "/hop/*");
    server.setHandler(context);
    server.start();
  }

  private Filter markerFilter() {
    return new Filter() {
      @Override
      public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
          throws IOException, jakarta.servlet.ServletException {
        downstreamReached.set(true);
        chain.doFilter(request, response);
      }
    };
  }

  private static HttpServlet okServlet() {
    return new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setStatus(200);
        resp.getWriter().write("ok");
      }
    };
  }

  private int statusFor(String secFetchSite) throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/hop/status"))
            .timeout(Duration.ofSeconds(30))
            .GET();
    if (secFetchSite != null) {
      builder.header(CrossSiteRequestHandler.SEC_FETCH_SITE, secFetchSite);
    }
    try (HttpClient client = HttpClient.newHttpClient()) {
      return client.send(builder.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
