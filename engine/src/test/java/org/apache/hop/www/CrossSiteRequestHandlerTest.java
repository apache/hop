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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.CrossSitePolicy;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.server.HopServerMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Exercises the cross-site check against a running {@link WebServer} with the real servlets
 * registered, so it covers the wiring as well as the policy: the handler has to sit in front of
 * every context, and in front of authentication, whether or not authentication is enabled.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class CrossSiteRequestHandlerTest {

  private static final String HOST_NAME = "localhost";

  private static final int OK = 200;
  private static final int BAD_REQUEST = 400;
  private static final int UNAUTHORIZED = 401;
  private static final int FORBIDDEN = 403;

  /** A read-only servlet: answers 200 once the request gets past the check. */
  private static final String STATUS_PATH = GetStatusServlet.CONTEXT_PATH;

  /**
   * A state-changing servlet, the kind this whole check exists for. There is no such pipeline, so
   * reaching the servlet shows up as a 400 — which is exactly what proves the request got through.
   */
  private static final String STOP_PATH = StopPipelineServlet.CONTEXT_PATH + "?name=no-such";

  private WebServer webServer;
  private int port;

  @BeforeAll
  static void initHop() throws Exception {
    // Registers the servlet plugins that WebServer mounts.
    HopEnvironment.init();
  }

  @AfterEach
  void tearDown() {
    if (webServer != null) {
      webServer.stop();
      webServer = null;
    }
  }

  @Test
  void crossSiteRequestIsRejectedWithoutAuthentication() throws Exception {
    startServer(CrossSitePolicy.SAME_SITE, false);

    assertEquals(FORBIDDEN, statusFor(STATUS_PATH, "cross-site"));
    assertEquals(FORBIDDEN, statusFor(STOP_PATH, "cross-site"));
  }

  /** The check must not be reachable only through the authenticated branch of WebServer.start(). */
  @Test
  void crossSiteRequestIsRejectedBeforeAuthentication() throws Exception {
    startServer(CrossSitePolicy.SAME_SITE, true);

    // Without credentials an allowed request gets as far as the 401...
    assertEquals(UNAUTHORIZED, statusFor(STATUS_PATH, "same-origin"));
    // ...so a 403 for the cross-site one shows the check ran ahead of authentication rather than
    // riding on it.
    assertEquals(FORBIDDEN, statusFor(STATUS_PATH, "cross-site"));
    assertEquals(FORBIDDEN, statusFor(STOP_PATH, "cross-site"));
  }

  @Test
  void sameSiteRequestIsAllowedByDefaultButNotUnderSameOrigin() throws Exception {
    startServer(CrossSitePolicy.SAME_SITE, false);
    assertEquals(OK, statusFor(STATUS_PATH, "same-site"));
    tearDown();

    startServer(CrossSitePolicy.SAME_ORIGIN, false);
    assertEquals(FORBIDDEN, statusFor(STATUS_PATH, "same-site"));
  }

  @Test
  void ownPagesAndNonBrowserClientsAreUntouched() throws Exception {
    startServer(CrossSitePolicy.SAME_ORIGIN, false);

    // A typed address or a bookmark.
    assertEquals(OK, statusFor(STATUS_PATH, "none"));
    // A link or a form on the server's own status page.
    assertEquals(OK, statusFor(STATUS_PATH, "same-origin"));
    // hop-run, the Hop GUI, curl, automation: no Sec-Fetch-* headers at all.
    assertEquals(OK, statusFor(STATUS_PATH, null));
    assertEquals(OK, statusFor(GetRootServlet.CONTEXT_PATH, null));
    // Including on the state-changing servlets, which have to keep working.
    assertEquals(BAD_REQUEST, statusFor(STOP_PATH, null));
    assertEquals(BAD_REQUEST, statusFor(STOP_PATH, "same-origin"));
  }

  /** A client that cannot send the header correctly can always send nothing at all. */
  @Test
  void unrecognisedHeaderValueIsRejected() throws Exception {
    startServer(CrossSitePolicy.SAME_SITE, false);

    assertEquals(FORBIDDEN, statusFor(STATUS_PATH, "nonsense"));
  }

  @Test
  void nothingIsRejectedWhenTheCheckIsOff() throws Exception {
    startServer(CrossSitePolicy.OFF, false);

    assertEquals(OK, statusFor(STATUS_PATH, "cross-site"));
    assertEquals(BAD_REQUEST, statusFor(STOP_PATH, "cross-site"));
  }

  private void startServer(CrossSitePolicy policy, boolean enableAuth) throws Exception {
    HopServerMeta hopServer =
        new HopServerMeta("test", HOST_NAME, "0", "cluster", "cluster-password");
    hopServer.setEnableAuth(enableAuth);

    HopServerConfig serverConfig = new HopServerConfig(hopServer);

    PipelineMap pipelineMap = new PipelineMap();
    pipelineMap.setHopServerConfig(serverConfig);
    WorkflowMap workflowMap = new WorkflowMap();
    workflowMap.setHopServerConfig(serverConfig);

    port = freePort();
    webServer =
        new WebServer(
            new LogChannel("cross-site-test"), pipelineMap, workflowMap, HOST_NAME, port, null);
    webServer.setCrossSitePolicy(policy);
    webServer.start();
  }

  /**
   * Issue a GET against the running server.
   *
   * @param path the path to request
   * @param secFetchSite the Sec-Fetch-Site value to send, or null to send no such header
   * @return the HTTP status code
   */
  private int statusFor(String path, String secFetchSite) throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create("http://" + HOST_NAME + ":" + port + path))
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
