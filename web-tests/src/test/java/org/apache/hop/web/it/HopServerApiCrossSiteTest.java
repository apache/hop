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

package org.apache.hop.web.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

/**
 * API-level integration test for the cross-site guard on the embedded Hop Server API ({@code
 * /hop/*}) in Hop Web (issue #8371).
 *
 * <p>Hop Web co-deploys the Hop Server servlets outside the standalone hop-server, so the guard
 * there is a {@code web.xml} filter rather than the Jetty handler. This test drives the deployed
 * war over HTTP, which is the only way to prove the filter is actually mapped and ordered ahead of
 * authentication rather than merely present on the classpath.
 *
 * <p>The state-changing endpoints answer on {@code GET}, so the attack this defends against is a
 * page the operator is visiting issuing the request in their browser, with the session cookie
 * attached automatically. {@code Sec-Fetch-Site: cross-site} is what such a request carries.
 *
 * <p><b>Opt-in</b>, like {@link HopServerApiRbacTest}: the image under test must contain the
 * filter, so this only runs with {@code -Dhopweb.crosssite.it=true}. Point it at a fixed image with
 * {@code -Dhopweb.image=<image>} (default {@code hop-web:local}).
 */
@EnabledIfSystemProperty(named = "hopweb.crosssite.it", matches = "(?i)true|1|yes")
class HopServerApiCrossSiteTest {

  private static final int HOP_WEB_PORT = 8080;
  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  private static final String SEC_FETCH_SITE = "Sec-Fetch-Site";

  /** Read-only endpoint: answers 200 for an authorized caller that gets past the filter. */
  private static final String STATUS = "/hop/status/?xml=Y";

  /** State-changing endpoint, the kind the guard exists for. */
  private static final String EXEC = "/hop/execPipeline/?xml=Y";

  /** Default policy (same-site). */
  private static GenericContainer<?> guarded;

  /**
   * The documented escape hatch, so we know an operator can actually get the old behaviour back.
   */
  private static GenericContainer<?> unguarded;

  private static String guardedUrl;
  private static String unguardedUrl;

  @BeforeAll
  static void startContainers() {
    guarded = hopWeb(null);
    unguarded = hopWeb("off");
    guarded.start();
    unguarded.start();
    guardedUrl = baseUrl(guarded);
    unguardedUrl = baseUrl(unguarded);
  }

  @AfterAll
  static void stopContainers() {
    if (guarded != null) {
      guarded.stop();
    }
    if (unguarded != null) {
      unguarded.stop();
    }
  }

  private static GenericContainer<?> hopWeb(String crossSitePolicy) {
    String image = System.getProperty("hopweb.image", "hop-web:local");
    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse(image))
            .withExposedPorts(HOP_WEB_PORT)
            .withEnv("HOP_WEB_SECURITY_MODE", "BASIC")
            .withEnv("HOP_WEB_SEED_DEMO_USERS", "true")
            .withImagePullPolicy(
                image.endsWith(":local") ? PullPolicy.defaultPolicy() : PullPolicy.alwaysPull())
            .waitingFor(Wait.forHttp("/login").forStatusCode(200))
            .withStartupTimeout(Duration.ofSeconds(120));
    if (crossSitePolicy != null) {
      // The same environment variable the standalone hop-server uses.
      container = container.withEnv("HOP_SERVER_CROSS_SITE_POLICY", crossSitePolicy);
    }
    return container;
  }

  private static String baseUrl(GenericContainer<?> container) {
    return "http://" + container.getHost() + ":" + container.getMappedPort(HOP_WEB_PORT);
  }

  // --- tests ---------------------------------------------------------------

  /**
   * The admin has every permission, so a 403 here can only come from the cross-site filter — which
   * also shows it runs ahead of authentication and RBAC rather than depending on them.
   */
  @Test
  void crossSiteRequestIsRejectedEvenForAnAdmin() throws Exception {
    assertEquals(403, get(guardedUrl, STATUS, "admin", "cross-site"));
    assertEquals(403, get(guardedUrl, EXEC, "admin", "cross-site"));
  }

  @Test
  void crossSiteRequestIsRejectedWithoutCredentialsToo() throws Exception {
    assertEquals(403, get(guardedUrl, EXEC, null, "cross-site"));
  }

  /** hop-run, the Hop GUI, curl and customer automation send no Sec-Fetch-* headers at all. */
  @Test
  void nonBrowserClientsAreUnaffected() throws Exception {
    assertEquals(200, get(guardedUrl, STATUS, "admin", null));
    assertNotEquals(403, get(guardedUrl, EXEC, "admin", null));
  }

  /** A bookmark, Hop Web's own pages, and a sibling host on the same domain. */
  @Test
  void browserRequestsThatAreNotCrossSiteStillWork() throws Exception {
    assertEquals(200, get(guardedUrl, STATUS, "admin", "none"));
    assertEquals(200, get(guardedUrl, STATUS, "admin", "same-origin"));
    assertEquals(200, get(guardedUrl, STATUS, "admin", "same-site"));
  }

  /** A client that cannot send the header correctly can always send nothing at all. */
  @Test
  void unrecognisedHeaderValueIsRejected() throws Exception {
    assertEquals(403, get(guardedUrl, STATUS, "admin", "nonsense"));
  }

  /**
   * HOP_SERVER_CROSS_SITE_POLICY=off restores the previous behaviour, in Hop Web exactly as on the
   * standalone hop-server.
   */
  @Test
  void offEnvironmentVariableRestoresTheOldBehaviour() throws Exception {
    assertEquals(200, get(unguardedUrl, STATUS, "admin", "cross-site"));
    assertNotEquals(403, get(unguardedUrl, EXEC, "admin", "cross-site"));
  }

  // --- helper --------------------------------------------------------------

  private int get(String baseUrl, String path, String user, String secFetchSite) throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(TIMEOUT).GET();
    if (user != null) {
      String creds =
          Base64.getEncoder().encodeToString((user + ":" + user).getBytes(StandardCharsets.UTF_8));
      builder.header("Authorization", "Basic " + creds);
    }
    if (secFetchSite != null) {
      builder.header(SEC_FETCH_SITE, secFetchSite);
    }
    // Do not follow the login redirect: a 3xx to /login is itself the "unauthenticated" signal.
    HttpResponse<String> response =
        HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build()
            .send(builder.build(), HttpResponse.BodyHandlers.ofString());
    return response.statusCode();
  }
}
