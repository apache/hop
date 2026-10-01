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
 * API-level integration test for the RBAC guard on the embedded Hop Server API ({@code /hop/*}) in
 * Hop Web. Drives the endpoints directly over HTTP (no UI / Selenium): the authorization decision
 * is observable purely from the status code — {@code 403} means the request was refused before the
 * servlet ran, any other code means it passed authorization.
 *
 * <p>Starts its own Hop Web container in mode {@code BASIC} with the demo users seeded
 * (admin/developer/operator/viewer, password = username).
 *
 * <p>For manual, curl-based exploration of the same endpoints (including from a browser) see the
 * scripts under {@code web-tests/api-checks}.
 *
 * <p><b>Opt-in.</b> The image under test must contain the RBAC filter (issue #8150). To avoid
 * turning the daily job red while the published image predates the fix, this test only runs when
 * {@code -Dhopweb.rbac.it=true} is set. Point it at a fixed image with {@code
 * -Dhopweb.image=<image>} (default {@code hop-web:local}). Once the fix ships in the published
 * image, the guard can be removed.
 */
@EnabledIfSystemProperty(named = "hopweb.rbac.it", matches = "(?i)true|1|yes")
class HopServerApiRbacTest {

  private static final int HOP_WEB_PORT = 8080;
  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  private static GenericContainer<?> container;
  private static String baseUrl;

  @BeforeAll
  static void startContainer() {
    String image = System.getProperty("hopweb.image", "hop-web:local");
    container =
        new GenericContainer<>(DockerImageName.parse(image))
            .withExposedPorts(HOP_WEB_PORT)
            .withEnv("HOP_WEB_SECURITY_MODE", "BASIC")
            .withEnv("HOP_WEB_SEED_DEMO_USERS", "true")
            .withImagePullPolicy(
                image.endsWith(":local") ? PullPolicy.defaultPolicy() : PullPolicy.alwaysPull())
            .waitingFor(Wait.forHttp("/login").forStatusCode(200))
            .withStartupTimeout(Duration.ofSeconds(120));
    container.start();
    baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(HOP_WEB_PORT);
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  // --- helpers -------------------------------------------------------------

  private int get(String path, String user) throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(TIMEOUT).GET();
    return send(b, user);
  }

  private int send(HttpRequest.Builder builder, String user) throws Exception {
    if (user != null) {
      String creds =
          Base64.getEncoder().encodeToString((user + ":" + user).getBytes(StandardCharsets.UTF_8));
      builder.header("Authorization", "Basic " + creds);
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

  // --- tests ---------------------------------------------------------------

  @Test
  void readOnlyMayReadStatus() throws Exception {
    // FILE_VIEW is granted to READ_ONLY.
    assertEquals(200, get("/hop/status/?xml=Y", "viewer"));
  }

  @Test
  void readOnlyMayNotDeployOrRunOrRemove() throws Exception {
    assertEquals(403, get("/hop/addWorkflow/?xml=Y", "viewer"), "addWorkflow");
    assertEquals(403, get("/hop/addPipeline/?xml=Y", "viewer"), "addPipeline");
    assertEquals(403, get("/hop/startPipeline/?xml=Y", "viewer"), "startPipeline");
    assertEquals(403, get("/hop/execWorkflow/?xml=Y", "viewer"), "execWorkflow");
    assertEquals(403, get("/hop/removePipeline/?xml=Y", "viewer"), "removePipeline");
  }

  @Test
  void operatorMayRunButNotDeploy() throws Exception {
    // Operator has RUN_EXECUTE but not FILE_SAVE.
    assertNotEquals(403, get("/hop/startPipeline/?xml=Y", "operator"), "startPipeline (run)");
    assertEquals(403, get("/hop/addPipeline/?xml=Y", "operator"), "addPipeline (deploy)");
  }

  @Test
  void developerMayDeploy() throws Exception {
    // User role has FILE_SAVE; authz passes (any non-403 code means it got past the filter).
    assertNotEquals(403, get("/hop/addPipeline/?xml=Y", "developer"));
  }

  @Test
  void adminPassesEverywhere() throws Exception {
    assertNotEquals(403, get("/hop/addWorkflow/?xml=Y", "admin"), "addWorkflow");
    assertNotEquals(403, get("/hop/execWorkflow/?xml=Y", "admin"), "execWorkflow");
    assertNotEquals(403, get("/hop/removeWorkflow/?xml=Y", "admin"), "removeWorkflow");
  }

  @Test
  void unknownEndpointDeniedEvenForAdmin() throws Exception {
    assertEquals(403, get("/hop/somethingBrandNew/?xml=Y", "admin"));
  }

  @Test
  void unauthenticatedIsRefused() throws Exception {
    // No credentials: the auth filter redirects browser navigations to /login (3xx) or
    // challenges API clients (401). Either way it is not a successful 2xx.
    int code = get("/hop/status/?xml=Y", null);
    assertNotEquals(200, code, "anonymous must not reach the server API");
  }
}
