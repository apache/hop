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

package org.apache.hop.marketplace.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.marketplace.resolve.MavenRepositoryClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End to end over real HTTP: what a download triggered by an install spec actually puts on the
 * wire. The unit tests assert which credentials are selected; these assert that the selection is
 * what reaches the server, which is the property issue #8393 is about.
 *
 * <p>Two loopback servers on different ports are two different origins, so "the operator's
 * repository" and "a repository the project names" can be told apart without DNS.
 */
class InstallSpecRepositoryCredentialScopeHttpTest {

  private static final MavenCoordinates COORDS =
      new MavenCoordinates("org.apache.hop", "hop-tech-parquet", "2.19.0");

  @TempDir private Path tempDir;

  private HttpServer configured;
  private HttpServer foreign;

  /** Authorization headers seen by each server, in order. */
  private final List<String> configuredAuth = new ArrayList<>();

  private final List<String> foreignAuth = new ArrayList<>();

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @BeforeEach
  void startServers() throws IOException {
    configured = start(configuredAuth);
    foreign = start(foreignAuth);
  }

  @AfterEach
  void stopServers() {
    configured.stop(0);
    foreign.stop(0);
  }

  private static HttpServer start(List<String> authorizations) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        (HttpExchange exchange) -> {
          authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
          byte[] body = "zip".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    return server;
  }

  private static String url(HttpServer server, String path) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + path;
  }

  /** hop-config with one private repository on the {@link #configured} server. */
  private MarketplaceConfig operatorConfig() {
    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().clear();
    MarketplaceRepository repo =
        new MarketplaceRepository(
            "nexus", url(configured, "/repository/hop/"), "operator", "operator-secret");
    repo.setPrimary(true);
    config.getRepositories().add(repo);
    return config;
  }

  private HopInstallSpec specPointingAt(String repositoryUrl) {
    HopInstallSpec spec = new HopInstallSpec();
    HopInstallSpec.RepositoryRef ref = new HopInstallSpec.RepositoryRef();
    ref.setId("project");
    ref.setUrl(repositoryUrl);
    spec.getRepositories().add(ref);
    return spec;
  }

  private void downloadUsing(HopInstallSpec spec) throws Exception {
    MarketplaceConfig applied =
        new EnvironmentApplier(new LogChannel("test"), tempDir, operatorConfig())
            .configFromEnv(spec);
    Path target = Files.createTempDirectory(tempDir, "dl").resolve("plugin.zip");
    new MavenRepositoryClient(new LogChannel("test"))
        .downloadZip(applied.primaryRepository(), COORDS, target);
  }

  @Test
  void aSpecPointingAtAnotherHostGetsNoAuthorizationHeader() throws Exception {
    downloadUsing(specPointingAt(url(foreign, "/repository/hop/")));

    assertEquals(1, foreignAuth.size(), "the foreign server must have served the download");
    assertNull(
        foreignAuth.get(0), "operator credentials must not reach a repository it never configured");
    assertTrue(configuredAuth.isEmpty(), "the configured repository was not contacted at all");
  }

  @Test
  void aSpecPointingAtTheConfiguredRepositoryStillAuthenticates() throws Exception {
    downloadUsing(specPointingAt(url(configured, "/repository/hop-extra/")));

    assertEquals(1, configuredAuth.size());
    assertEquals(
        "Basic "
            + Base64.getEncoder()
                .encodeToString("operator:operator-secret".getBytes(StandardCharsets.UTF_8)),
        configuredAuth.get(0),
        "a second path on the operator's own repository keeps working");
  }

  @Test
  void aSpecMayCarryItsOwnCredentialsForAForeignHost() throws Exception {
    HopInstallSpec spec = specPointingAt(url(foreign, "/repository/hop/"));
    spec.getRepositories().get(0).setUsername("project-user");
    spec.getRepositories().get(0).setPassword("project-secret");

    downloadUsing(spec);

    assertEquals(
        "Basic "
            + Base64.getEncoder()
                .encodeToString("project-user:project-secret".getBytes(StandardCharsets.UTF_8)),
        foreignAuth.get(0));
  }
}
