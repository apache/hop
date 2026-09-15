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

package org.apache.hop.marketplace.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.marketplace.resolve.MavenRepositoryClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * The Artifactory browser against a real JFrog Artifactory OSS server.
 *
 * <p>{@link JfrogRepositoryBrowserHttpTest} covers the same wiring against a stand-in server and
 * runs on every build. This one exists for the part a stand-in cannot check: that the AQL query Hop
 * sends is accepted and answered as expected by Artifactory itself, that the storage walk matches
 * the real folder layout, and that a bearer token issued by Artifactory authenticates.
 *
 * <p>The image is around 1.5 GB and takes minutes to become ready, so this is opt-in — an ordinary
 * {@code mvn install} skips it:
 *
 * <pre>
 * ./mvnw -pl plugins/misc/marketplace verify -Dmarketplace.it=true
 * </pre>
 */
@EnabledIfSystemProperty(
    named = "marketplace.it",
    matches = "true",
    disabledReason = "Artifactory container is opt-in; run with -Dmarketplace.it=true")
class JfrogArtifactoryBrowseIT {

  private static final DockerImageName IMAGE =
      DockerImageName.parse("releases-docker.jfrog.io/jfrog/artifactory-oss:7.98.7");

  /**
   * Artifactory's services wait for these before they start, and supplying either one turns off the
   * entrypoint's own key generation, so both have to be given. Without them the container never
   * becomes ready and the only symptom is a startup timeout. Throwaway keys for a disposable
   * container.
   */
  private static final String MASTER_KEY =
      "1d0f4a5e8b2c7d3f6a9e0b1c4d7f2a5e8b3c6d9f0a1e4b7c2d5f8a3e6b9c0d1f";

  private static final String JOIN_KEY =
      "9f2c5a8e1b4d7f0a3c6e9b2d5f8a1c4e7b0d3f6a9c2e5b8d1f4a7c0e3b6d9f2c";

  /**
   * Artifactory 7.98 dropped the bundled Derby database and refuses to start on anything but
   * PostgreSQL, so the database is part of the fixture rather than something the image provides.
   */
  private static final String DB_HOST = "postgres";

  private static final String DB_NAME = "artifactory";
  private static final String DB_USER = "artifactory";
  private static final String DB_PASSWORD = "artifactory";

  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "password";

  /**
   * Creating a repository over REST is an Artifactory Pro feature, so the OSS image cannot be given
   * a purpose-made one. It ships this generic local repository, which serves fine: the browser
   * matches paths and {@code .zip} names and never asks what package type a repository declares.
   */
  private static final String REPO_KEY = "example-repo-local";

  private static final String GROUP = "com.acme.hop";

  private static Network network;
  private static GenericContainer<?> postgres;
  private static GenericContainer<?> artifactory;
  private static HttpClient client;
  private static String baseUrl;
  private static String accessToken;

  @BeforeAll
  static void startArtifactory() {
    HopLogStore.init();
    network = Network.newNetwork();
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withNetwork(network)
            .withNetworkAliases(DB_HOST)
            .withEnv("POSTGRES_DB", DB_NAME)
            .withEnv("POSTGRES_USER", DB_USER)
            .withEnv("POSTGRES_PASSWORD", DB_PASSWORD)
            .waitingFor(
                Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    postgres.start();

    artifactory =
        new GenericContainer<>(IMAGE)
            .withNetwork(network)
            // 8082 is the platform router; 8081 is the legacy direct port.
            .withExposedPorts(8082)
            .withEnv("JF_SHARED_DATABASE_TYPE", "postgresql")
            .withEnv("JF_SHARED_DATABASE_DRIVER", "org.postgresql.Driver")
            .withEnv("JF_SHARED_DATABASE_URL", "jdbc:postgresql://" + DB_HOST + ":5432/" + DB_NAME)
            .withEnv("JF_SHARED_DATABASE_USERNAME", DB_USER)
            .withEnv("JF_SHARED_DATABASE_PASSWORD", DB_PASSWORD)
            .withEnv("JF_SHARED_SECURITY_MASTERKEY", MASTER_KEY)
            .withEnv("JF_SHARED_SECURITY_JOINKEY", JOIN_KEY)
            .waitingFor(
                Wait.forHttp("/artifactory/api/system/ping")
                    .forPort(8082)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(10)));

    // Deliberately not abort(): this class only runs when someone asked for it with
    // -Dmarketplace.it=true, and an aborted @BeforeAll reports as "0 tests, build success", which
    // is indistinguishable from a passing run. A container that will not start is a failure.
    artifactory.start();

    client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    baseUrl =
        "http://" + artifactory.getHost() + ":" + artifactory.getMappedPort(8082) + "/artifactory/";

    try {
      assertRepositoryExists();
      publishPlugins();
      accessToken = createAccessToken();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to prepare Artifactory fixtures", e);
    }
  }

  @AfterAll
  static void stopArtifactory() {
    if (artifactory != null) {
      artifactory.stop();
    }
    if (postgres != null) {
      postgres.stop();
    }
    if (network != null) {
      network.close();
    }
  }

  // ------------------------------------------------------------- fixtures

  /** Fail early and clearly if the image ever stops shipping the repository the fixtures use. */
  private static void assertRepositoryExists() throws Exception {
    String repositories = send(request("api/repositories"), 200).body();
    assertTrue(
        repositories.contains("\"" + REPO_KEY + "\""),
        "Artifactory does not have " + REPO_KEY + ": " + repositories);
  }

  /**
   * Two versions of one plugin, a second plugin, a SNAPSHOT, and a jar-only artifact that is a
   * shared library rather than an installable plugin.
   */
  private static void publishPlugins() throws Exception {
    deployZip("acme-parser", "2026.06");
    deployZip("acme-parser", "2026.09");
    deployZip("acme-writer", "1.0.0");
    deployZip("acme-snapshot", "1.0.0-SNAPSHOT");
    deploy(
        mavenPath("acme-lib", "1.0.0") + "/acme-lib-1.0.0.jar",
        "not-a-plugin".getBytes(StandardCharsets.UTF_8));
  }

  private static void deployZip(String artifactId, String version) throws Exception {
    deploy(
        mavenPath(artifactId, version) + "/" + artifactId + "-" + version + ".zip",
        pluginZip(artifactId));
  }

  private static String mavenPath(String artifactId, String version) {
    return GROUP.replace('.', '/') + "/" + artifactId + "/" + version;
  }

  private static void deploy(String path, byte[] body) throws Exception {
    send(
        request(REPO_KEY + "/" + path)
            .header("Content-Type", "application/octet-stream")
            .PUT(HttpRequest.BodyPublishers.ofByteArray(body)),
        200,
        201);
  }

  /**
   * A zip shaped like a real Hop plugin. {@link PluginInstaller} unpacks entries relative to the
   * Hop home, so the paths inside the archive are where the plugin ends up.
   */
  private static byte[] pluginZip(String artifactId) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ZipOutputStream zip = new ZipOutputStream(out)) {
      zip.putNextEntry(new ZipEntry(pluginDir(artifactId) + "/"));
      zip.closeEntry();
      zip.putNextEntry(new ZipEntry(pluginJarPath(artifactId)));
      zip.write("placeholder".getBytes(StandardCharsets.UTF_8));
      zip.closeEntry();
    }
    return out.toByteArray();
  }

  private static String pluginDir(String artifactId) {
    return "plugins/misc/" + artifactId;
  }

  private static String pluginJarPath(String artifactId) {
    return pluginDir(artifactId) + "/" + artifactId + ".jar";
  }

  /**
   * An Artifactory access token, so the bearer path is exercised against a real issuer.
   *
   * <p>The platform endpoint ({@code /access/api/v1/tokens}) answers "Unsupported authentication
   * method Basic", so this uses the Artifactory one, which does accept Basic. Scope {@code
   * applied-permissions/admin} is rejected as unaccepted; {@code applied-permissions/user} is
   * granted and is all a read of one repository needs.
   */
  private static String createAccessToken() throws Exception {
    HttpResponse<String> response =
        send(
            request("api/security/token")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        "username=" + ADMIN + "&scope=applied-permissions/user&expires_in=3600")),
            200);
    String body = response.body();
    int start = body.indexOf("\"access_token\"");
    assertTrue(start >= 0, "no access_token in " + body);
    int from = body.indexOf('"', body.indexOf(':', start)) + 1;
    return body.substring(from, body.indexOf('"', from));
  }

  private static HttpRequest.Builder request(String path) {
    return HttpRequest.newBuilder(URI.create(baseUrl + path))
        .timeout(Duration.ofMinutes(1))
        .header(
            "Authorization",
            "Basic "
                + Base64.getEncoder()
                    .encodeToString(
                        (ADMIN + ":" + ADMIN_PASSWORD).getBytes(StandardCharsets.UTF_8)));
  }

  private static HttpResponse<String> send(HttpRequest.Builder builder, int... acceptable)
      throws Exception {
    HttpResponse<String> response =
        client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    boolean ok = false;
    for (int status : acceptable) {
      ok |= response.statusCode() == status;
    }
    assertTrue(
        ok,
        "unexpected HTTP "
            + response.statusCode()
            + " for "
            + builder.build().uri()
            + ": "
            + response.body());
    return response;
  }

  // ------------------------------------------------------------ repository

  private static MarketplaceRepository repository() {
    MarketplaceRepository repo = new MarketplaceRepository("artifactory", baseUrl + REPO_KEY + "/");
    repo.setBrowse(true);
    repo.setGroupIdFilter(GROUP);
    return repo;
  }

  private static MarketplaceRepository withToken() {
    MarketplaceRepository repo = repository();
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    repo.setPassword(accessToken);
    return repo;
  }

  private static MarketplaceRepository withBasic() {
    MarketplaceRepository repo = repository();
    repo.setUsername(ADMIN);
    repo.setPassword(ADMIN_PASSWORD);
    return repo;
  }

  private static Optional<OptionalPluginInfo> find(
      List<OptionalPluginInfo> plugins, String artifactId) {
    return plugins.stream().filter(p -> artifactId.equals(p.getArtifactId())).findFirst();
  }

  // ----------------------------------------------------------------- tests

  @Test
  void artifactoryUrlSelectsTheJfrogBrowser() {
    assertEquals(MarketplaceRepository.BROWSER_JFROG, repository().effectiveBrowserType());
  }

  @Test
  void aqlListsPluginsForAnAuthenticatedRepository() throws Exception {
    List<OptionalPluginInfo> found = JfrogRepositoryBrowser.browse(withToken(), null, null);

    assertTrue(find(found, "acme-parser").isPresent(), () -> found.toString());
    assertTrue(find(found, "acme-writer").isPresent(), () -> found.toString());
    // A jar is a shared library, not something that can be installed as a plugin.
    assertTrue(find(found, "acme-lib").isEmpty(), () -> found.toString());
  }

  @Test
  void basicAuthWorksAsWellAsABearerToken() throws Exception {
    assertTrue(
        find(JfrogRepositoryBrowser.browse(withBasic(), null, null), "acme-parser").isPresent());
  }

  @Test
  void theStorageWalkAgreesWithAql() throws Exception {
    // The two backends must not disagree about what is installable, or which one ran becomes
    // visible to the user as a different plugin list.
    List<String> viaAql =
        JfrogRepositoryBrowser.browse(withToken(), null, null).stream()
            .map(OptionalPluginInfo::getArtifactId)
            .sorted()
            .toList();
    List<String> viaWalk =
        JfrogRepositoryBrowser.walkStorage(
                withBasic(),
                JfrogRepositoryBrowser.extractArtifactoryBase(repository().getUrl()),
                REPO_KEY,
                null,
                HttpClient.newHttpClient())
            .stream()
            .map(OptionalPluginInfo::getArtifactId)
            .distinct()
            .sorted()
            .toList();
    assertEquals(viaAql, viaWalk);
  }

  @Test
  void onlyTheNewestVersionOfAPluginIsOffered() throws Exception {
    OptionalPluginInfo parser =
        find(JfrogRepositoryBrowser.browse(withToken(), null, null), "acme-parser").orElseThrow();
    assertEquals("2026.09", parser.getVersion());
  }

  @Test
  void snapshotsCanBeHidden() throws Exception {
    assertTrue(
        find(JfrogRepositoryBrowser.browse(withToken(), null, null), "acme-snapshot").isPresent());

    MarketplaceRepository noSnapshots = withToken();
    noSnapshots.setIncludeSnapshots(false);
    assertTrue(
        find(JfrogRepositoryBrowser.browse(noSnapshots, null, null), "acme-snapshot").isEmpty());
  }

  @Test
  void aTextFilterNarrowsTheListing() throws Exception {
    List<OptionalPluginInfo> found = JfrogRepositoryBrowser.browse(withToken(), "writer", null);
    assertTrue(find(found, "acme-writer").isPresent(), () -> found.toString());
    assertTrue(find(found, "acme-parser").isEmpty(), () -> found.toString());
  }

  @Test
  void aBrowsedPluginCanActuallyBeDownloaded(@TempDir Path dir) throws Exception {
    // What browsing lists has to resolve as a download at the coordinates it reported.
    OptionalPluginInfo parser =
        find(JfrogRepositoryBrowser.browse(withToken(), null, null), "acme-parser").orElseThrow();
    assertNotNull(parser.getInstallPath());

    Path target = dir.resolve("acme-parser.zip");
    new MavenRepositoryClient(new LogChannel("jfrog-it"))
        .downloadZip(
            withToken(),
            new MavenCoordinates(parser.getGroupId(), parser.getArtifactId(), parser.getVersion()),
            target);

    assertTrue(Files.size(target) > 0);
  }

  @Test
  void aBrowsedPluginCanActuallyBeInstalled(@TempDir Path dir) throws Exception {
    // The whole chain in one go: browse Artifactory, take the coordinates off the result, and
    // install from them. Downloading proves the artifact is reachable; only installing proves the
    // coordinates discovery reports are the ones the installer can actually resolve.
    OptionalPluginInfo parser =
        find(JfrogRepositoryBrowser.browse(withToken(), null, null), "acme-parser").orElseThrow();

    Path hopHome = dir.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins"));

    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().clear();
    config.getRepositories().add(withToken());

    InstallReceipt receipt =
        new PluginInstaller(new LogChannel("jfrog-it"), hopHome, config)
            .install(
                new MavenCoordinates(
                    parser.getGroupId(), parser.getArtifactId(), parser.getVersion()),
                true);

    assertEquals("artifactory", receipt.getRepositoryId());
    assertEquals(parser.getVersion(), receipt.getVersion());
    assertTrue(
        Files.isRegularFile(hopHome.resolve(pluginJarPath("acme-parser"))),
        () -> "plugin jar not installed under " + hopHome);
    assertTrue(
        Files.isRegularFile(
            hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve("acme-parser.json")));
  }

  @Test
  void rejectedCredentialsExplainWhatWasSent() {
    MarketplaceRepository repo = repository();
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    repo.setPassword("not-a-real-token");

    HopException e =
        assertThrows(HopException.class, () -> JfrogRepositoryBrowser.browse(repo, null, null));
    // AQL is refused, the walk is refused too, and the message names the scheme that was used.
    assertTrue(
        e.getMessage().contains("bearer token") || e.getMessage().contains("401"), e.getMessage());
    assertFalse(e.getMessage().contains("Basic auth was sent"), e.getMessage());
  }
}
