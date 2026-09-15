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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The browser against a stand-in Artifactory: which endpoint is called for which configuration, and
 * what comes back out. The unit tests cover the parsers in isolation; these cover the wiring —
 * choosing AQL over the storage walk, falling back when AQL is refused, and the request budget.
 */
class JfrogRepositoryBrowserHttpTest {

  private HttpServer server;
  private String baseUrl;

  /** Every path requested, in order, so a test can assert which API was actually used. */
  private final List<String> requests = new ArrayList<>();

  /** Authorization headers seen, to prove the anonymous path stays anonymous. */
  private final List<String> authorizations = new ArrayList<>();

  /** Storage tree: folder path (relative to the repository) to its Folder Info response. */
  private final Map<String, String> storage = new LinkedHashMap<>();

  /** Status returned by the AQL endpoint; 200 serves {@link #aqlResponse}. */
  private int aqlStatus = 200;

  private String aqlResponse = "{\"results\":[]}";

  /** Last AQL query body received. */
  private String aqlQuery;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/artifactory/", this::handle);
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/artifactory/hop-plugins/";
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    requests.add(path);
    String authorization = exchange.getRequestHeaders().getFirst("Authorization");
    if (authorization != null) {
      authorizations.add(authorization);
    }
    try {
      if (path.endsWith("/api/search/aql")) {
        aqlQuery = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        respond(exchange, aqlStatus, aqlStatus == 200 ? aqlResponse : "{\"errors\":[]}");
        return;
      }
      String prefix = "/artifactory/api/storage/hop-plugins";
      if (path.startsWith(prefix)) {
        String folder = path.substring(prefix.length());
        folder = folder.startsWith("/") ? folder.substring(1) : folder;
        String body = storage.get(folder);
        respond(exchange, body == null ? 404 : 200, body == null ? "{}" : body);
        return;
      }
      respond(exchange, 404, "{}");
    } finally {
      exchange.close();
    }
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
  }

  private static java.net.http.HttpClient client() {
    return java.net.http.HttpClient.newHttpClient();
  }

  private MarketplaceRepository repository() {
    MarketplaceRepository repo = new MarketplaceRepository("artifactory", baseUrl);
    repo.setBrowse(true);
    return repo;
  }

  private MarketplaceRepository authenticated() {
    MarketplaceRepository repo = repository();
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    repo.setPassword("a-token");
    return repo;
  }

  /** A folder holding one plugin zip plus the sidecars Artifactory stores next to it. */
  private static String versionFolder(String zipName) {
    return """
        {
          "lastModified": "2026-07-21T10:00:00.000Z",
          "children": [
            {"uri": "/%s.pom", "folder": false},
            {"uri": "/%s.zip", "folder": false},
            {"uri": "/%s.zip.sha1", "folder": false}
          ]
        }
        """
        .formatted(zipName, zipName, zipName);
  }

  private static String folders(String... names) {
    String children =
        java.util.Arrays.stream(names)
            .map(n -> "{\"uri\": \"/" + n + "\", \"folder\": true}")
            .collect(Collectors.joining(","));
    return "{\"children\": [" + children + "]}";
  }

  /** com/acme/hop/acme-parser/{2026.06,2026.09} plus a jar-only artifact that must not list. */
  private void seedStorage() {
    storage.put("", folders("com"));
    storage.put("com", folders("acme"));
    storage.put("com/acme", folders("hop"));
    storage.put("com/acme/hop", folders("acme-parser", "acme-lib"));
    storage.put("com/acme/hop/acme-parser", folders("2026.06", "2026.09"));
    storage.put("com/acme/hop/acme-parser/2026.06", versionFolder("acme-parser-2026.06"));
    storage.put("com/acme/hop/acme-parser/2026.09", versionFolder("acme-parser-2026.09"));
    storage.put("com/acme/hop/acme-lib", folders("1.0.0"));
    storage.put(
        "com/acme/hop/acme-lib/1.0.0",
        "{\"children\": [{\"uri\": \"/acme-lib-1.0.0.jar\", \"folder\": false}]}");
  }

  @Test
  void credentialsSelectAqlInOneRoundTrip() throws Exception {
    aqlResponse =
        """
        {"results": [
          {"repo":"hop-plugins","path":"com/acme/hop/acme-parser/2026.09",
           "name":"acme-parser-2026.09.zip","modified":"2026-07-21T10:00:00.000Z"}
        ]}
        """;

    List<OptionalPluginInfo> found =
        JfrogRepositoryBrowser.browse(authenticated(), null, null, client());

    assertEquals(1, found.size());
    assertEquals("acme-parser", found.get(0).getArtifactId());
    assertEquals(List.of("/artifactory/api/search/aql"), requests);
    assertTrue(aqlQuery.contains("\"repo\":\"hop-plugins\""), aqlQuery);
    assertEquals(List.of("Bearer a-token"), authorizations);
  }

  @Test
  void anonymousRepositoriesWalkStorageWithoutTryingAql() throws Exception {
    // AQL needs an authenticated user, so an anonymous repository must not waste a call on it.
    seedStorage();

    List<OptionalPluginInfo> found =
        JfrogRepositoryBrowser.browse(repository(), null, null, client());

    assertFalse(requests.stream().anyMatch(r -> r.contains("aql")), requests.toString());
    assertTrue(authorizations.isEmpty(), authorizations.toString());

    // The jar-only artifact is not installable and does not appear.
    assertEquals(1, found.size());
    assertEquals("com.acme.hop", found.get(0).getGroupId());
    assertEquals("acme-parser", found.get(0).getArtifactId());
    assertEquals("2026.09", found.get(0).getVersion());
    assertEquals(
        "com/acme/hop/acme-parser/2026.09/acme-parser-2026.09.zip", found.get(0).getInstallPath());
  }

  @Test
  void aqlRefusalFallsBackToTheStorageWalk() throws Exception {
    // A token without AQL permission, or an instance with it disabled, still has to browse.
    aqlStatus = 403;
    seedStorage();

    List<OptionalPluginInfo> found =
        JfrogRepositoryBrowser.browse(authenticated(), null, null, client());

    assertTrue(requests.get(0).endsWith("/api/search/aql"), requests.toString());
    assertTrue(requests.size() > 1, "expected a storage walk after the refusal");
    assertEquals(1, found.size());
    assertEquals("acme-parser", found.get(0).getArtifactId());
  }

  @Test
  void groupIdFilterStartsTheWalkDeeperInsteadOfScanningTheRepository() throws Exception {
    seedStorage();
    MarketplaceRepository repo = repository();
    repo.setGroupIdFilter("com.acme.hop");

    List<OptionalPluginInfo> found = JfrogRepositoryBrowser.browse(repo, null, null, client());

    assertEquals(1, found.size());
    // The com/ and com/acme/ levels are skipped entirely.
    assertFalse(requests.contains("/artifactory/api/storage/hop-plugins/com"), requests.toString());
    assertTrue(
        requests.contains("/artifactory/api/storage/hop-plugins/com/acme/hop"),
        requests.toString());
  }

  @Test
  void snapshotsCanBeHiddenOnTheWalk() throws Exception {
    storage.put("", folders("com"));
    storage.put("com", folders("acme"));
    storage.put("com/acme", folders("hop"));
    storage.put("com/acme/hop", folders("acme-parser"));
    storage.put("com/acme/hop/acme-parser", folders("1.0.0-SNAPSHOT"));
    storage.put(
        "com/acme/hop/acme-parser/1.0.0-SNAPSHOT", versionFolder("acme-parser-1.0.0-SNAPSHOT"));

    MarketplaceRepository repo = repository();
    assertEquals(1, JfrogRepositoryBrowser.browse(repo, null, null, client()).size());

    repo.setIncludeSnapshots(false);
    assertTrue(JfrogRepositoryBrowser.browse(repo, null, null, client()).isEmpty());
  }

  @Test
  void textFilterNarrowsTheResults() throws Exception {
    seedStorage();
    storage.put("com/acme/hop", folders("acme-parser", "acme-writer"));
    storage.put("com/acme/hop/acme-writer", folders("1.0.0"));
    storage.put("com/acme/hop/acme-writer/1.0.0", versionFolder("acme-writer-1.0.0"));

    List<OptionalPluginInfo> found =
        JfrogRepositoryBrowser.browse(repository(), "writer", null, client());

    assertEquals(1, found.size());
    assertEquals("acme-writer", found.get(0).getArtifactId());
  }

  @Test
  void anUnreadableStartingPointIsReportedRatherThanReturningNothing() {
    // An empty list here would look like "no plugins published", which is the wrong diagnosis.
    MarketplaceRepository repo = repository();
    repo.setGroupIdFilter("com.absent");

    HopException e =
        assertThrows(
            HopException.class, () -> JfrogRepositoryBrowser.browse(repo, null, null, client()));
    assertTrue(e.getMessage().contains("404"), e.getMessage());
  }

  @Test
  void aUrlThatIsNotArtifactoryFailsWithAUsableMessage() {
    MarketplaceRepository repo =
        new MarketplaceRepository("nexus", "https://nexus.example.org/repository/hop/");
    HopException e =
        assertThrows(
            HopException.class, () -> JfrogRepositoryBrowser.browse(repo, null, null, client()));
    assertTrue(e.getMessage().contains("artifactory/{repository}"), e.getMessage());
  }

  @Test
  void blankRepositoryIsEmptyRatherThanAFailure() throws Exception {
    assertTrue(JfrogRepositoryBrowser.browse(null, null, null).isEmpty());
    assertTrue(
        JfrogRepositoryBrowser.browse(new MarketplaceRepository("empty", ""), null, null, client())
            .isEmpty());
  }

  @Test
  void discoveryRoutesArtifactoryUrlsToThisBrowser() throws Exception {
    // The dispatch in PluginDiscovery is where a new backend gets shadowed by an older one.
    seedStorage();
    MarketplaceRepository repo = repository();
    assertEquals(MarketplaceRepository.BROWSER_JFROG, repo.effectiveBrowserType());

    List<OptionalPluginInfo> found = PluginDiscovery.discoverRepoLive(repo, null, null);
    assertEquals(1, found.size());
    assertEquals("acme-parser", found.get(0).getArtifactId());
    assertTrue(requests.stream().anyMatch(r -> r.contains("/api/storage/")), requests.toString());
  }

  @Test
  void aCatalogUrlStillWinsOverLiveBrowsing() throws Exception {
    seedStorage();
    MarketplaceRepository repo = repository();
    repo.setCatalogUrl(baseUrl + "does-not-exist.yaml");

    assertThrows(HopException.class, () -> PluginDiscovery.discoverRepoLive(repo, null, null));
    assertFalse(requests.stream().anyMatch(r -> r.contains("/api/storage/")), requests.toString());
  }

  @Test
  void definitionMetadataStillEnrichesLiveResults() throws Exception {
    seedStorage();
    MarketplaceRepository repo = repository();
    OptionalPluginInfo described = new OptionalPluginInfo();
    described.setGroupId("com.acme.hop");
    described.setArtifactId("acme-parser");
    described.setName("Acme Parser");
    described.setDescription("Parses Acme files");
    repo.setPlugins(List.of(described));

    List<OptionalPluginInfo> found = PluginDiscovery.discoverRepoLive(repo, null, null);
    assertEquals(1, found.size());
    assertEquals("Acme Parser", found.get(0).getName());
    assertEquals("Parses Acme files", found.get(0).getDescription());
    // The live version is what can actually be installed, so it is not overwritten by metadata.
    assertEquals("2026.09", found.get(0).getVersion());
  }

  @Test
  void theWalkIsBoundedAndSaysSoWhenItStops() throws Exception {
    // Breadth, not depth: a repository with more folders than the budget allows requests for.
    int width = JfrogRepositoryBrowser.MAX_WALK_REQUESTS + 50;
    String[] children = new String[width];
    for (int i = 0; i < width; i++) {
      children[i] = "a" + i;
      storage.put(children[i], folders());
    }
    storage.put("", folders(children));
    RecordingLog log = new RecordingLog();

    List<OptionalPluginInfo> found =
        JfrogRepositoryBrowser.browse(repository(), null, log, client());

    assertTrue(found.isEmpty());
    assertTrue(requests.size() <= JfrogRepositoryBrowser.MAX_WALK_REQUESTS, "" + requests.size());
    assertNotNull(log.firstMatching("stopped after"), log.messages.toString());
  }

  @Test
  void depthIsCappedEvenWithinTheRequestBudget() throws Exception {
    // A single chain deeper than the cap: the walk stops descending instead of following it.
    StringBuilder path = new StringBuilder();
    storage.put("", folders("a"));
    for (int depth = 1; depth <= JfrogRepositoryBrowser.MAX_WALK_DEPTH + 3; depth++) {
      path.append(depth == 1 ? "a" : "/a");
      storage.put(path.toString(), folders("a"));
    }

    assertTrue(JfrogRepositoryBrowser.browse(repository(), null, null, client()).isEmpty());
    assertTrue(
        requests.size() <= JfrogRepositoryBrowser.MAX_WALK_DEPTH + 2,
        "walked " + requests.size() + " folders: " + requests);
  }

  @Test
  void unreadableSubfoldersAreSkippedRatherThanFailingTheBrowse() throws Exception {
    // Permissions in Artifactory are per path; one denied branch must not lose the whole listing.
    seedStorage();
    storage.remove("com/acme/hop/acme-lib");

    List<OptionalPluginInfo> found =
        JfrogRepositoryBrowser.browse(repository(), null, null, client());

    assertEquals(1, found.size());
    assertEquals("acme-parser", found.get(0).getArtifactId());
  }

  /** Minimal log channel that keeps what it was told. */
  private static class RecordingLog extends org.apache.hop.core.logging.LogChannel {
    private final List<String> messages = new ArrayList<>();

    RecordingLog() {
      super("jfrog-test");
    }

    @Override
    public void logBasic(String s) {
      messages.add(s);
    }

    @Override
    public void logDetailed(String s) {
      messages.add(s);
    }

    String firstMatching(String needle) {
      return messages.stream().filter(m -> m.contains(needle)).findFirst().orElse(null);
    }
  }

  @Test
  void listedPluginsAreMarkedAsDiscovered() throws Exception {
    seedStorage();
    OptionalPluginInfo info =
        JfrogRepositoryBrowser.browse(repository(), null, null, client()).get(0);
    assertEquals("auto-discovered", info.getCategory());
    assertEquals("artifactory", info.getSource());
    assertEquals("2026-07-21T10:00:00.000Z", info.getLastUpdated());
  }
}
