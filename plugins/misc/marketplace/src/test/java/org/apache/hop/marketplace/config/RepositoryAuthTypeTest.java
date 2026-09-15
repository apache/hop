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

package org.apache.hop.marketplace.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hop.core.json.HopJson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Authentication type selection: which scheme a repository uses, and what is sent as a result.
 *
 * <p>The {@code auto} cases are a regression lock. Before {@code authType} existed the rule was
 * "Basic when a username and a password both resolve, anonymous otherwise", and {@code auto} has to
 * keep reproducing it exactly — in particular it must not infer a bearer token from a lone
 * password, because the global {@code HOP_MARKETPLACE_PASSWORD} reaches every repository including
 * public ones.
 */
class RepositoryAuthTypeTest {

  private static void env(Map<String, String> values) {
    MarketplaceRepository.setEnvironmentForTesting(values::get);
  }

  @AfterEach
  void restoreEnvironment() {
    MarketplaceRepository.setEnvironmentForTesting(null);
  }

  /** The Authorization header {@code applyAuth} would put on a request, if any. */
  private static Optional<String> authorization(MarketplaceRepository repo) {
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("https://example.org/")).GET();
    MarketplaceHttp.applyAuth(builder, repo);
    return builder.build().headers().firstValue("Authorization");
  }

  private static MarketplaceRepository repo(String id) {
    return new MarketplaceRepository(id, "https://example.org/");
  }

  // ---------------------------------------------------------------- auto

  @Test
  void autoIsTheDefault() {
    assertEquals(MarketplaceRepository.AUTH_AUTO, repo("acme").getAuthType());
  }

  @Test
  void autoSelectsBasicWhenBothPartsResolve() {
    env(Map.of());
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://example.org/", "u", "p");
    assertEquals(MarketplaceRepository.AUTH_BASIC, repo.effectiveAuthType());
    assertTrue(repo.hasCredentials());
    assertEquals(
        "Basic " + Base64.getEncoder().encodeToString("u:p".getBytes(StandardCharsets.UTF_8)),
        authorization(repo).orElseThrow());
  }

  @Test
  void autoSelectsNoneWhenNothingIsConfigured() {
    env(Map.of());
    MarketplaceRepository repo = repo("asf");
    assertEquals(MarketplaceRepository.AUTH_NONE, repo.effectiveAuthType());
    assertFalse(repo.hasCredentials());
    assertTrue(authorization(repo).isEmpty());
  }

  @Test
  void autoNeverInfersATokenFromALonePassword() {
    // Historical behaviour: a password with no username sends nothing at all. Inferring Bearer
    // here would leak the global HOP_MARKETPLACE_PASSWORD to ASF, Central and every other public
    // repository, all of which are contacted anonymously today.
    env(Map.of("HOP_MARKETPLACE_PASSWORD", "global-secret"));
    MarketplaceRepository repo = repo("asf");
    assertEquals(MarketplaceRepository.AUTH_NONE, repo.effectiveAuthType());
    assertFalse(repo.hasCredentials());
    assertTrue(authorization(repo).isEmpty());

    MarketplaceRepository configured = repo("acme");
    configured.setPassword("on-the-entry");
    assertEquals(MarketplaceRepository.AUTH_NONE, configured.effectiveAuthType());
    assertTrue(authorization(configured).isEmpty());
  }

  @Test
  void autoSelectsNoneWhenOnlyAUsernameResolves() {
    env(Map.of());
    MarketplaceRepository repo = repo("acme");
    repo.setUsername("reader");
    assertEquals(MarketplaceRepository.AUTH_NONE, repo.effectiveAuthType());
    assertFalse(repo.hasCredentials());
  }

  // ---------------------------------------------------------------- none

  @Test
  void noneSuppressesEnvironmentCredentials() {
    // The whole point of 'none': global variables reach every repository, and a public one that
    // rejects them looks broken. Opting out is a repository-level decision.
    env(Map.of("HOP_MARKETPLACE_USERNAME", "reader", "HOP_MARKETPLACE_PASSWORD", "secret"));
    MarketplaceRepository repo = repo("central");
    repo.setAuthType(MarketplaceRepository.AUTH_NONE);

    assertEquals(MarketplaceRepository.AUTH_NONE, repo.effectiveAuthType());
    assertFalse(repo.hasCredentials());
    assertFalse(repo.credentialsFromEnvironmentOnly());
    assertTrue(authorization(repo).isEmpty());
  }

  @Test
  void noneSuppressesConfiguredCredentialsToo() {
    env(Map.of());
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://example.org/", "u", "p");
    repo.setAuthType(MarketplaceRepository.AUTH_NONE);
    assertFalse(repo.hasCredentials());
    assertTrue(authorization(repo).isEmpty());
  }

  // ---------------------------------------------------------------- token

  @Test
  void tokenSendsBearerFromTheConfiguredPassword() {
    env(Map.of());
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    repo.setPassword("jfrog-access-token");

    assertEquals(MarketplaceRepository.AUTH_TOKEN, repo.effectiveAuthType());
    assertTrue(repo.hasCredentials());
    assertEquals("jfrog-access-token", repo.effectiveToken());
    assertEquals("Bearer jfrog-access-token", authorization(repo).orElseThrow());
  }

  @Test
  void tokenIgnoresAnyUsername() {
    env(Map.of("HOP_MARKETPLACE_USERNAME", "reader"));
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    repo.setPassword("t");
    assertEquals("Bearer t", authorization(repo).orElseThrow());
  }

  @Test
  void tokenReadsTheScopedTokenVariable() {
    env(Map.of("HOP_MARKETPLACE_ARTIFACTORY_TOKEN", "from-env"));
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);

    assertEquals("from-env", repo.effectiveToken());
    assertEquals("Bearer from-env", authorization(repo).orElseThrow());
    // Supplied by the environment, so a rejection may fall back to an anonymous retry.
    assertTrue(repo.credentialsFromEnvironmentOnly());
  }

  @Test
  void scopedPasswordVariableAlsoFeedsTheToken() {
    // _TOKEN reads better, but the existing _PASSWORD name has to keep working.
    env(Map.of("HOP_MARKETPLACE_ARTIFACTORY_PASSWORD", "from-password-var"));
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    assertEquals("Bearer from-password-var", authorization(repo).orElseThrow());
  }

  @Test
  void authTypeIsCaseAndWhitespaceInsensitive() {
    env(Map.of());
    MarketplaceRepository repo = repo("artifactory");
    repo.setPassword("t");
    repo.setAuthType("  Token ");
    assertEquals(MarketplaceRepository.AUTH_TOKEN, repo.effectiveAuthType());
    assertEquals("Bearer t", authorization(repo).orElseThrow());
  }

  // -------------------------------------------------- unsatisfiable choices

  @Test
  void basicWithoutAUsernameSendsNothingAndSaysWhy() {
    env(Map.of());
    MarketplaceRepository repo = repo("acme");
    repo.setAuthType(MarketplaceRepository.AUTH_BASIC);
    repo.setPassword("p");

    // The requested type is still reported, so the hint can name it.
    assertEquals(MarketplaceRepository.AUTH_BASIC, repo.effectiveAuthType());
    assertFalse(repo.hasCredentials());
    assertTrue(authorization(repo).isEmpty());

    String hint = MarketplaceHttp.authHint(401, repo);
    assertTrue(hint.contains("authType is 'basic'"), hint);
    assertTrue(hint.contains("no username"), hint);
    assertTrue(hint.contains("HOP_MARKETPLACE_ACME_USERNAME"), hint);
  }

  @Test
  void tokenWithoutATokenSendsNothingAndSaysWhy() {
    env(Map.of());
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);

    assertFalse(repo.hasCredentials());
    assertTrue(authorization(repo).isEmpty());

    String hint = MarketplaceHttp.authHint(401, repo);
    assertTrue(hint.contains("authType is 'token'"), hint);
    assertTrue(hint.contains("HOP_MARKETPLACE_ARTIFACTORY_TOKEN"), hint);
  }

  @Test
  void unrecognisedAuthTypeSendsNothingRatherThanGuessing() {
    env(Map.of("HOP_MARKETPLACE_USERNAME", "u", "HOP_MARKETPLACE_PASSWORD", "p"));
    MarketplaceRepository repo = repo("acme");
    repo.setAuthType("oauth2");

    assertFalse(repo.hasCredentials());
    assertTrue(authorization(repo).isEmpty());

    String hint = MarketplaceHttp.authHint(401, repo);
    assertTrue(hint.contains("'oauth2' is not recognised"), hint);
  }

  @Test
  void explicitNoneIsDistinguishedFromNothingConfigured() {
    env(Map.of());
    MarketplaceRepository configured = repo("central");
    configured.setAuthType(MarketplaceRepository.AUTH_NONE);
    assertTrue(MarketplaceHttp.authHint(401, configured).contains("authType is 'none'"));

    // auto with nothing set resolves to none as well, but the advice is different.
    String hint = MarketplaceHttp.authHint(401, repo("central"));
    assertTrue(hint.contains("HOP_MARKETPLACE_USERNAME"), hint);
    assertFalse(hint.contains("authType"), hint);
  }

  // ---------------------------------------------------------------- hints

  @Test
  void rejectedTokenHintsAtTheToken() {
    env(Map.of());
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    repo.setPassword("t");

    String hint = MarketplaceHttp.authHint(403, repo);
    assertTrue(hint.contains("bearer token"), hint);
    assertFalse(hint.contains("Basic"), hint);
  }

  @Test
  void rejectedEnvironmentTokenHintsAtTheScopedVariable() {
    env(Map.of("HOP_MARKETPLACE_ARTIFACTORY_TOKEN", "t"));
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);

    String hint = MarketplaceHttp.authHint(401, repo);
    assertTrue(hint.contains("anonymous retry also failed"), hint);
    assertTrue(hint.contains("HOP_MARKETPLACE_ARTIFACTORY_TOKEN"), hint);
  }

  @Test
  void unresolvedTokenVariableIsReportedAsSuch() {
    env(Map.of());
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    repo.setPassword("${NO_SUCH_VARIABLE}");

    String hint = MarketplaceHttp.authHint(401, repo);
    assertTrue(hint.contains("variable that is not set"), hint);
    assertTrue(hint.contains("HOP_MARKETPLACE_ARTIFACTORY_TOKEN"), hint);
  }

  // ------------------------------------------------------- persistence

  @Test
  void authTypeSurvivesExportImport(@TempDir Path dir) throws Exception {
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);

    Path file = dir.resolve("repo.yaml");
    MarketplaceRepositoryDefinition.save(file, repo);
    assertTrue(Files.readString(file).contains("authType"));

    MarketplaceRepository loaded = MarketplaceRepositoryDefinition.load(file);
    assertEquals(MarketplaceRepository.AUTH_TOKEN, loaded.getAuthType());
    assertEquals(MarketplaceRepository.AUTH_TOKEN, loaded.effectiveAuthType());
  }

  @Test
  void autoIsNotWrittenToYaml() {
    assertFalse(
        MarketplaceRepositoryDefinition.toYamlMap(repo("asf"), false).containsKey("authType"));
  }

  @Test
  void definitionWithoutAuthTypeKeepsTheDefault(@TempDir Path dir) throws Exception {
    Path file =
        Files.writeString(
            dir.resolve("legacy.yaml"),
            """
            schemaVersion: "1.0"
            kind: hop-marketplace-repository
            id: legacy
            url: https://repository.example.com/repository/hop/
            """);
    MarketplaceRepository repo = MarketplaceRepositoryDefinition.load(file);
    assertEquals(MarketplaceRepository.AUTH_AUTO, repo.getAuthType());
  }

  @Test
  void authTypeSurvivesTheHopConfigRoundTrip() {
    // hop-config.json is written by mapping the bean to a Map and read back the same way.
    MarketplaceConfig config = new MarketplaceConfig();
    MarketplaceRepository repo = repo("artifactory");
    repo.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    config.setRepositories(new ArrayList<>(List.of(repo)));

    Map<?, ?> asMap = HopJson.newMapper().convertValue(config, Map.class);
    MarketplaceConfig reloaded = HopJson.newMapper().convertValue(asMap, MarketplaceConfig.class);

    assertEquals(
        MarketplaceRepository.AUTH_TOKEN, reloaded.findRepository("artifactory").getAuthType());
  }

  @Test
  void reimportOverExistingEntryCarriesAuthTypeAcross() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setRepositories(
        new ArrayList<>(List.of(new MarketplaceRepository("acme", "https://old.example.org/"))));

    MarketplaceRepository imported = new MarketplaceRepository("acme", "https://new.example.org/");
    imported.setAuthType(MarketplaceRepository.AUTH_TOKEN);
    MarketplaceRepositoryDefinition.applyToConfig(config, imported, false);

    assertEquals(MarketplaceRepository.AUTH_TOKEN, config.findRepository("acme").getAuthType());
  }
}
