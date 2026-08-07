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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.marketplace.env.HopEnvironmentSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Passwords must not reach hop-config.json (or an exported definition) in clear text, and whatever
 * form they are stored in must survive a round trip.
 */
class RepositoryPasswordStorageTest {

  private static final String PLAIN = "s3cret-token";

  @AfterEach
  void restoreEnvironment() {
    MarketplaceRepository.setEnvironmentForTesting(null);
    System.clearProperty("MARKETPLACE_TEST_TOKEN");
  }

  /** The password as it would appear in hop-config.json. */
  @SuppressWarnings("unchecked")
  private static String storedPassword(MarketplaceConfig config) {
    Map<String, Object> map = HopJson.newMapper().convertValue(config, Map.class);
    List<Map<String, Object>> repos = (List<Map<String, Object>>) map.get("repositories");
    return (String) repos.get(0).get("password");
  }

  private static MarketplaceConfig configWithPassword(String password) {
    MarketplaceConfig config = new MarketplaceConfig();
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://example.org/", "user", password);
    config.setRepositories(new java.util.ArrayList<>(List.of(repo)));
    return config;
  }

  @Test
  void passwordIsObfuscatedInTheConfigFile() {
    String stored = storedPassword(configWithPassword(PLAIN));
    assertNotEquals(PLAIN, stored);
    assertFalse(stored.contains(PLAIN), "clear text password found in stored config: " + stored);
    assertTrue(stored.startsWith("Encrypted "), "expected an encoded password, got: " + stored);
  }

  @Test
  void obfuscatedPasswordSurvivesASaveLoadRoundTrip() {
    Map<String, Object> saved =
        HopJson.newMapper().convertValue(configWithPassword(PLAIN), Map.class);
    MarketplaceConfig reloaded = HopJson.newMapper().convertValue(saved, MarketplaceConfig.class);

    MarketplaceRepository repo = reloaded.findRepository("acme");
    assertEquals(PLAIN, repo.getPassword());
    assertEquals(PLAIN, repo.effectivePassword());
  }

  @Test
  void clearTextPasswordsFromOlderConfigsStillLoad() {
    // Written by a Hop version that had no obfuscation: read it as-is, re-save it encoded.
    Map<String, Object> legacy =
        Map.of(
            "repositories",
            List.of(Map.of("id", "acme", "url", "https://example.org/", "password", PLAIN)));

    MarketplaceConfig loaded = HopJson.newMapper().convertValue(legacy, MarketplaceConfig.class);
    assertEquals(PLAIN, loaded.findRepository("acme").getPassword());
    assertTrue(storedPassword(loaded).startsWith("Encrypted "));
  }

  @Test
  void variablesAreStoredAsTypedAndResolvedWhenUsed() {
    MarketplaceConfig config = configWithPassword("${MARKETPLACE_TEST_TOKEN}");
    // A variable is a reference, not a secret: it stays readable in the file.
    assertEquals("${MARKETPLACE_TEST_TOKEN}", storedPassword(config));

    MarketplaceRepository repo = config.findRepository("acme");
    System.setProperty("MARKETPLACE_TEST_TOKEN", PLAIN);
    assertEquals(PLAIN, repo.effectivePassword());
    assertTrue(repo.hasCredentials());
  }

  @Test
  void unresolvedVariablesAreExplainedOnAuthFailure() {
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://example.org/", "user", "${NO_SUCH_VARIABLE}");
    // Nothing to resolve to, so the expression is passed through rather than silently emptied.
    assertEquals("${NO_SUCH_VARIABLE}", repo.effectivePassword());
    assertTrue(MarketplaceHttp.authHint(401, repo).contains("variable that is not set"));
  }

  @Test
  void exportedDefinitionsCarryAnObfuscatedPassword() {
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://example.org/", "user", PLAIN);

    Map<String, Object> withoutPassword = MarketplaceRepositoryDefinition.toYamlMap(repo, false);
    assertFalse(withoutPassword.containsKey("password"));

    Map<String, Object> withPassword = MarketplaceRepositoryDefinition.toYamlMap(repo, true);
    assertTrue(String.valueOf(withPassword.get("password")).startsWith("Encrypted "));
  }

  @Test
  void importedDefinitionsAcceptBothForms() throws Exception {
    Map<String, Object> encoded =
        Map.of(
            "id",
            "acme",
            "url",
            "https://example.org/",
            "password",
            MarketplaceSecrets.encode(PLAIN));
    assertEquals(PLAIN, MarketplaceRepositoryDefinition.fromMap(encoded).getPassword());

    // Hand-written definitions are clear text; they must import unchanged.
    Map<String, Object> plain =
        Map.of("id", "acme", "url", "https://example.org/", "password", PLAIN);
    assertEquals(PLAIN, MarketplaceRepositoryDefinition.fromMap(plain).getPassword());
  }

  @Test
  void environmentFilesObfuscatePasswordsToo() {
    HopEnvironmentSpec.RepositoryRef ref = new HopEnvironmentSpec.RepositoryRef();
    ref.setId("acme");
    ref.setUrl("https://example.org/");
    ref.setPassword(PLAIN);

    Map<String, Object> written = HopJson.newMapper().convertValue(ref, Map.class);
    assertTrue(String.valueOf(written.get("password")).startsWith("Encrypted "));

    HopEnvironmentSpec.RepositoryRef read =
        HopJson.newMapper().convertValue(written, HopEnvironmentSpec.RepositoryRef.class);
    assertEquals(PLAIN, read.getPassword());
  }
}
