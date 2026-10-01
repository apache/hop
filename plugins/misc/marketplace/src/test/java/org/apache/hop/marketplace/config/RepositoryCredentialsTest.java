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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RepositoryCredentialsTest {

  private static void env(Map<String, String> values) {
    MarketplaceRepository.setEnvironmentForTesting(values::get);
  }

  @AfterEach
  void restoreEnvironment() {
    MarketplaceRepository.setEnvironmentForTesting(null);
  }

  @Test
  void noCredentialsMeansAnonymous() {
    env(Map.of());
    MarketplaceRepository repo = new MarketplaceRepository("asf", "https://example.org/");
    assertNull(repo.effectiveUsername());
    assertNull(repo.effectivePassword());
    assertFalse(repo.hasCredentials());
    assertFalse(repo.credentialsFromEnvironmentOnly());
  }

  @Test
  void globalEnvironmentAppliesToAnyRepository() {
    env(Map.of("HOP_MARKETPLACE_USERNAME", "reader", "HOP_MARKETPLACE_PASSWORD", "secret"));
    MarketplaceRepository repo = new MarketplaceRepository("asf", "https://example.org/");
    assertEquals("reader", repo.effectiveUsername());
    assertEquals("secret", repo.effectivePassword());
    // Global credentials are not specific to this repository, so a rejection may be retried.
    assertTrue(repo.credentialsFromEnvironmentOnly());
  }

  @Test
  void scopedEnvironmentBeatsGlobal() {
    env(
        Map.of(
            "HOP_MARKETPLACE_USERNAME", "global-user",
            "HOP_MARKETPLACE_PASSWORD", "global-pass",
            "HOP_MARKETPLACE_ACME_USERNAME", "acme-user",
            "HOP_MARKETPLACE_ACME_PASSWORD", "acme-pass"));
    MarketplaceRepository acme = new MarketplaceRepository("acme", "https://example.org/");
    assertEquals("acme-user", acme.effectiveUsername());
    assertEquals("acme-pass", acme.effectivePassword());

    // A different repository still gets the global pair.
    MarketplaceRepository other = new MarketplaceRepository("asf", "https://example.org/");
    assertEquals("global-user", other.effectiveUsername());
  }

  @Test
  void repositoryIdIsNormalisedForEnvironmentNames() {
    assertEquals(
        "LOCAL_NEXUS", new MarketplaceRepository("local-nexus", "u").environmentIdPrefix());
    assertEquals("ACME", new MarketplaceRepository("acme", "u").environmentIdPrefix());
    assertEquals("A_B_C", new MarketplaceRepository("a.b-c", "u").environmentIdPrefix());

    env(Map.of("HOP_MARKETPLACE_LOCAL_NEXUS_USERNAME", "n"));
    assertEquals("n", new MarketplaceRepository("local-nexus", "u").effectiveUsername());
  }

  @Test
  void configuredCredentialsAreNeverTreatedAsEnvironmental() {
    env(Map.of("HOP_MARKETPLACE_USERNAME", "global", "HOP_MARKETPLACE_PASSWORD", "global"));
    MarketplaceRepository repo =
        new MarketplaceRepository("private", "https://example.org/", "user", "pass");
    assertEquals("user", repo.effectiveUsername());
    assertEquals("pass", repo.effectivePassword());
    // Deliberate credentials: a 401 must surface, not silently retry anonymously.
    assertFalse(repo.credentialsFromEnvironmentOnly());
  }

  @Test
  void partiallyConfiguredCredentialsAreNotEnvironmental() {
    // username on the entry, password from the environment: still a deliberate pairing
    env(Map.of("HOP_MARKETPLACE_PASSWORD", "from-env"));
    MarketplaceRepository repo = new MarketplaceRepository("mixed", "https://example.org/");
    repo.setUsername("configured");
    assertTrue(repo.hasCredentials());
    assertFalse(repo.credentialsFromEnvironmentOnly());
  }

  @Test
  void authHintExplainsWhatWasSent() {
    env(Map.of());
    MarketplaceRepository anonymous = new MarketplaceRepository("asf", "https://example.org/");
    assertTrue(MarketplaceHttp.authHint(401, anonymous).contains("No credentials were sent"));

    env(Map.of("HOP_MARKETPLACE_USERNAME", "u", "HOP_MARKETPLACE_PASSWORD", "p"));
    MarketplaceRepository fromEnv = new MarketplaceRepository("acme", "https://example.org/");
    String hint = MarketplaceHttp.authHint(401, fromEnv);
    assertTrue(hint.contains("anonymous retry also failed"));
    assertTrue(hint.contains("HOP_MARKETPLACE_ACME_USERNAME"));

    // Not an auth failure: no hint at all.
    assertEquals("", MarketplaceHttp.authHint(404, fromEnv));
  }
}
