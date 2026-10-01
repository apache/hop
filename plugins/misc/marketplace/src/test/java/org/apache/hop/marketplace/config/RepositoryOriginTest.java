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

/**
 * The origin comparison that decides whether stored credentials belong to a repository URL, and the
 * opt-out from the global environment credentials that goes with it.
 */
class RepositoryOriginTest {

  @AfterEach
  void restoreEnvironment() {
    MarketplaceRepository.setEnvironmentForTesting(null);
  }

  @Test
  void originIsSchemeHostAndPortWithDefaultPortMadeExplicit() {
    assertEquals(
        "https://nexus.example.org:443",
        MarketplaceRepository.originOf("https://nexus.example.org/repository/hop/"));
    assertEquals(
        "http://nexus.example.org:80", MarketplaceRepository.originOf("http://nexus.example.org/"));
    assertEquals(
        "https://nexus.example.org:8443",
        MarketplaceRepository.originOf("https://nexus.example.org:8443/repository/hop/"));
  }

  @Test
  void originIsCaseInsensitiveOnSchemeAndHost() {
    assertEquals(
        MarketplaceRepository.originOf("https://nexus.example.org/repository/hop/"),
        MarketplaceRepository.originOf("HTTPS://Nexus.Example.ORG/repository/hop/"));
  }

  @Test
  void originIsNullWhenThereIsNoHostToCompare() {
    assertNull(MarketplaceRepository.originOf(null));
    assertNull(MarketplaceRepository.originOf("  "));
    assertNull(MarketplaceRepository.originOf("repository/hop/"));
    assertNull(MarketplaceRepository.originOf("file:/var/repo/"));
    assertNull(MarketplaceRepository.originOf("https://nexus example.org/"));
  }

  @Test
  void sameOriginIgnoresThePath() {
    MarketplaceRepository repo =
        new MarketplaceRepository("nexus", "https://nexus.example.org/repository/hop/");
    assertTrue(repo.sameOriginAs("https://nexus.example.org/repository/hop-private/"));
    assertTrue(repo.sameOriginAs("https://nexus.example.org:443/repository/hop/"));
  }

  @Test
  void sameOriginRejectsADifferentHostSchemeOrPort() {
    MarketplaceRepository repo =
        new MarketplaceRepository("nexus", "https://nexus.example.org/repository/hop/");
    assertFalse(repo.sameOriginAs("https://evil.example.com/repository/hop/"));
    assertFalse(repo.sameOriginAs("http://nexus.example.org/repository/hop/"));
    assertFalse(repo.sameOriginAs("https://nexus.example.org:8443/repository/hop/"));
    // A subdomain is a different host, not a relative of it.
    assertFalse(repo.sameOriginAs("https://nexus.example.org.evil.com/repository/hop/"));
  }

  @Test
  void anUnparseableUrlOnEitherSideNeverMatches() {
    assertFalse(new MarketplaceRepository("nexus", "not a url").sameOriginAs("not a url"));
    assertFalse(
        new MarketplaceRepository("nexus", "https://nexus.example.org/").sameOriginAs("  "));
  }

  @Test
  void globalEnvironmentCredentialsCanBeSuppressedWhileScopedOnesStillApply() {
    MarketplaceRepository.setEnvironmentForTesting(
        Map.of(
                "HOP_MARKETPLACE_USERNAME", "global-user",
                "HOP_MARKETPLACE_PASSWORD", "global-pass",
                "HOP_MARKETPLACE_SPEC_USERNAME", "spec-user",
                "HOP_MARKETPLACE_SPEC_PASSWORD", "spec-pass")
            ::get);

    MarketplaceRepository foreign =
        new MarketplaceRepository("other", "https://other.example.org/");
    foreign.setGlobalEnvironmentCredentials(false);
    assertNull(foreign.effectiveUsername());
    assertNull(foreign.effectivePassword());
    assertFalse(foreign.hasCredentials());

    // The repository-scoped pair names one repository, so it is still honoured.
    MarketplaceRepository spec = new MarketplaceRepository("spec", "https://other.example.org/");
    spec.setGlobalEnvironmentCredentials(false);
    assertEquals("spec-user", spec.effectiveUsername());
    assertEquals("spec-pass", spec.effectivePassword());
  }

  @Test
  void suppressionIsNotPersisted() throws Exception {
    MarketplaceRepository repo = new MarketplaceRepository("spec", "https://other.example.org/");
    repo.setGlobalEnvironmentCredentials(false);
    String json = org.apache.hop.core.json.HopJson.newMapper().writeValueAsString(repo);
    assertFalse(json.contains("globalEnvironmentCredentials"));
    assertTrue(
        org.apache.hop.core.json.HopJson.newMapper()
            .readValue(json, MarketplaceRepository.class)
            .isGlobalEnvironmentCredentials());
  }
}
