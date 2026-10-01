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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

/**
 * A definition fetched from a URL says where a repository is, never who connects to it, and must
 * arrive over a channel nobody can rewrite. Importing the same file from disk is deliberate and
 * keeps whatever it contains.
 */
class PublicUrlImportTest {

  private static Map<String, Object> definitionWithCredentials() {
    return Map.of(
        "id", "acme",
        "url", "https://nexus.example.org/repository/hop/",
        "urlTemplate", "https://downloads.example.org/${artifactId}-${version}.zip",
        "username", "planted-user",
        "password", "planted-password");
  }

  @Test
  void httpsIsRequired() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> MarketplaceRepositoryDefinition.requireHttps("http://example.org/repo.yaml"));
    assertTrue(e.getMessage().contains("https"));

    // Not a transport we can vouch for either.
    assertThrows(
        HopException.class,
        () -> MarketplaceRepositoryDefinition.requireHttps("ftp://example.org/repo.yaml"));
    assertThrows(
        HopException.class, () -> MarketplaceRepositoryDefinition.requireHttps("/tmp/repo.yaml"));
    assertThrows(HopException.class, () -> MarketplaceRepositoryDefinition.requireHttps("  "));
    assertThrows(HopException.class, () -> MarketplaceRepositoryDefinition.requireHttps(null));
  }

  @Test
  void httpsUrlsAreAcceptedAndTrimmed() throws Exception {
    assertEquals(
        "https://example.org/repo.yaml",
        MarketplaceRepositoryDefinition.requireHttps("  https://example.org/repo.yaml  "));
    // Scheme comparison is case-insensitive; a valid URL must not be rejected on casing.
    assertEquals(
        "HTTPS://example.org/repo.yaml",
        MarketplaceRepositoryDefinition.requireHttps("HTTPS://example.org/repo.yaml"));
  }

  @Test
  void credentialsAreStrippedFromDownloadedDefinitions() throws Exception {
    MarketplaceRepository downloaded =
        MarketplaceRepositoryDefinition.withoutCredentials(
            MarketplaceRepositoryDefinition.fromMap(definitionWithCredentials()));

    assertNull(downloaded.getUsername());
    assertNull(downloaded.getPassword());
    // Everything that describes the repository itself is untouched.
    assertEquals("acme", downloaded.getId());
    assertEquals("https://nexus.example.org/repository/hop/", downloaded.getUrl());
    assertEquals(
        "https://downloads.example.org/${artifactId}-${version}.zip", downloaded.getUrlTemplate());
  }

  @Test
  void importingTheSameFileFromDiskKeepsItsCredentials() throws Exception {
    // The asymmetry is the point: a file on disk was put there deliberately, so an admin can
    // provision one with credentials. A URL is only ever a pointer.
    MarketplaceRepository fromDisk =
        MarketplaceRepositoryDefinition.fromMap(definitionWithCredentials());

    assertEquals("planted-user", fromDisk.getUsername());
    assertEquals("planted-password", fromDisk.getPassword());
  }

  private static MarketplaceRepository claiming(boolean primary) {
    MarketplaceRepository repo = new MarketplaceRepository("putki", "https://forge.example.org/");
    repo.setPrimary(primary);
    return repo;
  }

  @Test
  void aDefinitionThatClaimsPrimaryIsFlaggedAsATakeover() {
    MarketplaceConfig config = new MarketplaceConfig(); // asf (primary) + central
    MarketplaceRepositoryDefinition.ImportRisk risk =
        MarketplaceRepositoryDefinition.assess(config, claiming(true));

    assertTrue(risk.takesOverPrimary());
    assertEquals(MarketplaceConfig.DEFAULT_ASF_NAME, risk.currentPrimaryName());
    assertFalse(risk.isSafe());
  }

  @Test
  void anOrdinaryDefinitionIsSafe() {
    MarketplaceRepositoryDefinition.ImportRisk risk =
        MarketplaceRepositoryDefinition.assess(new MarketplaceConfig(), claiming(false));

    assertFalse(risk.takesOverPrimary());
    assertNull(risk.currentPrimaryName());
    assertTrue(risk.isSafe());
  }

  @Test
  void reimportingTheRepositoryThatIsAlreadyPrimaryIsNotATakeover() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.addRepository(claiming(true));
    config.setPrimary("putki");

    // Same id, still primary: nothing is being taken over, so do not cry wolf.
    assertFalse(MarketplaceRepositoryDefinition.assess(config, claiming(true)).takesOverPrimary());
  }

  @Test
  void missingPublicRepositoriesLeaveNoFallback() throws Exception {
    MarketplaceConfig bare = new MarketplaceConfig();
    bare.setRepositories(new java.util.ArrayList<>());
    bare.addRepository(claiming(true));

    assertTrue(MarketplaceRepositoryDefinition.assess(bare, claiming(false)).noPublicFallback());
    // The shipped defaults are the fallback, whatever the imported definition looks like.
    assertFalse(
        MarketplaceRepositoryDefinition.assess(new MarketplaceConfig(), claiming(false))
            .noPublicFallback());
  }

  @Test
  void aDisabledApacheRepositoryIsNotAFallback() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setEnabled(MarketplaceConfig.DEFAULT_ASF_ID, false);
    config.setEnabled(MarketplaceConfig.DEFAULT_CENTRAL_ID, false);

    assertTrue(MarketplaceRepositoryDefinition.assess(config, claiming(false)).noPublicFallback());
  }

  @Test
  void strippingLeavesTheDiscoveryPayloadIntact() throws Exception {
    Map<String, Object> withPlugins =
        Map.of(
            "id",
            "acme",
            "url",
            "https://nexus.example.org/repository/hop/",
            "username",
            "planted-user",
            "plugins",
            List.of(Map.of("artifactId", "hop-tech-parquet", "version", "2.19.0")));

    MarketplaceRepository downloaded =
        MarketplaceRepositoryDefinition.withoutCredentials(
            MarketplaceRepositoryDefinition.fromMap(withPlugins));

    assertNull(downloaded.getUsername());
    assertEquals(1, downloaded.getPlugins().size());
    assertEquals("hop-tech-parquet", downloaded.getPlugins().get(0).getArtifactId());
  }
}
