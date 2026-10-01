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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MarketplaceRepositoryBrowserTypeTest {

  @Test
  void forgejoRegistryUrlIsDetected() {
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://forge.example.org/api/packages/acme/maven");
    assertEquals(MarketplaceRepository.BROWSER_FORGEJO, repo.effectiveBrowserType());
  }

  @Test
  void artifactoryUrlIsDetected() {
    assertEquals(
        MarketplaceRepository.BROWSER_JFROG,
        new MarketplaceRepository("cloud", "https://acme.jfrog.io/artifactory/hop-plugins/")
            .effectiveBrowserType());
    assertEquals(
        MarketplaceRepository.BROWSER_JFROG,
        new MarketplaceRepository(
                "self-hosted", "https://artifactory.example.com/artifactory/hop-plugins-local/")
            .effectiveBrowserType());
  }

  @Test
  void forgejoIsTestedBeforeArtifactory() {
    // A Forgejo instance reachable under a path containing /artifactory/ must still read as
    // Forgejo.
    assertEquals(
        MarketplaceRepository.BROWSER_FORGEJO,
        new MarketplaceRepository(
                "mixed", "https://forge.example.org/artifactory/api/packages/a/maven")
            .effectiveBrowserType());
  }

  @Test
  void explicitJfrogWinsOverANexusLookingUrl() {
    // Artifactory behind a custom context path has no /artifactory/ segment to detect.
    MarketplaceRepository repo =
        new MarketplaceRepository("custom", "https://build.example.com/repo/hop-plugins/");
    assertEquals(MarketplaceRepository.BROWSER_NEXUS, repo.effectiveBrowserType());
    repo.setBrowserType(" JFrog ");
    assertEquals(MarketplaceRepository.BROWSER_JFROG, repo.effectiveBrowserType());
  }

  @Test
  void nexusRemainsTheDefault() {
    assertEquals(
        MarketplaceRepository.BROWSER_NEXUS,
        new MarketplaceRepository("asf", "https://repository.apache.org/content/groups/public/")
            .effectiveBrowserType());
    assertEquals(
        MarketplaceRepository.BROWSER_NEXUS,
        new MarketplaceRepository("local", "http://127.0.0.1:8081/repository/hop-plugins/")
            .effectiveBrowserType());
  }

  @Test
  void explicitTypeWinsOverDetection() {
    MarketplaceRepository repo =
        new MarketplaceRepository("proxy", "https://mirror.example.com/api/packages/acme/maven");
    repo.setBrowserType("NEXUS");
    assertEquals(MarketplaceRepository.BROWSER_NEXUS, repo.effectiveBrowserType());

    repo.setBrowserType(" Forgejo ");
    assertEquals(MarketplaceRepository.BROWSER_FORGEJO, repo.effectiveBrowserType());

    repo.setBrowserType(MarketplaceRepository.BROWSER_AUTO);
    assertEquals(MarketplaceRepository.BROWSER_FORGEJO, repo.effectiveBrowserType());
  }

  @Test
  void browserTypeSurvivesExportImport(@TempDir Path dir) throws Exception {
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://forge.example.org/api/packages/acme/maven");
    repo.setBrowse(true);
    repo.setBrowserType(MarketplaceRepository.BROWSER_FORGEJO);

    Path file = dir.resolve("repo.yaml");
    MarketplaceRepositoryDefinition.save(file, repo);
    assertTrue(Files.readString(file).contains("browserType"));

    MarketplaceRepository loaded = MarketplaceRepositoryDefinition.load(file);
    assertEquals(MarketplaceRepository.BROWSER_FORGEJO, loaded.getBrowserType());
    assertEquals(MarketplaceRepository.BROWSER_FORGEJO, loaded.effectiveBrowserType());
  }

  @Test
  void autoIsNotWrittenToYaml() {
    MarketplaceRepository repo =
        new MarketplaceRepository("asf", "https://repository.apache.org/content/groups/public/");
    Map<String, Object> yaml = MarketplaceRepositoryDefinition.toYamlMap(repo, false);
    assertFalse(yaml.containsKey("browserType"));
  }

  @Test
  void missingBrowserTypeKeepsDefault() throws Exception {
    // A definition written before browserType existed must still load.
    MarketplaceRepository repo =
        MarketplaceRepositoryDefinition.load(
            Files.writeString(
                Files.createTempFile("repo", ".yaml"),
                """
                schemaVersion: "1.0"
                kind: hop-marketplace-repository
                id: legacy
                url: https://repository.example.com/repository/hop/
                browse: true
                """));
    assertEquals(MarketplaceRepository.BROWSER_AUTO, repo.getBrowserType());
    assertEquals(MarketplaceRepository.BROWSER_NEXUS, repo.effectiveBrowserType());
  }

  @Test
  void urlTemplateSurvivesExportImport(@TempDir Path dir) throws Exception {
    MarketplaceRepository repo =
        new MarketplaceRepository("acme", "https://forge.example.org/api/packages/acme/maven");
    repo.setUrlTemplate(
        "https://forge.example.org/acme/dist/releases/download/v${version}/${artifactId}-${version}.zip");

    Path file = dir.resolve("template.yaml");
    MarketplaceRepositoryDefinition.save(file, repo);
    MarketplaceRepository loaded = MarketplaceRepositoryDefinition.load(file);
    assertEquals(repo.getUrlTemplate(), loaded.getUrlTemplate());
  }

  @Test
  void urlTemplateAbsentByDefault() {
    MarketplaceRepository repo =
        new MarketplaceRepository("asf", "https://repository.apache.org/content/groups/public/");
    assertNull(repo.getUrlTemplate());
    assertFalse(MarketplaceRepositoryDefinition.toYamlMap(repo, false).containsKey("urlTemplate"));
  }

  /**
   * Re-importing over an existing entry must carry the new fields across. The upsert path copies
   * field by field, so a field missing there is dropped silently on every re-import.
   */
  @Test
  void reimportOverExistingEntryKeepsBrowserTypeAndTemplate() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setRepositories(
        new java.util.ArrayList<>(
            List.of(new MarketplaceRepository("acme", "https://old.example.org/repository/hop/"))));

    MarketplaceRepository imported =
        new MarketplaceRepository("acme", "https://forge.example.org/api/packages/acme/maven");
    imported.setBrowserType(MarketplaceRepository.BROWSER_FORGEJO);
    imported.setUrlTemplate(
        "https://forge.example.org/acme/dist/releases/download/v${version}/${artifactId}-${version}.zip");

    MarketplaceRepositoryDefinition.applyToConfig(config, imported, false);

    MarketplaceRepository stored = config.findRepository("acme");
    assertEquals(MarketplaceRepository.BROWSER_FORGEJO, stored.getBrowserType());
    assertEquals(imported.getUrlTemplate(), stored.getUrlTemplate());
  }

  @Test
  void firstImportKeepsBrowserTypeAndTemplate() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setRepositories(new java.util.ArrayList<>());

    MarketplaceRepository imported =
        new MarketplaceRepository("acme", "https://forge.example.org/api/packages/acme/maven");
    imported.setUrlTemplate("https://example.org/${artifactId}-${version}.zip");
    MarketplaceRepositoryDefinition.applyToConfig(config, imported, false);

    assertEquals(
        "https://example.org/${artifactId}-${version}.zip",
        config.findRepository("acme").getUrlTemplate());
  }
}
