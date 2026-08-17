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
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MarketplaceRepositoryDefinitionTest {

  @TempDir Path tempDir;

  @Test
  void exportImportRoundTripOmitsPassword() throws Exception {
    MarketplaceRepository repo = new MarketplaceRepository();
    repo.setId("community");
    repo.setName("Community");
    repo.setUrl("https://repository.example/repository/hop/");
    repo.setBrowse(true);
    repo.setCatalogUrl("https://repository.example/catalog.yaml");
    repo.setIncludeSnapshots(true);
    repo.setGroupIdFilter("com.example");
    repo.setSearchQuery("vault");
    repo.setPassword("secret");

    Path file = tempDir.resolve("def.yaml");
    MarketplaceRepositoryDefinition.save(file, repo);
    String text = Files.readString(file);
    assertFalse(text.contains("secret"));
    assertTrue(text.contains("browse: true"));

    MarketplaceRepository loaded = MarketplaceRepositoryDefinition.load(file);
    assertEquals("community", loaded.getId());
    assertEquals("https://repository.example/repository/hop/", loaded.getUrl());
    assertTrue(loaded.isBrowse());
    assertEquals("https://repository.example/catalog.yaml", loaded.getCatalogUrl());
    assertEquals("com.example", loaded.getGroupIdFilter());
    assertEquals("vault", loaded.getSearchQuery());
    assertNull(loaded.getPassword());
  }

  @Test
  void applyToConfigUpserts() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    MarketplaceRepository first = new MarketplaceRepository("community", "https://a.example/r/");
    first.setBrowse(false);
    config.addRepository(first);

    MarketplaceRepository imported = new MarketplaceRepository();
    imported.setId("community");
    imported.setUrl("https://b.example/r/");
    imported.setBrowse(true);
    imported.setCatalogUrl("https://b.example/c.yaml");
    MarketplaceRepositoryDefinition.applyToConfig(config, imported, false);

    MarketplaceRepository found = config.findRepository("community");
    assertEquals("https://b.example/r/", found.getUrl());
    assertTrue(found.isBrowse());
    assertEquals("https://b.example/c.yaml", found.getCatalogUrl());
  }

  @Test
  void importExportsEmbeddedPlugins() throws Exception {
    Path file = tempDir.resolve("with-plugins.yaml");
    Files.writeString(
        file,
        """
        kind: hop-marketplace-repository
        id: community
        url: https://repository.example/repository/hop/
        browse: true
        plugins:
          - groupId: org.apache.hop
            artifactId: hop-datavault
            version: 0.4.0-SNAPSHOT
            minHopVersion: "2.18.1"
            category: auto-discovered
            description: org/apache/hop/hop-datavault/0.4.0-SNAPSHOT/
            lastUpdated: "2026-07-21T12:00:00.000+00:00"
        """);

    MarketplaceRepository loaded = MarketplaceRepositoryDefinition.load(file);
    assertEquals(1, loaded.getPlugins().size());
    assertEquals("hop-datavault", loaded.getPlugins().get(0).getArtifactId());
    assertEquals("auto-discovered", loaded.getPlugins().get(0).getCategory());
    assertEquals("2.18.1", loaded.getPlugins().get(0).getMinHopVersion());

    Path out = tempDir.resolve("out.yaml");
    MarketplaceRepositoryDefinition.save(out, loaded);
    String saved = Files.readString(out);
    assertTrue(saved.contains("hop-datavault"));
    assertTrue(saved.contains("minHopVersion"));
  }

  @Test
  void applyToConfigMergesPluginsFromSameIdImports() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();

    MarketplaceRepository dataVault = new MarketplaceRepository();
    dataVault.setId("data-hopper-community");
    dataVault.setName("Data Hopper Community Plugins");
    dataVault.setUrl("https://repository.data-hopper.com/repository/hop-community-plugins/");
    dataVault.setBrowse(true);
    dataVault.setPlugins(List.of(plugin("org.apache.hop", "hop-datavault", "0.5.0", "2.18.1")));
    MarketplaceRepositoryDefinition.applyToConfig(config, dataVault, false);

    MarketplaceRepository pentaho = new MarketplaceRepository();
    pentaho.setId("data-hopper-community");
    pentaho.setName("Data Hopper Community Plugins");
    pentaho.setUrl("https://repository.data-hopper.com/repository/hop-community-plugins/");
    pentaho.setBrowse(true);
    pentaho.setPlugins(
        List.of(
            plugin(
                "org.projectdatahopper.hop", "hop-pentaho-reporting-output", "1.0.0", "2.19.0")));
    MarketplaceRepositoryDefinition.applyToConfig(config, pentaho, false);

    MarketplaceRepository found = config.findRepository("data-hopper-community");
    assertEquals(2, found.getPlugins().size());
    assertEquals("hop-datavault", found.getPlugins().get(0).getArtifactId());
    assertEquals("hop-pentaho-reporting-output", found.getPlugins().get(1).getArtifactId());
  }

  @Test
  void applyToConfigReimportUpdatesMatchingPluginKeepsOthers() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();

    MarketplaceRepository first = new MarketplaceRepository();
    first.setId("community");
    first.setUrl("https://example.com/repository/hop/");
    first.setPlugins(
        List.of(
            plugin("org.apache.hop", "hop-datavault", "0.4.0", "2.18.1"),
            plugin("org.example", "other-plugin", "1.0.0", null)));
    MarketplaceRepositoryDefinition.applyToConfig(config, first, false);

    MarketplaceRepository update = new MarketplaceRepository();
    update.setId("community");
    update.setUrl("https://example.com/repository/hop/");
    OptionalPluginInfo refreshed = plugin("org.apache.hop", "hop-datavault", "0.5.0", "2.19.0");
    refreshed.setName("Data Vault");
    update.setPlugins(List.of(refreshed));
    MarketplaceRepositoryDefinition.applyToConfig(config, update, false);

    MarketplaceRepository found = config.findRepository("community");
    assertEquals(2, found.getPlugins().size());
    assertEquals("0.5.0", found.getPlugins().get(0).getVersion());
    assertEquals("2.19.0", found.getPlugins().get(0).getMinHopVersion());
    assertEquals("Data Vault", found.getPlugins().get(0).getName());
    assertEquals("other-plugin", found.getPlugins().get(1).getArtifactId());
  }

  @Test
  void applyToConfigEmptyPluginsDoesNotWipeExisting() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();

    MarketplaceRepository withPlugins = new MarketplaceRepository();
    withPlugins.setId("community");
    withPlugins.setUrl("https://example.com/repository/hop/");
    withPlugins.setPlugins(List.of(plugin("org.apache.hop", "hop-datavault", "0.5.0", null)));
    MarketplaceRepositoryDefinition.applyToConfig(config, withPlugins, false);

    MarketplaceRepository noPlugins = new MarketplaceRepository();
    noPlugins.setId("community");
    noPlugins.setUrl("https://example.com/repository/hop-v2/");
    noPlugins.setBrowse(true);
    noPlugins.setPlugins(List.of());
    MarketplaceRepositoryDefinition.applyToConfig(config, noPlugins, false);

    MarketplaceRepository found = config.findRepository("community");
    assertEquals("https://example.com/repository/hop-v2/", found.getUrl());
    assertTrue(found.isBrowse());
    assertEquals(1, found.getPlugins().size());
    assertEquals("hop-datavault", found.getPlugins().get(0).getArtifactId());
  }

  @Test
  void mergePluginsMatchesArtifactIdWhenGroupIdMissing() {
    OptionalPluginInfo existing = plugin(null, "hop-datavault", "0.4.0", null);
    OptionalPluginInfo incoming = plugin("org.apache.hop", "hop-datavault", "0.5.0", "2.18.1");

    List<OptionalPluginInfo> merged =
        MarketplaceRepositoryDefinition.mergePlugins(List.of(existing), List.of(incoming));

    assertEquals(1, merged.size());
    assertEquals("0.5.0", merged.get(0).getVersion());
    assertEquals("org.apache.hop", merged.get(0).getGroupId());
  }

  private static OptionalPluginInfo plugin(
      String groupId, String artifactId, String version, String minHopVersion) {
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setGroupId(groupId);
    info.setArtifactId(artifactId);
    info.setVersion(version);
    info.setMinHopVersion(minHopVersion);
    info.setName(artifactId);
    return info;
  }
}
