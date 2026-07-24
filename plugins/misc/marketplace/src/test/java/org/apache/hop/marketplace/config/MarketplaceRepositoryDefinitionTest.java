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
}
