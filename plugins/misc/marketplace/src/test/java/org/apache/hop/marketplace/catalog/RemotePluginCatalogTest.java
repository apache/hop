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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RemotePluginCatalogTest {

  @TempDir Path tempDir;

  @Test
  void loadYamlCatalogAndFilter() throws Exception {
    Path file = tempDir.resolve("catalog.yaml");
    Files.writeString(
        file,
        """
        schemaVersion: "1.0"
        kind: hop-marketplace-catalog
        plugins:
          - groupId: org.apache.hop
            artifactId: hop-datavault
            version: 0.4.0-SNAPSHOT
            name: Data Vault
            category: Community
            description: DV transforms
          - groupId: com.example
            artifactId: other
            version: 1.0.0
            name: Other
        """,
        StandardCharsets.UTF_8);

    MarketplaceRepository repo = new MarketplaceRepository();
    repo.setId("community");
    repo.setCatalogUrl(file.toString());
    repo.setGroupIdFilter("org.apache.hop");
    repo.setIncludeSnapshots(true);

    List<OptionalPluginInfo> all = RemotePluginCatalog.load(repo);
    assertEquals(1, all.size());
    assertEquals("hop-datavault", all.get(0).getArtifactId());
    assertEquals("community", all.get(0).getSource());

    List<OptionalPluginInfo> filtered = RemotePluginCatalog.filter(all, repo, "vault");
    assertEquals(1, filtered.size());

    repo.setIncludeSnapshots(false);
    assertTrue(RemotePluginCatalog.filter(all, repo, null).isEmpty());
  }

  @Test
  void nexusUrlParsing() {
    assertEquals(
        "hop-community-plugins",
        NexusRepositoryBrowser.extractRepositoryName(
            "https://repository.data-hopper.com/repository/hop-community-plugins/"));
    assertEquals(
        "https://repository.data-hopper.com",
        NexusRepositoryBrowser.extractNexusBase(
            "https://repository.data-hopper.com/repository/hop-community-plugins/"));
  }
}
