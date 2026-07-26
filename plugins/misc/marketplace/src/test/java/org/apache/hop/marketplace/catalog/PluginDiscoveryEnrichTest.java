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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.junit.jupiter.api.Test;

class PluginDiscoveryEnrichTest {

  @Test
  void enrichCategoryAndNameFromYamlWithoutGroupId() {
    OptionalPluginInfo live = new OptionalPluginInfo();
    live.setGroupId("org.apache.hop");
    live.setArtifactId("hop-datavault");
    live.setVersion("0.4.0-SNAPSHOT");
    live.setName("hop-datavault");
    live.setCategory("auto-discovered");
    live.setDescription("org/apache/hop/hop-datavault/0.4.0-SNAPSHOT/...");

    OptionalPluginInfo meta = new OptionalPluginInfo();
    // no groupId — common in hand-written YAML
    meta.setArtifactId("hop-datavault");
    meta.setName("Data Vault");
    meta.setCategory("Community");
    meta.setDescription("Data Vault transforms for Hop");
    meta.setMinHopVersion("2.18.1");
    meta.setVersion("0.4.0-SNAPSHOT");

    MarketplaceRepository repo = new MarketplaceRepository("community", "https://example/repo/");
    repo.setPlugins(new ArrayList<>(List.of(meta)));

    List<OptionalPluginInfo> out =
        PluginDiscovery.enrichWithDefinitionMetadata(List.of(live), repo);
    assertEquals(1, out.size());
    assertEquals("Community", out.get(0).getCategory());
    assertEquals("Data Vault", out.get(0).getName());
    assertEquals("Data Vault transforms for Hop", out.get(0).getDescription());
    assertEquals("2.18.1", out.get(0).getMinHopVersion());
  }

  @Test
  void enrichMatchesOnFullGa() {
    OptionalPluginInfo live = new OptionalPluginInfo();
    live.setGroupId("org.apache.hop");
    live.setArtifactId("hop-datavault");
    live.setCategory("auto-discovered");
    live.setName("hop-datavault");

    OptionalPluginInfo meta = new OptionalPluginInfo();
    meta.setGroupId("org.apache.hop");
    meta.setArtifactId("hop-datavault");
    meta.setCategory("Community");
    meta.setName("Data Vault");

    MarketplaceRepository repo = new MarketplaceRepository("c", "https://x/");
    repo.setPlugins(List.of(meta));

    PluginDiscovery.enrichWithDefinitionMetadata(List.of(live), repo);
    assertEquals("Community", live.getCategory());
    assertEquals("Data Vault", live.getName());
  }
}
