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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.junit.jupiter.api.Test;

class NexusRepositoryBrowserTest {

  @Test
  void asfPublicGroupIsNotNexusBrowseUrl() {
    assertFalse(
        NexusRepositoryBrowser.isNexusBrowseUrl(
            "https://repository.apache.org/content/groups/public/"));
    assertFalse(NexusRepositoryBrowser.isNexusBrowseUrl("https://repo1.maven.org/maven2/"));
  }

  @Test
  void nexusRepositoryPathIsBrowsable() {
    assertTrue(
        NexusRepositoryBrowser.isNexusBrowseUrl(
            "https://repository.data-hopper.com/repository/hop-community-plugins/"));
    assertEquals(
        "hop-community-plugins",
        NexusRepositoryBrowser.extractRepositoryName(
            "https://repository.data-hopper.com/repository/hop-community-plugins/"));
  }

  @Test
  void browseAsfReturnsEmptyWithoutThrowing() throws Exception {
    MarketplaceRepository asf =
        new MarketplaceRepository("asf", "https://repository.apache.org/content/groups/public/");
    asf.setBrowse(true);
    assertTrue(NexusRepositoryBrowser.browse(asf, null, null).isEmpty());
  }
}
