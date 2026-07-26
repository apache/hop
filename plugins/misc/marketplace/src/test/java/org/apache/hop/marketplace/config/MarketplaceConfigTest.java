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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class MarketplaceConfigTest {

  @Test
  void defaultsAreAsfPrimaryThenCentral() {
    MarketplaceConfig config = new MarketplaceConfig();
    assertEquals(2, config.getRepositories().size());
    MarketplaceRepository primary = config.primaryRepository();
    assertEquals(MarketplaceConfig.DEFAULT_ASF_ID, primary.getId());
    assertTrue(primary.isPrimary());
    assertTrue(primary.normalizedUrl().contains("repository.apache.org"));

    List<MarketplaceRepository> ordered = config.orderedRepositories();
    assertEquals(2, ordered.size());
    assertEquals(MarketplaceConfig.DEFAULT_ASF_ID, ordered.get(0).getId());
    assertEquals(MarketplaceConfig.DEFAULT_CENTRAL_ID, ordered.get(1).getId());
  }

  @Test
  void ensureValidPrimaryPicksFirstWhenNoneMarked() {
    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().forEach(r -> r.setPrimary(false));
    config.ensureValidPrimary();
    assertTrue(config.getRepositories().get(0).isPrimary());
    assertFalse(config.getRepositories().get(1).isPrimary());
  }

  @Test
  void setPrimaryClearsOthers() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setPrimary(MarketplaceConfig.DEFAULT_CENTRAL_ID);
    assertTrue(config.findRepository(MarketplaceConfig.DEFAULT_CENTRAL_ID).isPrimary());
    assertFalse(config.findRepository(MarketplaceConfig.DEFAULT_ASF_ID).isPrimary());
    assertEquals(MarketplaceConfig.DEFAULT_CENTRAL_ID, config.orderedRepositories().get(0).getId());
  }

  @Test
  void addAndRemoveRepository() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.addRepository(
        new MarketplaceRepository(
            "local", "Local Nexus", "http://127.0.0.1:8081/repository/hop-plugins/", true));
    assertEquals(3, config.getRepositories().size());
    assertEquals("local", config.primaryRepository().getId());
    config.removeRepository("local");
    assertEquals(2, config.getRepositories().size());
    assertTrue(config.primaryRepository().isPrimary());
  }

  @Test
  void duplicateIdRejected() {
    MarketplaceConfig config = new MarketplaceConfig();
    assertThrows(
        Exception.class,
        () ->
            config.addRepository(
                new MarketplaceRepository(MarketplaceConfig.DEFAULT_ASF_ID, "https://example/")));
  }

  @Test
  void resetToDefaults() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.addRepository(new MarketplaceRepository("x", "http://example.com/repo/", false));
    config.resetToDefaults();
    assertEquals(2, config.getRepositories().size());
    assertEquals(MarketplaceConfig.DEFAULT_ASF_ID, config.primaryRepository().getId());
  }

  @Test
  void disabledReposSkippedInOrder() throws Exception {
    MarketplaceConfig config = new MarketplaceConfig();
    config.setEnabled(MarketplaceConfig.DEFAULT_CENTRAL_ID, false);
    List<MarketplaceRepository> ordered = config.orderedRepositories();
    assertEquals(1, ordered.size());
    assertEquals(MarketplaceConfig.DEFAULT_ASF_ID, ordered.get(0).getId());
  }
}
