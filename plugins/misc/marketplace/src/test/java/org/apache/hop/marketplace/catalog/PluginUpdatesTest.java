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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.junit.jupiter.api.Test;

class PluginUpdatesTest {

  private static InstallReceipt receipt(String groupId, String version) {
    InstallReceipt receipt = new InstallReceipt();
    receipt.setGroupId(groupId);
    receipt.setArtifactId("hop-tech-test");
    receipt.setVersion(version);
    return receipt;
  }

  private static OptionalPluginInfo info(String groupId, String version, String source) {
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setGroupId(groupId);
    info.setArtifactId("hop-tech-test");
    info.setVersion(version);
    info.setSource(source);
    return info;
  }

  @Test
  void newestDiscoveredVersionWhenItIsNewer() {
    List<OptionalPluginInfo> discovered =
        List.of(
            info("org.apache.hop", "2.20.0", "apache"),
            info("org.apache.hop", "2.21.0", "corp"),
            info("org.apache.hop", "2.21.0-SNAPSHOT", "snapshots"),
            info("com.example", "9.0.0", "corp"));

    assertEquals(
        "2.21.0",
        PluginUpdates.newerVersion(receipt("org.apache.hop", "2.20.0"), discovered).getVersion());
    // A receipt without a group id matches the artifact id in any group.
    assertEquals(
        "9.0.0", PluginUpdates.newerVersion(receipt(null, "2.20.0"), discovered).getVersion());
    assertNull(PluginUpdates.newerVersion(receipt("org.apache.hop", "2.21.0"), discovered));
    assertNull(PluginUpdates.newerVersion(receipt("org.apache.hop", "2.22.0"), discovered));
    assertEquals(
        "9.0.0",
        PluginUpdates.newerVersion(receipt("com.example", "1.0.0"), discovered).getVersion());
  }

  @Test
  void stagedAndUninstalledPluginsHaveNoUpdate() {
    List<OptionalPluginInfo> discovered = List.of(info("org.apache.hop", "3.0.0", "apache"));
    InstallReceipt staged = receipt("org.apache.hop", "2.0.0");
    staged.setPendingActivation(true);
    InstallReceipt uninstalling = receipt("org.apache.hop", "2.0.0");
    uninstalling.setUninstallPending(true);

    assertNull(PluginUpdates.newerVersion(staged, discovered));
    assertNull(PluginUpdates.newerVersion(uninstalling, discovered));
    assertNull(PluginUpdates.newerVersion(null, discovered));
    assertEquals(
        List.of(),
        PluginUpdates.outdated(List.of(staged, receipt("org.apache.hop", "3.0.0")), discovered));
  }

  @Test
  void updatesComeFromWhereDiscoveryFoundThemElseFromWhereThePluginCameFrom() {
    MarketplaceConfig config = new MarketplaceConfig();
    config.getRepositories().clear();
    config.getRepositories().add(new MarketplaceRepository("corp", "https://corp.example/", true));
    config.getRepositories().add(new MarketplaceRepository("asf", "https://asf.example/", false));
    InstallReceipt installed = receipt("org.apache.hop", "1.0.0");
    installed.setRepositoryId("asf");

    assertEquals(
        "corp",
        PluginUpdates.preferredRepository(
            new PluginUpdates.Update(installed, info(null, "2.0.0", "corp")), config));
    assertEquals(
        "asf",
        PluginUpdates.preferredRepository(
            new PluginUpdates.Update(installed, info(null, "2.0.0", "apache")), config));
    installed.setRepositoryId("removed");
    assertNull(
        PluginUpdates.preferredRepository(
            new PluginUpdates.Update(installed, info(null, "2.0.0", "apache")), config));
  }

  @Test
  void aSnapshotIsOnlyAnUpdateForAnInstalledSnapshot() {
    List<OptionalPluginInfo> discovered =
        List.of(
            info("org.apache.hop", "2.20.0", "corp"),
            info("org.apache.hop", "2.21.0-SNAPSHOT", "snapshots"));

    assertNull(PluginUpdates.newerVersion(receipt("org.apache.hop", "2.20.0"), discovered));
    assertEquals(
        "2.21.0-SNAPSHOT",
        PluginUpdates.newerVersion(receipt("org.apache.hop", "2.20.0-SNAPSHOT"), discovered)
            .getVersion());
  }

  @Test
  void anUpdateWithoutAGroupIdKeepsTheInstalledGroupId() {
    MarketplaceConfig config = new MarketplaceConfig();
    InstallReceipt installed = receipt("com.acme", "1.0.0");

    assertEquals(
        new MavenCoordinates("com.acme", "hop-tech-test", "2.0.0"),
        PluginUpdates.coordinates(
            new PluginUpdates.Update(installed, info(null, "2.0.0", "corp")), config));
    assertEquals(
        new MavenCoordinates("org.example", "hop-tech-test", "2.0.0"),
        PluginUpdates.coordinates(
            new PluginUpdates.Update(installed, info("org.example", "2.0.0", "corp")), config));
  }

  @Test
  void aPluginDiscoveryDidNotFindIsNotUpToDate() {
    InstallReceipt installed = receipt("org.apache.hop", "1.0.0");

    assertTrue(
        PluginUpdates.isDiscovered(installed, List.of(info("org.apache.hop", "1.0.0", "corp"))));
    assertFalse(
        PluginUpdates.isDiscovered(installed, List.of(info("com.example", "1.0.0", "corp"))));
    assertFalse(PluginUpdates.isDiscovered(installed, List.of()));
  }
}
