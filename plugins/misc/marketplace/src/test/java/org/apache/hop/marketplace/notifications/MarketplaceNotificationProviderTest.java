/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.marketplace.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the comparison of installed plugin versions against the catalog. */
public class MarketplaceNotificationProviderTest {

  private static final Path HOP_HOME = Paths.get("/opt/hop");

  private MarketplaceNotificationProvider provider;
  private MarketplaceConfig config;

  @BeforeEach
  public void setUp() {
    provider = new MarketplaceNotificationProvider();
    config = new MarketplaceConfig();
    MarketplaceRepository repository =
        new MarketplaceRepository("putki", "Putki Marketplace", "https://example.com/repo", true);
    repository.setHomepage("https://example.com");
    config.setRepositories(List.of(repository));
  }

  @Test
  public void testNewerVersionIsReported() {
    installed("2026.09");

    Notification notification =
        provider.updateNotification(
            available("putki-hubspot", "HubSpot", "2026.10"), HOP_HOME, config);

    assertNotNull(notification);
    assertEquals("HubSpot 2026.10 is available", notification.getTitle());
    assertTrue(notification.getMessage().contains("2026.09"));
    assertTrue(notification.getMessage().contains("Putki Marketplace"));
    // The marketplace overlays rather than replaces, so the advice has to say uninstall first.
    assertTrue(notification.getMessage().toLowerCase().contains("uninstall"));
    assertEquals(NotificationCategory.PLUGIN, notification.getCategory());
    assertEquals("2026.10", notification.getVersion());
    assertEquals("https://example.com", notification.getLink());
    assertEquals("2026.09", notification.getMetadata().get("installedVersion"));
  }

  @Test
  public void testSameVersionIsNotReported() {
    installed("2026.10");

    assertNull(
        provider.updateNotification(
            available("putki-hubspot", "HubSpot", "2026.10"), HOP_HOME, config));
  }

  @Test
  public void testOlderCatalogVersionIsNotReported() {
    // A repository that has fallen behind must not offer a downgrade as an update.
    installed("2026.10");

    assertNull(
        provider.updateNotification(
            available("putki-hubspot", "HubSpot", "2026.09"), HOP_HOME, config));
  }

  @Test
  public void testPluginThatIsNotInstalledIsNotReported() {
    provider.setReceiptReaderForTesting((home, artifactId) -> null);

    assertNull(
        provider.updateNotification(
            available("putki-hubspot", "HubSpot", "2026.10"), HOP_HOME, config));
  }

  @Test
  public void testUnreadableReceiptIsSkippedRatherThanFailing() {
    provider.setReceiptReaderForTesting(
        (home, artifactId) -> {
          throw new IllegalStateException("receipt is corrupt");
        });

    assertNull(
        provider.updateNotification(
            available("putki-hubspot", "HubSpot", "2026.10"), HOP_HOME, config));
  }

  @Test
  public void testIdIdentifiesTheUpdateAndIsStable() {
    installed("2026.09");
    OptionalPluginInfo available = available("putki-hubspot", "HubSpot", "2026.10");

    String first = provider.updateNotification(available, HOP_HOME, config).getId();
    String second = provider.updateNotification(available, HOP_HOME, config).getId();

    assertEquals(first, second);
    assertEquals("putki-hubspot-2026.10", first);
  }

  @Test
  public void testArtifactIdIsUsedWhenTheCatalogHasNoName() {
    installed("1.0");

    Notification notification =
        provider.updateNotification(available("putki-hubspot", null, "2.0"), HOP_HOME, config);

    assertEquals("putki-hubspot 2.0 is available", notification.getTitle());
  }

  @Test
  public void testUnknownRepositoryLeavesTheNotificationWithoutALink() {
    installed("1.0");
    OptionalPluginInfo available = available("putki-hubspot", "HubSpot", "2.0");
    available.setSource("a-repository-that-is-not-configured");

    Notification notification = provider.updateNotification(available, HOP_HOME, config);

    assertNull(notification.getLink());
  }

  @Test
  public void testSnapshotIsNotAdvertisedAsAvailable() {
    // The Apache catalog carries no versions, so discovery fills in the running Hop version. On a
    // development build that is a snapshot, and no release repository has it to install.
    installed("2.19.0");

    assertNull(
        provider.updateNotification(
            available("hop-misc-documentation", "Documentation", "2.20.0-SNAPSHOT"),
            HOP_HOME,
            config));
  }

  @Test
  public void testSnapshotIsStillReportedToSomeoneAlreadyRunningOne() {
    // Someone on a snapshot has a snapshot repository configured; they are the exception.
    installed("2.19.0-SNAPSHOT");

    assertNotNull(
        provider.updateNotification(
            available("hop-misc-documentation", "Documentation", "2.20.0-SNAPSHOT"),
            HOP_HOME,
            config));
  }

  private void installed(String version) {
    provider.setReceiptReaderForTesting(
        (home, artifactId) -> {
          InstallReceipt receipt = new InstallReceipt();
          receipt.setGroupId("io.putki");
          receipt.setArtifactId(artifactId);
          receipt.setVersion(version);
          return receipt;
        });
  }

  private OptionalPluginInfo available(String artifactId, String name, String version) {
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setGroupId("io.putki");
    info.setArtifactId(artifactId);
    info.setName(name);
    info.setVersion(version);
    info.setSource("putki");
    return info;
  }
}
