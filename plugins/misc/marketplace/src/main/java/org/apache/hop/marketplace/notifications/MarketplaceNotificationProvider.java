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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;
import org.apache.hop.core.notifications.NotificationProviderPlugin;
import org.apache.hop.core.util.VersionCompare;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.catalog.PluginDiscovery;
import org.apache.hop.marketplace.catalog.VersionCompat;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;

/**
 * Reports installed marketplace plugins that have a newer version available.
 *
 * <p>Everything this needs is already known to the marketplace: the configured repositories say
 * what is on offer and at which version, and the install receipt under {@code plugins/.marketplace}
 * says what is installed and where it came from. Comparing the two is the whole job, so a user does
 * not have to configure a release feed per plugin - and, because discovery goes through the
 * marketplace, private repositories are read with the credentials that are already set up for them.
 */
@NotificationProviderPlugin(
    id = "marketplace-plugin-updates",
    name = "i18n::MarketplaceNotifications.Name",
    description = "i18n::MarketplaceNotifications.Description")
public class MarketplaceNotificationProvider implements INotificationProvider {

  private static final Class<?> PKG = MarketplaceNotificationProvider.class;

  private static final long DEFAULT_POLL_INTERVAL_MS = 6 * 60 * 60 * 1000L;

  private final ILogChannel log = new LogChannel("MarketplaceNotifications");

  private boolean enabled = true;
  private long pollInterval = DEFAULT_POLL_INTERVAL_MS;

  /**
   * How an install receipt is read. A seam rather than a direct static call, so the comparison this
   * class exists to do can be tested without an installation on disk.
   */
  private ReceiptReader receiptReader = PluginInstaller::readReceipt;

  /** Reads the install receipt of one plugin. */
  @FunctionalInterface
  interface ReceiptReader {
    InstallReceipt read(Path hopHome, String artifactId) throws Exception;
  }

  void setReceiptReaderForTesting(ReceiptReader reader) {
    this.receiptReader = reader == null ? PluginInstaller::readReceipt : reader;
  }

  @Override
  public String getId() {
    return "marketplace-plugin-updates";
  }

  @Override
  public String getName() {
    return BaseMessages.getString(PKG, "MarketplaceNotifications.Name");
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, "MarketplaceNotifications.Description");
  }

  @Override
  public List<Notification> fetchNotifications() throws HopException {
    MarketplaceConfig config = MarketplaceConfig.load();
    if (config == null || !config.isEnabled()) {
      return new ArrayList<>();
    }

    Path hopHome = HopHome.resolve();
    List<Notification> notifications = new ArrayList<>();

    // An empty filter asks for the whole catalog of every enabled repository. This reaches the
    // network, which is fine: providers are polled on a background thread.
    for (OptionalPluginInfo available : PluginDiscovery.query("", null, config, log)) {
      if (available == null || isEmpty(available.getArtifactId())) {
        continue;
      }
      Notification notification = updateNotification(available, hopHome, config);
      if (notification != null) {
        notifications.add(notification);
      }
    }
    return notifications;
  }

  /**
   * Build the notification for one catalog entry, if the installed copy is behind it.
   *
   * @param available The plugin as the catalog offers it
   * @param hopHome The Hop installation to read the install receipt from
   * @param config The marketplace configuration, used to name the repository
   * @return A notification, or null when the plugin is not installed or is already current
   */
  Notification updateNotification(
      OptionalPluginInfo available, Path hopHome, MarketplaceConfig config) {
    InstallReceipt receipt;
    try {
      receipt = receiptReader.read(hopHome, available.getArtifactId());
    } catch (Exception e) {
      // A receipt we cannot read tells us nothing about that plugin; the others still count.
      log.logDetailed(
          "Unable to read the install receipt of "
              + available.getArtifactId()
              + ": "
              + e.getMessage());
      return null;
    }
    if (receipt == null) {
      // Not installed through the marketplace, so there is no version to compare against and
      // nothing the user asked to be kept up to date.
      return null;
    }

    String installedVersion = receipt.getVersion();
    String availableVersion = available.getVersion();
    if (isEmpty(installedVersion)
        || isEmpty(availableVersion)
        || VersionCompat.compare(availableVersion, installedVersion) <= 0) {
      return null;
    }
    // The Apache catalog carries no versions, so discovery fills in the running Hop version. On a
    // development build that is a snapshot, and telling somebody a snapshot is "available" points
    // them at an artifact the release repository does not have. Someone already running a snapshot
    // has a snapshot repository configured and is the exception.
    if (VersionCompare.isSnapshot(availableVersion)
        && !VersionCompare.isSnapshot(installedVersion)) {
      log.logDetailed(
          "Not reporting "
              + available.getArtifactId()
              + " "
              + availableVersion
              + ": a snapshot is not a published version");
      return null;
    }

    String name = isEmpty(available.getName()) ? available.getArtifactId() : available.getName();

    Notification notification = new Notification();
    // Identifies the update rather than the plugin, so upgrading and a later update are two
    // separate notifications and dismissing one does not hide the next.
    notification.setId(available.getArtifactId() + "-" + availableVersion);
    notification.setTitle(
        BaseMessages.getString(PKG, "MarketplaceNotifications.Title", name, availableVersion));
    // The marketplace installs over an existing version rather than replacing it, which leaves
    // both jars on the plugin classpath. Until that is fixed, say what actually has to be done.
    notification.setMessage(
        BaseMessages.getString(
            PKG,
            "MarketplaceNotifications.Message",
            installedVersion,
            repositorySuffix(available, config)));
    notification.setSource(getName());
    notification.setSourceId(getId());
    notification.setLink(homepage(available, config));
    notification.setTimestamp(new Date());
    notification.setPriority(NotificationPriority.INFO);
    notification.setCategory(NotificationCategory.PLUGIN);
    notification.setVersion(availableVersion);
    notification.getMetadata().put("groupId", available.getGroupId());
    notification.getMetadata().put("artifactId", available.getArtifactId());
    notification.getMetadata().put("installedVersion", installedVersion);
    return notification;
  }

  private String repositorySuffix(OptionalPluginInfo available, MarketplaceConfig config) {
    MarketplaceRepository repository = repositoryOf(available, config);
    if (repository == null || isEmpty(repository.getName())) {
      return "";
    }
    return BaseMessages.getString(
        PKG, "MarketplaceNotifications.Message.Repository", repository.getName());
  }

  /**
   * The page to open when the notification is clicked. The repository's own homepage is the only
   * link the marketplace knows; without one the notification simply has none, which the panel
   * handles.
   */
  private String homepage(OptionalPluginInfo available, MarketplaceConfig config) {
    MarketplaceRepository repository = repositoryOf(available, config);
    return repository == null ? null : repository.getHomepage();
  }

  private MarketplaceRepository repositoryOf(
      OptionalPluginInfo available, MarketplaceConfig config) {
    if (isEmpty(available.getSource()) || config.getRepositories() == null) {
      return null;
    }
    for (MarketplaceRepository repository : config.getRepositories()) {
      if (repository != null && available.getSource().equals(repository.getId())) {
        return repository;
      }
    }
    return null;
  }

  private static boolean isEmpty(String value) {
    return value == null || value.trim().isEmpty();
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public long getPollInterval() {
    return pollInterval;
  }

  @Override
  public void setPollInterval(long interval) {
    this.pollInterval = interval;
  }

  @Override
  public void initialize() {
    // The marketplace configuration is read on every fetch, so there is nothing to set up.
  }

  @Override
  public void shutdown() {
    // Nothing to clean up
  }
}
