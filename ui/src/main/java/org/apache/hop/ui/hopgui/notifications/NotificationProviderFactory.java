/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use it in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.notifications;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig;
import org.apache.hop.ui.hopgui.notifications.providers.GitHubReleasesNotificationProvider;
import org.apache.hop.ui.hopgui.notifications.providers.RssNotificationProvider;

/**
 * Factory for creating INotificationProvider instances from NotificationSourceConfig. Centralizes
 * provider creation logic so both startup (NotificationSystemInitializer) and hot reload
 * (NotificationService.reloadFromConfig) use the same code path.
 */
class NotificationProviderFactory {

  private NotificationProviderFactory() {
    // Utility class
  }

  /**
   * Create a notification provider from source configuration.
   *
   * @param source Source configuration
   * @param log Log channel for errors
   * @return The provider, or null if creation failed (invalid config, missing fields, etc.)
   */
  static INotificationProvider createProvider(NotificationSourceConfig source, ILogChannel log) {
    if (source == null || source.getId() == null) {
      log.logError("Cannot create provider: source or source ID is null");
      return null;
    }

    if (!source.isEnabled()) {
      return null;
    }

    long pollIntervalMs = parsePollInterval(source.getPollIntervalMinutes(), 60);
    if (pollIntervalMs <= 0) {
      pollIntervalMs = 3600000; // Default 1 hour
    }

    try {
      switch (source.getType()) {
        case GITHUB_RELEASES:
          return createGitHubProvider(source, pollIntervalMs, log);
        case RSS_FEED:
          return createRssProvider(source, pollIntervalMs, log);
        case CUSTOM_PLUGIN:
          return createCustomPluginProvider(source, pollIntervalMs, log);
        default:
          log.logError("Unknown notification source type: " + source.getType());
          return null;
      }
    } catch (Exception e) {
      log.logError(
          "Error creating notification provider for source '"
              + source.getName()
              + "': "
              + e.getMessage(),
          e);
      return null;
    }
  }

  private static long parsePollInterval(String pollIntervalStr, int defaultMinutes) {
    if (pollIntervalStr == null || pollIntervalStr.trim().isEmpty()) {
      return defaultMinutes * 60L * 1000L;
    }
    try {
      int minutes = Integer.parseInt(pollIntervalStr.trim());
      return minutes > 0 ? minutes * 60L * 1000L : defaultMinutes * 60L * 1000L;
    } catch (NumberFormatException e) {
      return defaultMinutes * 60L * 1000L;
    }
  }

  private static INotificationProvider createGitHubProvider(
      NotificationSourceConfig source, long pollIntervalMs, ILogChannel log) {
    String owner = source.getGithubOwner();
    String repo = source.getGithubRepo();
    if (owner == null || owner.isEmpty() || repo == null || repo.isEmpty()) {
      log.logError(
          "GitHub source '" + source.getName() + "' is missing owner or repository configuration");
      return null;
    }
    GitHubReleasesNotificationProvider provider =
        new GitHubReleasesNotificationProvider(owner, repo, source.getId(), source.getName());
    provider.setPollInterval(pollIntervalMs);
    provider.setIncludePreReleases(source.isGithubIncludePrereleases());
    provider.setEnabled(source.isEnabled());
    return provider;
  }

  private static INotificationProvider createRssProvider(
      NotificationSourceConfig source, long pollIntervalMs, ILogChannel log) {
    String url = source.getRssUrl();
    if (url == null || url.isEmpty()) {
      log.logError("RSS source '" + source.getName() + "' is missing URL configuration");
      return null;
    }
    RssNotificationProvider provider =
        new RssNotificationProvider(url, source.getId(), source.getName());
    provider.setPollInterval(pollIntervalMs);
    provider.setEnabled(source.isEnabled());
    return provider;
  }

  private static INotificationProvider createCustomPluginProvider(
      NotificationSourceConfig source, long pollIntervalMs, ILogChannel log) {
    String pluginId = source.getPluginId();
    if (pluginId == null || pluginId.isEmpty()) {
      pluginId = source.getId(); // Fallback to id
    }
    if (pluginId == null || pluginId.isEmpty()) {
      log.logError("Custom plugin source '" + source.getName() + "' is missing plugin ID");
      return null;
    }
    // CUSTOM_PLUGIN: provider must already exist (registered by plugin at startup).
    // We cannot create it here; we only update poll interval on the existing provider.
    // Return null - the reload logic will handle this by updating the existing provider
    // if found, or logging that the plugin has not registered yet.
    return null;
  }
}
