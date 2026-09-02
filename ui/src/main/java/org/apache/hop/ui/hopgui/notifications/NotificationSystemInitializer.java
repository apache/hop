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

package org.apache.hop.ui.hopgui.notifications;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig;
import org.eclipse.swt.widgets.Display;

/**
 * Initializes the notification system when Hop GUI starts. This class registers providers and
 * starts the notification service.
 */
@ExtensionPoint(
    id = "NotificationSystemInitializer",
    extensionPointId = "HopGuiStart",
    description = "Initialize the notification system")
public class NotificationSystemInitializer implements IExtensionPoint<Object> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object)
      throws HopException {
    try {
      NotificationService service = NotificationService.getInstance();

      // Register RSS/Atom feed providers
      // DISABLED: Using GitHub API provider instead (better content and filtering)
      // org.apache.hop.ui.hopgui.notifications.providers.RssNotificationProvider apacheHopFeed =
      //     new org.apache.hop.ui.hopgui.notifications.providers.RssNotificationProvider(
      //         "https://github.com/apache/hop/releases.atom",
      //         "rss-apache-hop-releases",
      //         "Apache Hop Releases");
      // apacheHopFeed.setPollInterval(3600000); // 1 hour
      // service.registerProvider(apacheHopFeed);

      // Knowbi Putki releases feed - DISABLED: URL returns 404
      // org.apache.hop.ui.hopgui.notifications.providers.RssNotificationProvider knowbiFeed =
      //     new org.apache.hop.ui.hopgui.notifications.providers.RssNotificationProvider(
      //         "https://github.com/knowbi/knowbi-putki/releases.atom",
      //         "rss-knowbi-putki-releases",
      //         "Knowbi Putki Releases");
      // knowbiFeed.setPollInterval(3600000); // 1 hour
      // service.registerProvider(knowbiFeed);

      // Load notification sources from config
      boolean notificationsEnabled =
          HopConfig.readOptionString("notification.system.enabled", "true")
              .equalsIgnoreCase("true");

      if (!notificationsEnabled) {
        log.logDetailed("Notification system is disabled in configuration");
        return; // Don't start the service if disabled
      }

      // Load sources from config
      List<NotificationSourceConfig> sources = loadNotificationSources();

      if (sources.isEmpty()) {
        // If no sources configured, create a default one for backward compatibility
        log.logDetailed("No notification sources configured, creating default Apache Hop source");
        NotificationSourceConfig defaultSource = new NotificationSourceConfig();
        defaultSource.setId("github-apache-hop");
        defaultSource.setName("Apache Hop Releases");
        defaultSource.setType(NotificationSourceConfig.SourceType.GITHUB_RELEASES);
        defaultSource.setEnabled(true);
        defaultSource.setGithubOwner("apache");
        defaultSource.setGithubRepo("hop");
        defaultSource.setGithubIncludePrereleases(false);
        defaultSource.setPollIntervalMinutes("60");
        defaultSource.setColor("#FF5733"); // Default color
        sources.add(defaultSource);

        // Save the default source to config so it appears in the configuration UI
        try {
          ObjectMapper mapper = JsonUtil.jsonMapper();
          String sourcesJson = mapper.writeValueAsString(sources);
          HopConfig.getInstance().saveOption("notification.sources", sourcesJson);
          HopConfig.getInstance().saveToFile();
          log.logDetailed("Saved default notification source to configuration");
        } catch (Exception e) {
          log.logError("Error saving default notification source to config", e);
        }
      }

      // Register providers for each enabled source (shared logic with hot reload)
      for (NotificationSourceConfig source : sources) {
        if (!source.isEnabled()) {
          log.logDetailed("Skipping disabled notification source: " + source.getName());
          continue;
        }

        try {
          if (source.getType() == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
            updateOrWarnCustomPluginProvider(service, source, log);
          } else {
            INotificationProvider provider =
                NotificationProviderFactory.createProvider(source, log);
            if (provider != null) {
              provider.initialize();
              service.registerProvider(provider);
              log.logDetailed("Registered provider: " + source.getName());
            }
          }
        } catch (Exception e) {
          log.logError(
              "Error registering notification source '" + source.getName() + "': " + e.getMessage(),
              e);
        }
      }

      // Start the service (this will initialize providers and start polling)
      service.start();

      // Fetch initial notifications immediately
      service.fetchFromProviders();

      // Initialize badge manager with a delay to ensure toolbar is ready
      Display.getCurrent()
          .asyncExec(
              () -> {
                Display.getCurrent()
                    .timerExec(
                        500,
                        () -> {
                          NotificationBadgeManager badgeManager =
                              NotificationBadgeManager.getInstance();
                          badgeManager.initialize();
                        });
              });

      log.logDetailed("Notification system initialized with " + sources.size() + " source(s)");
    } catch (Exception e) {
      log.logError("Error initializing notification system", e);
    }
  }

  /**
   * Load notification sources from HopConfig
   *
   * @return List of notification source configurations
   */
  private List<NotificationSourceConfig> loadNotificationSources() {
    try {
      String sourcesJson = HopConfig.readOptionString("notification.sources", null);
      if (!Utils.isEmpty(sourcesJson)) {
        ObjectMapper mapper = JsonUtil.jsonMapper();
        return mapper.readValue(
            sourcesJson, new TypeReference<List<NotificationSourceConfig>>() {});
      }
    } catch (Exception e) {
      // If loading fails, return empty list
    }
    return new java.util.ArrayList<>();
  }

  /**
   * Handle CUSTOM_PLUGIN source: update existing provider if found, else log. Custom plugins
   * register their providers at startup; we only update poll interval.
   */
  private void updateOrWarnCustomPluginProvider(
      NotificationService service, NotificationSourceConfig source, ILogChannel log) {
    String pluginId = source.getPluginId();
    if (pluginId == null || pluginId.isEmpty()) {
      pluginId = source.getId();
    }
    if (pluginId == null || pluginId.isEmpty()) {
      log.logError("Custom plugin source '" + source.getName() + "' is missing plugin ID");
      return;
    }
    INotificationProvider existing = service.getProvider(pluginId);
    if (existing != null) {
      long pollIntervalMs = parsePollIntervalMs(source.getPollIntervalMinutes());
      existing.setPollInterval(pollIntervalMs);
      // Provider already registered; start() will schedule with updated interval
      log.logDetailed(
          "Updated poll interval for custom plugin provider '"
              + pluginId
              + "' to "
              + (pollIntervalMs / 60000)
              + " minutes");
    } else {
      log.logDetailed(
          "Custom plugin provider '"
              + pluginId
              + "' not found. "
              + "Plugins should register their INotificationProvider via "
              + "NotificationService.getInstance().registerProvider() during initialization.");
    }
  }

  private long parsePollIntervalMs(String value) {
    if (value == null || value.trim().isEmpty()) {
      return 3600000;
    }
    try {
      int minutes = Integer.parseInt(value.trim());
      return minutes > 0 ? minutes * 60L * 1000L : 3600000;
    } catch (NumberFormatException e) {
      return 3600000;
    }
  }
}
