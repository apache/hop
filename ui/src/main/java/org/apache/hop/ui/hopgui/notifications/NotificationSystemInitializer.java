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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.ServerPushSessionFacade;
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
        sources.add(NotificationSourceConfig.defaultHopReleasesSource());

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

      // Providers contributed by plugins, discovered from the registry. They need no entry in
      // the configuration to work; a stored source only records what the user changed about one.
      registerPluginProviders(service, sources, log);

      // Sources the user configured: feeds and repositories.
      for (NotificationSourceConfig source : sources) {
        if (source.getType() == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
          // Contributed by a plugin, handled above.
          continue;
        }
        if (!source.isEnabled()) {
          log.logDetailed("Skipping disabled notification source: " + source.getName());
          continue;
        }
        try {
          INotificationProvider provider = NotificationProviderFactory.createProvider(source, log);
          if (provider != null) {
            provider.initialize();
            service.registerProvider(provider);
            log.logDetailed("Registered provider: " + source.getName());
          }
        } catch (Exception e) {
          log.logError(
              "Error registering notification source '" + source.getName() + "': " + e.getMessage(),
              e);
        }
      }

      // Hop Web only delivers a background thread's asyncExec to the browser while a server push
      // session is running; without one, a poll updates the widgets on the server and the user
      // sees nothing until they click something. An unread indicator that only appears once you
      // go looking is not an indicator. On the desktop this call does nothing.
      ServerPushSessionFacade.start();

      // Start the service. This initializes the providers and schedules polling, which also
      // performs the first fetch shortly after startup. It is deliberately not fetched here:
      // HopGuiStart runs on the UI thread, and talking to every configured source from it would
      // block the GUI from opening for as long as the slowest one takes to answer.
      service.start();

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
    return org.apache.hop.ui.hopgui.notifications.config.NotificationSources.load();
  }

  /**
   * Register the providers that plugins declare, applying whatever the user changed about them.
   *
   * <p>A plugin's provider is enabled with its own defaults until there is a stored source saying
   * otherwise, so installing a plugin is all it takes and nothing is written to the configuration
   * on the user's behalf. A stored source naming a plugin that is no longer installed is left
   * alone: it costs nothing, and the plugin may come back.
   *
   * @param service The service to register with
   * @param sources The configured sources, used as overrides
   * @param log Where to report providers that fail to load
   */
  private void registerPluginProviders(
      NotificationService service, List<NotificationSourceConfig> sources, ILogChannel log) {
    for (IPlugin plugin : NotificationProviderPlugins.plugins()) {
      String pluginId = NotificationProviderPlugins.idOf(plugin);
      if (pluginId == null) {
        continue;
      }
      NotificationSourceConfig override = findSource(sources, pluginId);
      if (override != null && !override.isEnabled()) {
        log.logDetailed("Skipping disabled notification plugin: " + pluginId);
        continue;
      }
      INotificationProvider provider = NotificationProviderPlugins.load(plugin, log);
      if (provider == null) {
        continue;
      }
      try {
        if (override != null && !Utils.isEmpty(override.getPollIntervalMinutes())) {
          provider.setPollInterval(parsePollIntervalMs(override.getPollIntervalMinutes()));
        }
        provider.initialize();
        service.registerProvider(provider);
        log.logDetailed("Registered notification provider from plugin: " + pluginId);
      } catch (Exception e) {
        log.logError("Error registering the notification provider of plugin " + pluginId, e);
      }
    }
  }

  /**
   * Find the stored source for a plugin, by its plugin id or its own id.
   *
   * @param sources The configured sources
   * @param pluginId The plugin to look for
   * @return The source, or null when the plugin has never been configured
   */
  private NotificationSourceConfig findSource(
      List<NotificationSourceConfig> sources, String pluginId) {
    for (NotificationSourceConfig source : sources) {
      if (pluginId.equals(source.getPluginId()) || pluginId.equals(source.getId())) {
        return source;
      }
    }
    return null;
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
