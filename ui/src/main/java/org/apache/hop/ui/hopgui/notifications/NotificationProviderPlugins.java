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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.NotificationProviderPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig;

/**
 * The notification providers contributed by plugins, as found in the plugin registry.
 *
 * <p>A plugin declares a provider with {@link
 * org.apache.hop.core.notifications.NotificationProviderPlugin} and is discovered from its jar. It
 * therefore needs no entry in the configuration to work: a stored source only records what the user
 * changed about it, and a provider whose plugin has been uninstalled simply stops being found.
 */
public final class NotificationProviderPlugins {

  private NotificationProviderPlugins() {
    // Utility class
  }

  /**
   * @return The registry entries of every declared notification provider
   */
  public static List<IPlugin> plugins() {
    List<IPlugin> plugins =
        PluginRegistry.getInstance().getPlugins(NotificationProviderPluginType.class);
    return plugins == null ? new ArrayList<>() : plugins;
  }

  /**
   * @return The identifiers of every declared notification provider
   */
  public static Set<String> ids() {
    Set<String> ids = new LinkedHashSet<>();
    for (IPlugin plugin : plugins()) {
      String id = idOf(plugin);
      if (id != null) {
        ids.add(id);
      }
    }
    return ids;
  }

  /**
   * Instantiate a declared provider.
   *
   * @param plugin The registry entry
   * @param log Where to report a provider that cannot be loaded
   * @return The provider, or null when its class could not be loaded
   */
  public static INotificationProvider load(IPlugin plugin, ILogChannel log) {
    try {
      return PluginRegistry.getInstance().loadClass(plugin, INotificationProvider.class);
    } catch (Exception e) {
      log.logError("Unable to load the notification provider of plugin " + plugin.getName(), e);
      return null;
    }
  }

  /**
   * The identifier a plugin's provider is known by. This is the source id its notifications are
   * qualified with, so it has to be the plugin id and nothing derived from the instance.
   *
   * @param plugin The registry entry
   * @return The identifier, or null when the entry declares none
   */
  public static String idOf(IPlugin plugin) {
    String[] ids = plugin.getIds();
    return ids == null || ids.length == 0 ? null : ids[0];
  }

  /**
   * Describe the declared providers as configuration sources, so the Notifications settings can
   * list a plugin that has never been configured alongside the sources the user added.
   *
   * @return One source per declared provider, in registry order
   */
  public static List<NotificationSourceConfig> describeAsSources() {
    List<NotificationSourceConfig> described = new ArrayList<>();
    for (IPlugin plugin : plugins()) {
      String id = idOf(plugin);
      if (id == null) {
        continue;
      }
      NotificationSourceConfig source = new NotificationSourceConfig();
      source.setId(id);
      source.setPluginId(id);
      source.setName(
          plugin.getName() == null || plugin.getName().isEmpty() ? id : plugin.getName());
      source.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);
      source.setEnabled(true);
      source.setPollIntervalMinutes("60");
      source.setColor(colorFor(id));
      described.add(source);
    }
    return described;
  }

  /**
   * Add a source for every declared provider the configuration does not already mention.
   *
   * <p>A plugin's provider works without being configured, so nothing is written to the
   * configuration when one is installed. It still has to appear in the settings, or there would be
   * no way to turn it off or change how often it polls. A stored source always wins: it is the
   * record of what the user changed.
   *
   * @param sources The sources loaded from the configuration, appended to in place
   */
  public static void addDiscovered(List<NotificationSourceConfig> sources) {
    addMissing(sources, describeAsSources());
  }

  /**
   * @param sources The sources loaded from the configuration, appended to in place
   * @param discovered The sources describing the declared providers
   */
  static void addMissing(
      List<NotificationSourceConfig> sources, List<NotificationSourceConfig> discovered) {
    for (NotificationSourceConfig candidate : discovered) {
      boolean known = false;
      for (NotificationSourceConfig source : sources) {
        if (candidate.getId().equals(source.getId())
            || candidate.getId().equals(source.getPluginId())) {
          known = true;
          break;
        }
      }
      if (!known) {
        sources.add(candidate);
      }
    }
  }

  /**
   * A stable colour for a source indicator, derived from the identifier so a plugin keeps the same
   * colour between runs without having to declare one.
   *
   * @param id The provider identifier
   * @return A hex colour code
   */
  public static String colorFor(String id) {
    int hash = id.hashCode();
    int r = Math.abs(hash % 200) + 50;
    int g = Math.abs((hash >> 8) % 200) + 50;
    int b = Math.abs((hash >> 16) % 200) + 50;
    return String.format("#%02X%02X%02X", r, g, b);
  }
}
