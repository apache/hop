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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig;
import org.junit.jupiter.api.Test;

/** Unit tests for how declared providers meet the stored configuration. */
public class NotificationProviderPluginsTest {

  @Test
  public void testColorIsStableForAnIdentifier() {
    // A plugin keeps the same colour between runs without having to declare one.
    assertEquals(
        NotificationProviderPlugins.colorFor("marketplace-plugin-updates"),
        NotificationProviderPlugins.colorFor("marketplace-plugin-updates"));
  }

  @Test
  public void testColorIsAHexCode() {
    assertTrue(
        NotificationProviderPlugins.colorFor("some-plugin").matches("#[0-9A-F]{6}"),
        NotificationProviderPlugins.colorFor("some-plugin"));
  }

  @Test
  public void testDifferentPluginsGetDifferentColors() {
    assertTrue(
        !NotificationProviderPlugins.colorFor("plugin-a")
            .equals(NotificationProviderPlugins.colorFor("plugin-b")));
  }

  @Test
  public void testDiscoveredPluginIsAddedToTheConfiguredSources() {
    List<NotificationSourceConfig> sources = new ArrayList<>();
    sources.add(source("github-apache-hop", null));

    NotificationProviderPlugins.addMissing(sources, List.of(discovered("my-plugin")));

    assertEquals(2, sources.size());
    assertEquals("my-plugin", sources.get(1).getId());
  }

  @Test
  public void testAlreadyConfiguredPluginIsNotAddedTwice() {
    // The stored source is what the user changed about the plugin; it must win, not be duplicated.
    List<NotificationSourceConfig> sources = new ArrayList<>();
    NotificationSourceConfig stored = source("my-plugin", "my-plugin");
    stored.setEnabled(false);
    sources.add(stored);

    NotificationProviderPlugins.addMissing(sources, List.of(discovered("my-plugin")));

    assertEquals(1, sources.size());
    assertEquals(false, sources.get(0).isEnabled());
  }

  @Test
  public void testPluginMatchedOnPluginIdRatherThanSourceId() {
    List<NotificationSourceConfig> sources = new ArrayList<>();
    sources.add(source("a-renamed-source", "my-plugin"));

    NotificationProviderPlugins.addMissing(sources, List.of(discovered("my-plugin")));

    assertEquals(1, sources.size());
  }

  private NotificationSourceConfig source(String id, String pluginId) {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId(id);
    source.setName(id);
    source.setEnabled(true);
    source.setType(
        pluginId == null
            ? NotificationSourceConfig.SourceType.GITHUB_RELEASES
            : NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);
    if (pluginId != null) {
      source.setPluginId(pluginId);
    }
    return source;
  }

  private NotificationSourceConfig discovered(String pluginId) {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId(pluginId);
    source.setPluginId(pluginId);
    source.setName(pluginId);
    source.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);
    source.setEnabled(true);
    return source;
  }
}
