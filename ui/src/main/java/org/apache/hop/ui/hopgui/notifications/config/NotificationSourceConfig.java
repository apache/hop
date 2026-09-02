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

package org.apache.hop.ui.hopgui.notifications.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.i18n.BaseMessages;

/**
 * Configuration for a single notification source. This represents one provider (GitHub, RSS, or
 * custom plugin) that can be configured by the user.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationSourceConfig {

  public enum SourceType {
    GITHUB_RELEASES,
    RSS_FEED,
    CUSTOM_PLUGIN;

    public String getDisplayName() {
      return BaseMessages.getString(
          NotificationSourceConfig.class, "NotificationSourceConfig.SourceType." + name());
    }
  }

  private String id; // Unique identifier (e.g., "github-apache-hop", "rss-feed-1")
  private String name; // Display name (e.g., "Apache Hop Releases")
  private SourceType type; // Type of source
  private boolean enabled; // Whether this source is enabled
  private String color; // Hex color code for the indicator (e.g., "#FF5733")
  private Map<String, String> properties; // Type-specific properties

  public NotificationSourceConfig() {
    this.properties = new HashMap<>();
    this.enabled = true;
  }

  public NotificationSourceConfig(String id, String name, SourceType type) {
    this();
    this.id = id;
    this.name = name;
    this.type = type;
  }

  // Getters and setters
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public SourceType getType() {
    return type;
  }

  public void setType(SourceType type) {
    this.type = type;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getColor() {
    return color;
  }

  public void setColor(String color) {
    this.color = color;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  // Convenience methods for common properties
  public String getProperty(String key) {
    return properties != null ? properties.get(key) : null;
  }

  public void setProperty(String key, String value) {
    if (properties == null) {
      properties = new HashMap<>();
    }
    properties.put(key, value);
  }

  // Type-specific getters
  public String getGithubOwner() {
    return getProperty("github.owner");
  }

  public void setGithubOwner(String owner) {
    setProperty("github.owner", owner);
  }

  public String getGithubRepo() {
    return getProperty("github.repo");
  }

  public void setGithubRepo(String repo) {
    setProperty("github.repo", repo);
  }

  public boolean isGithubIncludePrereleases() {
    String value = getProperty("github.includePrereleases");
    return value != null && value.equalsIgnoreCase("true");
  }

  public void setGithubIncludePrereleases(boolean include) {
    setProperty("github.includePrereleases", String.valueOf(include));
  }

  public String getRssUrl() {
    return getProperty("rss.url");
  }

  public void setRssUrl(String url) {
    setProperty("rss.url", url);
  }

  /**
   * Get the plugin ID for CUSTOM_PLUGIN sources. Falls back to {@link #getId()} when plugin.id is
   * not set in properties (e.g. after JSON load from older config or manual edit), so providers can
   * be wired correctly.
   */
  public String getPluginId() {
    String pluginId = getProperty("plugin.id");
    if (pluginId != null && !pluginId.isEmpty()) {
      return pluginId;
    }
    return getId(); // Fallback: id and pluginId are the same for PluginHelper-created sources
  }

  public void setPluginId(String pluginId) {
    setProperty("plugin.id", pluginId);
  }

  public String getPollIntervalMinutes() {
    return getProperty("poll.intervalMinutes");
  }

  public void setPollIntervalMinutes(String minutes) {
    setProperty("poll.intervalMinutes", minutes);
  }

  public String getDaysToGoBack() {
    return getProperty("daysToGoBack");
  }

  public void setDaysToGoBack(String days) {
    setProperty("daysToGoBack", days);
  }

  /**
   * Generate a display string for the "Details" column based on source type
   *
   * @return Details string
   */
  public String getDetailsDisplay() {
    switch (type) {
      case GITHUB_RELEASES:
        String owner = getGithubOwner();
        String repo = getGithubRepo();
        if (owner != null && repo != null) {
          return owner + "/" + repo;
        }
        return "";
      case RSS_FEED:
        String url = getRssUrl();
        return url != null ? url : "";
      case CUSTOM_PLUGIN:
        String pluginId = getPluginId();
        return pluginId != null ? pluginId : "";
      default:
        return "";
    }
  }
}
