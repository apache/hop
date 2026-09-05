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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.i18n.BaseMessages;

/**
 * Configuration for a single notification source. This represents one provider (GitHub, RSS, or
 * custom plugin) that can be configured by the user.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationSourceConfig {

  /** Used when Hop's own encoder has not been initialised, so a password is never stored bare. */
  private static final ITwoWayPasswordEncoder FALLBACK = new HopTwoWayPasswordEncoder();

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
  @JsonIgnore
  public String getProperty(String key) {
    return properties != null ? properties.get(key) : null;
  }

  @JsonIgnore
  public void setProperty(String key, String value) {
    if (properties == null) {
      properties = new HashMap<>();
    }
    properties.put(key, value);
  }

  // Type-specific getters
  @JsonIgnore
  public String getGithubOwner() {
    return getProperty("github.owner");
  }

  @JsonIgnore
  public void setGithubOwner(String owner) {
    setProperty("github.owner", owner);
  }

  @JsonIgnore
  public String getGithubRepo() {
    return getProperty("github.repo");
  }

  @JsonIgnore
  public void setGithubRepo(String repo) {
    setProperty("github.repo", repo);
  }

  @JsonIgnore
  public boolean isGithubIncludePrereleases() {
    String value = getProperty("github.includePrereleases");
    return value != null && value.equalsIgnoreCase("true");
  }

  @JsonIgnore
  public void setGithubIncludePrereleases(boolean include) {
    setProperty("github.includePrereleases", String.valueOf(include));
  }

  @JsonIgnore
  public String getRssUrl() {
    return getProperty("rss.url");
  }

  @JsonIgnore
  public void setRssUrl(String url) {
    setProperty("rss.url", url);
  }

  /**
   * Get the plugin ID for CUSTOM_PLUGIN sources. Falls back to {@link #getId()} when plugin.id is
   * not set in properties (e.g. after JSON load from older config or manual edit), so providers can
   * be wired correctly.
   */
  @JsonIgnore
  public String getPluginId() {
    String pluginId = getProperty("plugin.id");
    if (pluginId != null && !pluginId.isEmpty()) {
      return pluginId;
    }
    return getId(); // Fallback: id and pluginId are the same for PluginHelper-created sources
  }

  @JsonIgnore
  public void setPluginId(String pluginId) {
    setProperty("plugin.id", pluginId);
  }

  /**
   * The user name sent to the source, or null for anonymous access.
   *
   * <p>Held as written, so a variable or a variable resolver expression stays a reference in the
   * configuration file and is only resolved when a request is made.
   *
   * @return The configured user name
   */
  @JsonIgnore
  public String getUsername() {
    return getProperty("auth.username");
  }

  @JsonIgnore
  public void setUsername(String username) {
    setProperty("auth.username", username);
  }

  /**
   * The password or token sent to the source.
   *
   * <p>For GitHub this is a personal access token, which also lifts the rate limit: unauthenticated
   * requests are counted per IP address and shared between everyone behind it, while authenticated
   * ones are counted per token. Prefer a variable over a literal here, so the token does not end up
   * in the configuration file.
   *
   * @return The configured password or token
   */
  @JsonIgnore
  public String getPassword() {
    String stored = getProperty("auth.password");
    if (stored == null || stored.isEmpty()) {
      return stored;
    }
    if (!stored.startsWith(Encr.PASSWORD_ENCRYPTED_PREFIX)) {
      return stored; // A variable, or a value written before this was obfuscated.
    }
    return encoder()
        .decode(stored.substring(Encr.PASSWORD_ENCRYPTED_PREFIX.length()).trim(), false);
  }

  @JsonIgnore
  public void setPassword(String password) {
    // Obfuscated on the way in, the way every other Hop password in a config file is, and left
    // alone when it is a variable so the reference survives. Obfuscation is not encryption: a
    // variable pointing at a real secret store is still the better answer for a private source.
    if (password == null || password.isEmpty() || password.contains("${")) {
      setProperty("auth.password", password);
      return;
    }
    setProperty(
        "auth.password", Encr.PASSWORD_ENCRYPTED_PREFIX + encoder().encode(password, false));
  }

  /**
   * The password encoder, or a plain one when Hop's has not been initialised.
   *
   * <p>{@link Encr} is only wired up by a full client environment. Falling back keeps a source
   * usable from a context that never called {@code Encr.init}, rather than failing on a field the
   * user may not even have filled in.
   *
   * @return An encoder, never null
   */
  private static ITwoWayPasswordEncoder encoder() {
    ITwoWayPasswordEncoder active = Encr.getEncoder();
    return active == null ? FALLBACK : active;
  }

  /**
   * @return Whether this source has credentials configured
   */
  @JsonIgnore
  public boolean hasCredentials() {
    String password = getPassword();
    return password != null && !password.trim().isEmpty();
  }

  /**
   * Only report releases newer than this, or null to report every release the source offers.
   *
   * <p>A release feed has no idea what you are running, so without a floor it reports a
   * repository's whole history - Apache Hop alone has more than twenty releases going back years.
   * The floor is what turns a feed into "tell me when there is something newer than what I have".
   *
   * @return The configured floor
   */
  @JsonIgnore
  public String getMinimumVersion() {
    return getProperty("version.minimum");
  }

  @JsonIgnore
  public void setMinimumVersion(String minimumVersion) {
    setProperty("version.minimum", minimumVersion);
  }

  /**
   * The source seeded when nothing is configured: Apache Hop's own releases, floored at the version
   * of Hop that is running, so a new installation hears about newer releases and not the back
   * catalogue.
   *
   * @return A new source configuration
   */
  public static NotificationSourceConfig defaultHopReleasesSource() {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("github-apache-hop");
    source.setName("Apache Hop Releases");
    source.setType(SourceType.GITHUB_RELEASES);
    source.setEnabled(true);
    source.setGithubOwner("apache");
    source.setGithubRepo("hop");
    source.setGithubIncludePrereleases(false);
    source.setPollIntervalMinutes("60");
    source.setColor("#FF5733");
    source.setMinimumVersion(runningHopVersion());
    return source;
  }

  /**
   * The version of Hop that is running, as the jar manifest reports it.
   *
   * @return The version, or null when it cannot be determined (a development classpath has no
   *     manifest, and a floor we cannot establish is better left unset than guessed)
   */
  static String runningHopVersion() {
    try {
      String[] versions = new org.apache.hop.core.HopVersionProvider().getVersion();
      if (versions != null && versions.length > 0 && versions[0] != null) {
        return versions[0].trim().isEmpty() ? null : versions[0].trim();
      }
    } catch (Exception e) {
      // Fall through: no floor is better than a wrong one.
    }
    return null;
  }

  @JsonIgnore
  public String getPollIntervalMinutes() {
    return getProperty("poll.intervalMinutes");
  }

  @JsonIgnore
  public void setPollIntervalMinutes(String minutes) {
    setProperty("poll.intervalMinutes", minutes);
  }

  @JsonIgnore
  public String getDaysToGoBack() {
    return getProperty("daysToGoBack");
  }

  @JsonIgnore
  public void setDaysToGoBack(String days) {
    setProperty("daysToGoBack", days);
  }

  /**
   * Generate a display string for the "Details" column based on source type
   *
   * @return Details string
   */
  @JsonIgnore
  public String getDetailsDisplay() {
    if (type == null) {
      // A stored source that names no type: hand-edited configuration, or one written by a
      // version that did not have this field. There is nothing to show for it, and the settings
      // table has to be able to draw the row regardless.
      return "";
    }
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
