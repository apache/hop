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

package org.apache.hop.marketplace.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;

/**
 * Marketplace settings stored under the {@code marketplace} key in hop-config.json (or defaults).
 *
 * <p>Hop always loads that file from {@code ${HOP_CONFIG_FOLDER}/hop-config.json}. When {@code
 * HOP_CONFIG_FOLDER} is unset, it defaults to {@code <user.dir>/config} (with the hop launcher, the
 * client install's {@code config/} directory).
 */
@Getter
@Setter
public class MarketplaceConfig {
  public static final String CONFIG_KEY = "marketplace";
  public static final String DEFAULT_GROUP_ID = "org.apache.hop";

  public static final String DEFAULT_ASF_ID = "asf";
  public static final String DEFAULT_ASF_NAME = "Apache Repository";
  public static final String DEFAULT_ASF_URL =
      "https://repository.apache.org/content/groups/public/";

  public static final String DEFAULT_CENTRAL_ID = "central";
  public static final String DEFAULT_CENTRAL_NAME = "Maven Central";
  public static final String DEFAULT_CENTRAL_URL = "https://repo1.maven.org/maven2/";

  /**
   * @deprecated use {@link #DEFAULT_ASF_URL}
   */
  @Deprecated public static final String DEFAULT_REPO_URL = DEFAULT_ASF_URL;

  private boolean enabled = true;
  private String groupId = DEFAULT_GROUP_ID;
  private String defaultVersion;
  private String searchApiUrl = "https://search.maven.org/solrsearch/select";
  private int cacheTtlHours = 24;
  private List<MarketplaceRepository> repositories = new ArrayList<>();

  public MarketplaceConfig() {
    repositories.addAll(defaultRepositories());
  }

  /** ASF primary + Maven Central fallback (shipped defaults). */
  public static List<MarketplaceRepository> defaultRepositories() {
    List<MarketplaceRepository> list = new ArrayList<>();
    list.add(new MarketplaceRepository(DEFAULT_ASF_ID, DEFAULT_ASF_NAME, DEFAULT_ASF_URL, true));
    list.add(
        new MarketplaceRepository(
            DEFAULT_CENTRAL_ID, DEFAULT_CENTRAL_NAME, DEFAULT_CENTRAL_URL, false));
    return list;
  }

  public static MarketplaceConfig load() {
    ILogChannel log = new LogChannel("Marketplace");
    try {
      Object raw = HopConfig.readOption(CONFIG_KEY);
      if (raw == null) {
        return new MarketplaceConfig();
      }
      MarketplaceConfig config = HopJson.newMapper().convertValue(raw, MarketplaceConfig.class);
      if (config.getRepositories() == null || config.getRepositories().isEmpty()) {
        config.setRepositories(defaultRepositories());
      }
      config.ensureValidPrimary();
      return config;
    } catch (Exception e) {
      log.logError("Unable to load marketplace config, using defaults", e);
      return new MarketplaceConfig();
    }
  }

  @SuppressWarnings("unchecked")
  public void save() {
    ensureValidPrimary();
    Map<String, Object> asMap = HopJson.newMapper().convertValue(this, Map.class);
    HopConfig.getInstance().saveOption(CONFIG_KEY, asMap);
  }

  /** Replace repository list with shipped ASF + Central defaults. */
  public void resetToDefaults() {
    repositories = defaultRepositories();
  }

  /**
   * Ensure exactly one primary among enabled repos when possible. Keeps the first {@code
   * primary=true} entry and clears the rest; if none, marks the first enabled repo.
   */
  public void ensureValidPrimary() {
    if (repositories == null) {
      repositories = new ArrayList<>();
    }
    MarketplaceRepository firstPrimary = null;
    for (MarketplaceRepository repo : repositories) {
      if (repo == null) {
        continue;
      }
      if (repo.isPrimary()) {
        if (firstPrimary == null) {
          firstPrimary = repo;
        } else {
          repo.setPrimary(false);
        }
      }
    }
    if (firstPrimary == null) {
      for (MarketplaceRepository repo : repositories) {
        if (repo != null && repo.isEnabled()) {
          repo.setPrimary(true);
          break;
        }
      }
    }
  }

  /** First repository base URL, or ASF public. Always ends with {@code /}. */
  public String primaryRepositoryUrl() {
    return primaryRepository().normalizedUrl();
  }

  /**
   * Primary repository (preferred for display and tried first on install), or ASF built-in
   * defaults.
   */
  public MarketplaceRepository primaryRepository() {
    ensureValidPrimary();
    if (repositories != null) {
      for (MarketplaceRepository repo : repositories) {
        if (repo != null
            && repo.isPrimary()
            && repo.isEnabled()
            && StringUtils.isNotBlank(repo.getUrl())) {
          return repo;
        }
      }
      for (MarketplaceRepository repo : repositories) {
        if (repo != null && repo.isEnabled() && StringUtils.isNotBlank(repo.getUrl())) {
          return repo;
        }
      }
    }
    return new MarketplaceRepository(DEFAULT_ASF_ID, DEFAULT_ASF_NAME, DEFAULT_ASF_URL, true);
  }

  /**
   * Enabled repositories in install order: primary first, then remaining list order. Used as the
   * fallback chain.
   */
  public List<MarketplaceRepository> orderedRepositories() {
    ensureValidPrimary();
    List<MarketplaceRepository> ordered = new ArrayList<>();
    MarketplaceRepository primary = null;
    if (repositories != null) {
      for (MarketplaceRepository repo : repositories) {
        if (repo != null
            && repo.isPrimary()
            && repo.isEnabled()
            && StringUtils.isNotBlank(repo.getUrl())) {
          primary = repo;
          break;
        }
      }
    }
    if (primary != null) {
      ordered.add(primary);
    }
    if (repositories != null) {
      for (MarketplaceRepository repo : repositories) {
        if (repo == null || !repo.isEnabled() || StringUtils.isBlank(repo.getUrl())) {
          continue;
        }
        if (primary != null && Objects.equals(primary.getId(), repo.getId()) && primary == repo) {
          continue;
        }
        if (primary != null && primary == repo) {
          continue;
        }
        // skip duplicate of primary by id
        if (primary != null
            && StringUtils.isNotBlank(primary.getId())
            && primary.getId().equals(repo.getId())) {
          continue;
        }
        ordered.add(repo);
      }
    }
    if (ordered.isEmpty()) {
      ordered.add(
          new MarketplaceRepository(DEFAULT_ASF_ID, DEFAULT_ASF_NAME, DEFAULT_ASF_URL, true));
    }
    return ordered;
  }

  public MarketplaceRepository findRepository(String id) {
    if (StringUtils.isBlank(id) || repositories == null) {
      return null;
    }
    for (MarketplaceRepository repo : repositories) {
      if (repo != null && id.equals(repo.getId())) {
        return repo;
      }
    }
    return null;
  }

  public void addRepository(MarketplaceRepository repo) throws HopException {
    if (repo == null || StringUtils.isBlank(repo.getId()) || StringUtils.isBlank(repo.getUrl())) {
      throw new HopException("Repository requires id and url");
    }
    if (findRepository(repo.getId()) != null) {
      throw new HopException("Repository id already exists: " + repo.getId());
    }
    if (repositories == null) {
      repositories = new ArrayList<>();
    }
    if (repo.isPrimary()) {
      clearPrimaryFlags();
    }
    repositories.add(repo);
    ensureValidPrimary();
  }

  public void removeRepository(String id) throws HopException {
    MarketplaceRepository existing = findRepository(id);
    if (existing == null) {
      throw new HopException("Unknown repository id: " + id);
    }
    repositories.remove(existing);
    ensureValidPrimary();
  }

  public void setPrimary(String id) throws HopException {
    MarketplaceRepository repo = findRepository(id);
    if (repo == null) {
      throw new HopException("Unknown repository id: " + id);
    }
    if (!repo.isEnabled()) {
      throw new HopException("Cannot set disabled repository as primary: " + id);
    }
    clearPrimaryFlags();
    repo.setPrimary(true);
  }

  public void setEnabled(String id, boolean enabled) throws HopException {
    MarketplaceRepository repo = findRepository(id);
    if (repo == null) {
      throw new HopException("Unknown repository id: " + id);
    }
    repo.setEnabled(enabled);
    if (!enabled && repo.isPrimary()) {
      repo.setPrimary(false);
    }
    ensureValidPrimary();
  }

  private void clearPrimaryFlags() {
    if (repositories == null) {
      return;
    }
    for (MarketplaceRepository repo : repositories) {
      if (repo != null) {
        repo.setPrimary(false);
      }
    }
  }
}
