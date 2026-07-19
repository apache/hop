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
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;

// Map used when serializing to hop-config.json

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
  public static final String DEFAULT_REPO_URL = "https://repo1.maven.org/maven2/";

  private boolean enabled = true;
  private String groupId = DEFAULT_GROUP_ID;
  private String defaultVersion;
  private String searchApiUrl = "https://search.maven.org/solrsearch/select";
  private int cacheTtlHours = 24;
  private List<MarketplaceRepository> repositories = new ArrayList<>();

  public MarketplaceConfig() {
    repositories.add(new MarketplaceRepository("central", DEFAULT_REPO_URL));
  }

  public static MarketplaceConfig load() {
    ILogChannel log = new LogChannel("Marketplace");
    try {
      Object raw = HopConfig.readOption(CONFIG_KEY);
      if (raw == null) {
        return new MarketplaceConfig();
      }
      return HopJson.newMapper().convertValue(raw, MarketplaceConfig.class);
    } catch (Exception e) {
      log.logError("Unable to load marketplace config, using defaults", e);
      return new MarketplaceConfig();
    }
  }

  @SuppressWarnings("unchecked")
  public void save() {
    Map<String, Object> asMap = HopJson.newMapper().convertValue(this, Map.class);
    HopConfig.getInstance().saveOption(CONFIG_KEY, asMap);
  }

  /** First repository base URL, or Maven Central. Always ends with {@code /}. */
  public String primaryRepositoryUrl() {
    return primaryRepository().normalizedUrl();
  }

  /** First configured repository (with credentials), or Maven Central defaults. */
  public MarketplaceRepository primaryRepository() {
    if (repositories != null && !repositories.isEmpty()) {
      MarketplaceRepository first = repositories.get(0);
      if (first != null && first.getUrl() != null && !first.getUrl().isBlank()) {
        return first;
      }
    }
    return new MarketplaceRepository("central", DEFAULT_REPO_URL);
  }
}
