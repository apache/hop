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

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@Getter
@Setter
public class MarketplaceRepository {
  private String id = MarketplaceConfig.DEFAULT_ASF_ID;
  private String name;
  private String url = MarketplaceConfig.DEFAULT_ASF_URL;

  /** When true, this repository is tried first for installs. */
  private boolean primary;

  /** When false, skipped in the install fallback chain. */
  private boolean enabled = true;

  /** Optional HTTP Basic auth username. */
  private String username;

  /**
   * Optional HTTP Basic auth password. Prefer leaving this empty in hop-config.json and setting
   * {@code HOP_MARKETPLACE_PASSWORD} instead for private repos. Do not set for anonymous ASF /
   * Central / local Nexus.
   */
  private String password;

  public MarketplaceRepository() {
    // Jackson
  }

  public MarketplaceRepository(String id, String url) {
    this.id = id;
    this.url = url;
    this.name = id;
  }

  public MarketplaceRepository(String id, String url, boolean primary) {
    this(id, url);
    this.primary = primary;
  }

  public MarketplaceRepository(String id, String name, String url, boolean primary) {
    this.id = id;
    this.name = name;
    this.url = url;
    this.primary = primary;
  }

  public MarketplaceRepository(String id, String url, String username, String password) {
    this(id, url);
    this.username = username;
    this.password = password;
  }

  public String displayName() {
    if (StringUtils.isNotBlank(name)) {
      return name;
    }
    return StringUtils.isNotBlank(id) ? id : normalizedUrl();
  }

  /** Base URL always ending with {@code /}. */
  public String normalizedUrl() {
    if (StringUtils.isBlank(url)) {
      return MarketplaceConfig.DEFAULT_ASF_URL;
    }
    return url.endsWith("/") ? url : url + "/";
  }

  /**
   * Effective credentials: repository config fields, then {@code HOP_MARKETPLACE_USERNAME} / {@code
   * HOP_MARKETPLACE_PASSWORD}. No credentials means anonymous HTTP.
   */
  public String effectiveUsername() {
    if (StringUtils.isNotBlank(username)) {
      return username;
    }
    String fromEnv =
        firstNonBlank(
            System.getenv("HOP_MARKETPLACE_USERNAME"), System.getenv("HOP_MARKETPLACE_USER"));
    return StringUtils.isNotBlank(fromEnv) ? fromEnv : null;
  }

  public String effectivePassword() {
    if (StringUtils.isNotBlank(password)) {
      return password;
    }
    String fromEnv = System.getenv("HOP_MARKETPLACE_PASSWORD");
    return StringUtils.isNotBlank(fromEnv) ? fromEnv : null;
  }

  public boolean hasCredentials() {
    return StringUtils.isNotBlank(effectiveUsername())
        && StringUtils.isNotBlank(effectivePassword());
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (StringUtils.isNotBlank(v)) {
        return v;
      }
    }
    return null;
  }
}
