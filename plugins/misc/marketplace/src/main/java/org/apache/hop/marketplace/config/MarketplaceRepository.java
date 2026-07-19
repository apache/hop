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
  private String id = "central";
  private String url = "https://repo1.maven.org/maven2/";

  /** Optional HTTP Basic auth username (e.g. Artifactory admin). */
  private String username;

  /**
   * Optional HTTP Basic auth password. Prefer leaving this empty in hop-config.json and setting
   * {@code HOP_MARKETPLACE_PASSWORD} instead. Do not send credentials for anonymous local Nexus.
   */
  private String password;

  public MarketplaceRepository() {
    // Jackson
  }

  public MarketplaceRepository(String id, String url) {
    this.id = id;
    this.url = url;
  }

  public MarketplaceRepository(String id, String url, String username, String password) {
    this.id = id;
    this.url = url;
    this.username = username;
    this.password = password;
  }

  /** Base URL always ending with {@code /}. */
  public String normalizedUrl() {
    if (StringUtils.isBlank(url)) {
      return MarketplaceConfig.DEFAULT_REPO_URL;
    }
    return url.endsWith("/") ? url : url + "/";
  }

  /**
   * Effective credentials: repository config fields, then {@code HOP_MARKETPLACE_USERNAME} / {@code
   * HOP_MARKETPLACE_PASSWORD}. No credentials means anonymous HTTP (correct for local Nexus after
   * {@code docker/marketplace-nexus/start.sh}).
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
