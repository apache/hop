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

package org.apache.hop.git.provider;

import java.util.List;

public enum GitProvider {
  GITHUB_CLOUD("GitHub.com", "https://api.github.com", false, null),
  GITHUB_ENTERPRISE("GitHub Enterprise", null, true, "https://github.example.com/api/v3"),
  GITLAB("GitLab", "https://gitlab.com/api/v4", true, null),
  BITBUCKET("Bitbucket", "https://api.bitbucket.org/2.0", true, null),
  // No default: a Forgejo instance is self-hosted, and defaulting to Codeberg meant an empty
  // URL silently queried somebody else's server.
  FORGEJO("Forgejo", null, true, "https://forgejo.example.com/api/v1"),
  GITEA("Gitea", null, true, "https://gitea.example.com/api/v1");

  private final String displayName;
  private final String defaultApiBaseUrl;
  private final boolean supportsCustomHost;
  private final String apiBaseUrlHint;

  GitProvider(
      String displayName,
      String defaultApiBaseUrl,
      boolean supportsCustomHost,
      String apiBaseUrlHint) {
    this.displayName = displayName;
    this.defaultApiBaseUrl = defaultApiBaseUrl;
    this.supportsCustomHost = supportsCustomHost;
    this.apiBaseUrlHint = apiBaseUrlHint;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDefaultApiBaseUrl() {
    return defaultApiBaseUrl;
  }

  public boolean isSupportsCustomHost() {
    return supportsCustomHost;
  }

  public String getApiBaseUrlHint() {
    return apiBaseUrlHint;
  }

  /** Default API URL shown in the UI when the connection has no custom value stored. */
  public String getUiDefaultApiBaseUrl() {
    if (defaultApiBaseUrl != null && !defaultApiBaseUrl.isBlank()) {
      return defaultApiBaseUrl;
    }
    return apiBaseUrlHint != null ? apiBaseUrlHint : "";
  }

  /**
   * The mechanisms this provider's API accepts, most preferred first.
   *
   * <p>Bitbucket is the only one that does not take a token: its API authenticates with a username
   * and an app password. Gitea and Forgejo accept all three - their API advertises both an
   * Authorization header and HTTP Basic.
   */
  public List<GitAuthType> getSupportedAuthTypes() {
    return switch (this) {
      case GITHUB_CLOUD, GITHUB_ENTERPRISE, GITLAB ->
          List.of(GitAuthType.TOKEN, GitAuthType.OAUTH2);
      case BITBUCKET -> List.of(GitAuthType.BASIC);
      case FORGEJO, GITEA -> List.of(GitAuthType.TOKEN, GitAuthType.BASIC, GitAuthType.OAUTH2);
    };
  }

  /** Used for a connection that has no stored auth type, so older connections keep working. */
  public GitAuthType getDefaultAuthType() {
    return this == BITBUCKET ? GitAuthType.BASIC : GitAuthType.TOKEN;
  }

  public boolean supports(GitAuthType authType) {
    return getSupportedAuthTypes().contains(authType);
  }

  /**
   * The header a given mechanism uses on this provider.
   *
   * <p>Only a personal access token varies between providers: GitLab wants its own PRIVATE-TOKEN
   * header, Gitea and Forgejo want {@code Authorization: token}, GitHub wants a bearer token. An
   * OAuth 2 token is a bearer token everywhere, which is why sending one through the personal
   * access token header does not work on GitLab.
   */
  public AuthStyle getAuthStyle(GitAuthType authType) {
    if (authType == GitAuthType.BASIC) {
      return AuthStyle.BASIC;
    }
    if (authType == GitAuthType.OAUTH2) {
      return AuthStyle.BEARER;
    }
    return switch (this) {
      case GITHUB_CLOUD, GITHUB_ENTERPRISE -> AuthStyle.BEARER;
      case GITLAB -> AuthStyle.GITLAB_PRIVATE_TOKEN;
      case BITBUCKET -> AuthStyle.BASIC;
      case FORGEJO, GITEA -> AuthStyle.TOKEN_HEADER;
    };
  }

  public enum AuthStyle {
    TOKEN_HEADER,
    GITLAB_PRIVATE_TOKEN,
    BEARER,
    BASIC
  }

  public static String[] displayNames() {
    GitProvider[] values = values();
    String[] names = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      names[i] = values[i].displayName;
    }
    return names;
  }

  public static GitProvider fromDisplayName(String displayName) {
    if (displayName == null || displayName.isBlank()) {
      return GITHUB_CLOUD;
    }
    for (GitProvider provider : values()) {
      if (provider.displayName.equals(displayName)) {
        return provider;
      }
    }
    return GITHUB_CLOUD;
  }

  public static GitProvider fromStored(String stored) {
    if (stored == null || stored.isBlank()) {
      return GITHUB_CLOUD;
    }
    try {
      return valueOf(stored.trim());
    } catch (IllegalArgumentException e) {
      return fromDisplayName(stored);
    }
  }
}
