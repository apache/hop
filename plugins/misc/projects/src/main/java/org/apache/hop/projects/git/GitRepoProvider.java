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

package org.apache.hop.projects.git;

public enum GitRepoProvider {
  GITHUB_CLOUD("GitHub.com", "https://api.github.com", "https://github.com", false),
  GITHUB_ENTERPRISE("GitHub Enterprise", null, null, true),
  GITLAB("GitLab", "https://gitlab.com/api/v4", "https://gitlab.com", true),
  BITBUCKET("Bitbucket", "https://api.bitbucket.org/2.0", "https://bitbucket.org", false);

  private final String displayName;
  private final String defaultApiBaseUrl;
  private final String defaultWebBaseUrl;

  /** Whether a custom host URL field should be offered in the UI for this provider. */
  private final boolean supportsCustomHost;

  GitRepoProvider(
      String displayName,
      String defaultApiBaseUrl,
      String defaultWebBaseUrl,
      boolean supportsCustomHost) {
    this.displayName = displayName;
    this.defaultApiBaseUrl = defaultApiBaseUrl;
    this.defaultWebBaseUrl = defaultWebBaseUrl;
    this.supportsCustomHost = supportsCustomHost;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDefaultApiBaseUrl() {
    return defaultApiBaseUrl;
  }

  public String getDefaultWebBaseUrl() {
    return defaultWebBaseUrl;
  }

  public boolean isSupportsCustomHost() {
    return supportsCustomHost;
  }

  /** Returns display names for all providers, in declaration order. */
  public static String[] displayNames() {
    GitRepoProvider[] values = values();
    String[] names = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      names[i] = values[i].displayName;
    }
    return names;
  }

  /** Best-effort detection of provider from a repository URL. */
  public static GitRepoProvider detectFromUrl(String url) {
    if (url == null || url.isBlank()) {
      return GITHUB_CLOUD;
    }
    String lower = url.toLowerCase();
    if (lower.contains("github.com")) {
      return GITHUB_CLOUD;
    }
    if (lower.contains("gitlab.com") || lower.contains("/api/v4")) {
      return GITLAB;
    }
    if (lower.contains("bitbucket.org")) {
      return BITBUCKET;
    }
    return GITHUB_CLOUD;
  }
}
