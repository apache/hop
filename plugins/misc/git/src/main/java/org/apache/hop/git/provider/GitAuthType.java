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

import org.apache.hop.i18n.BaseMessages;

/**
 * How a {@link GitConnection} authenticates to a provider API.
 *
 * <p>This belongs to the connection rather than the provider: most providers accept more than one
 * mechanism. GitLab and GitHub take a personal access token or an OAuth 2 token, and Gitea and
 * Forgejo take either of those or a username and password.
 */
public enum GitAuthType {

  /** A personal access token, sent in whichever header the provider expects. */
  TOKEN,

  /**
   * A username with a password or app password, sent as HTTP Basic.
   *
   * <p>Note that an account which only ever signs in through an external provider (GitHub sign-in
   * on a Forgejo instance, for example) usually has no local password, and has to use a token.
   */
  BASIC,

  /** An OAuth 2 access token, sent as a bearer token. */
  OAUTH2;

  private static final Class<?> PKG = GitAuthType.class;

  /** Label shown in the Authentication dropdown. */
  public String getDisplayName() {
    return BaseMessages.getString(PKG, "GitAuthType." + name() + ".Label");
  }

  public static String[] displayNames(java.util.List<GitAuthType> types) {
    return types.stream().map(GitAuthType::getDisplayName).toArray(String[]::new);
  }

  public static GitAuthType fromDisplayName(String displayName, GitAuthType fallback) {
    if (displayName == null || displayName.isBlank()) {
      return fallback;
    }
    for (GitAuthType type : values()) {
      if (type.getDisplayName().equals(displayName)) {
        return type;
      }
    }
    return fallback;
  }

  /** Reads a stored value, tolerating the empty field of a connection saved before auth types. */
  public static GitAuthType fromStored(String stored, GitAuthType fallback) {
    if (stored == null || stored.isBlank()) {
      return fallback;
    }
    try {
      return valueOf(stored.trim());
    } catch (IllegalArgumentException e) {
      return fromDisplayName(stored, fallback);
    }
  }
}
