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

import lombok.Getter;

/** Holds credentials for authenticating to a Git hosting provider's REST API. */
@Getter
public class GitAuth {

  private final GitAuthType type;

  /**
   * The header this credential must be sent in, decided by the connection that built it.
   *
   * <p>Null for a credential built by the legacy factories below, in which case the caller's own
   * default is used. Carrying it here keeps every client call site out of the decision: an OAuth 2
   * token and a personal access token need different headers on the same provider.
   */
  private final GitProvider.AuthStyle style;

  private final String token;
  private final String username;
  private final String password;

  private GitAuth(
      GitAuthType type,
      GitProvider.AuthStyle style,
      String token,
      String username,
      String password) {
    this.type = type;
    this.style = style;
    this.token = token;
    this.username = username;
    this.password = password;
  }

  /** A token credential whose header is decided by the provider and auth type. */
  static GitAuth forToken(GitAuthType type, GitProvider.AuthStyle style, String token) {
    if (token == null || token.isBlank()) {
      return null;
    }
    return new GitAuth(type, style, token, null, null);
  }

  /** A basic credential. Both halves are required: see {@link #isBasicAuth()}. */
  static GitAuth forBasic(GitProvider.AuthStyle style, String username, String password) {
    if (username == null || username.isBlank() || password == null || password.isBlank()) {
      return null;
    }
    return new GitAuth(GitAuthType.BASIC, style, null, username, password);
  }

  public static GitAuth forToken(String token) {
    return forToken(GitAuthType.TOKEN, null, token);
  }

  public static GitAuth forBasic(String username, String password) {
    return forBasic(null, username, password);
  }

  public boolean isTokenBased() {
    return token != null && !token.isBlank();
  }

  /**
   * Whether this is a usable basic credential.
   *
   * <p>Both halves must be present. Checking only the username let a blank password through, and
   * the request then went out as {@code Basic base64("user:null")} - a wrong credential sent to a
   * remote host, which is worse than sending none and reads as a failed login in its audit log.
   */
  public boolean isBasicAuth() {
    return username != null && !username.isBlank() && password != null && !password.isBlank();
  }
}
