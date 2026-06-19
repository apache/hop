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

/** Holds credentials for authenticating to a Git provider's API. */
public class GitRepoAuth {

  private final String token;
  private final String username;
  private final String password;

  private GitRepoAuth(String token, String username, String password) {
    this.token = token;
    this.username = username;
    this.password = password;
  }

  /** Token-based auth (GitHub PAT, GitLab PAT, GHE PAT). */
  public static GitRepoAuth forToken(String token) {
    return new GitRepoAuth(token, null, null);
  }

  /** Basic auth (Bitbucket username + app password). */
  public static GitRepoAuth forBasic(String username, String password) {
    return new GitRepoAuth(null, username, password);
  }

  public String getToken() {
    return token;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public boolean isTokenBased() {
    return token != null && !token.isBlank();
  }

  public boolean isBasicAuth() {
    return username != null && !username.isBlank();
  }
}
