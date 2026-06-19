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

import org.apache.hop.core.exception.HopException;

/**
 * Fetches repository listings from a Git provider's REST API.
 *
 * <p>Implementations exist for GitHub (cloud + enterprise), GitLab, and Bitbucket.
 */
public interface GitRepositoryBrowser {

  int PAGE_SIZE = 50;

  /**
   * Returns one page of repositories accessible with the given credentials.
   *
   * @param apiBaseUrl Provider API root (e.g. {@code https://api.github.com} or a custom GHE /
   *     GitLab host). May be {@code null} for providers where the default is baked in.
   * @param auth Authentication credentials.
   * @param searchQuery Optional name filter (empty/null means no filter).
   * @param page 1-based page number.
   * @return A page of results; never {@code null}.
   * @throws HopException on network or API errors (message is human-readable).
   */
  GitRepositoryPage listRepositories(
      String apiBaseUrl, GitRepoAuth auth, String searchQuery, int page) throws HopException;

  /**
   * Returns the correct {@link GitRepositoryBrowser} implementation for {@code provider}.
   *
   * @throws IllegalArgumentException for unknown providers.
   */
  static GitRepositoryBrowser forProvider(GitRepoProvider provider) {
    return switch (provider) {
      case GITHUB_CLOUD, GITHUB_ENTERPRISE -> new GitHubRepositoryBrowser();
      case GITLAB -> new GitLabRepositoryBrowser();
      case BITBUCKET -> new BitbucketRepositoryBrowser();
    };
  }
}
