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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;

/** Lists organizations and repositories from a Git hosting provider REST API. */
public interface GitRepositoryBrowser {

  int PAGE_SIZE = 100;

  /** Maximum pages a Browse dialog will walk when the user asks for more. */
  int DROPDOWN_MAX_PAGES = 50;

  /**
   * Pages loaded to fill a combo when a dialog opens.
   *
   * <p>One page. Filling a combo with every repository of a large organization costs a request per
   * hundred on every open - thirty-odd for an organization the size of apache - and produces a list
   * too long to scroll. The Browse dialogs page and filter properly; the combo is for small
   * organizations and for showing the value already stored.
   */
  int COMBO_PRELOAD_PAGES = 1;

  List<GitOrganizationInfo> listOrganizations(String apiBaseUrl, GitAuth auth) throws HopException;

  /**
   * Confirms the connection settings can reach the provider API.
   *
   * <p>With a credential this lists the organizations the account can see, which also proves the
   * token or app password works. Without one there is no authenticated user to ask about, so the
   * provider falls back to a read that needs no credential. That is a weaker check, but it matches
   * what an anonymous connection can actually do: read public repositories.
   */
  default void verify(String apiBaseUrl, GitAuth auth) throws HopException {
    if (auth == null) {
      verifyAnonymousAccess(apiBaseUrl);
    } else {
      listOrganizations(apiBaseUrl, auth);
    }
  }

  /**
   * Reads a public endpoint to check that the base URL points at this provider's API and that it
   * can be reached. Used by {@link #verify} when the connection carries no credential.
   */
  void verifyAnonymousAccess(String apiBaseUrl) throws HopException;

  GitRepositoryPage listRepositories(
      String apiBaseUrl,
      GitAuth auth,
      GitOrganizationInfo organization,
      String searchQuery,
      int page)
      throws HopException;

  /** Branch names, reading at most {@code maxPages} pages. */
  List<String> listBranches(
      String apiBaseUrl, GitAuth auth, String owner, String repository, int maxPages)
      throws HopException;

  default List<String> listBranches(
      String apiBaseUrl, GitAuth auth, String owner, String repository) throws HopException {
    return listBranches(apiBaseUrl, auth, owner, repository, DROPDOWN_MAX_PAGES);
  }

  /** Paginated repositories for UI dropdowns (multiple API calls, capped). */
  default List<GitRepositoryInfo> listRepositoriesForDropdown(
      String apiBaseUrl, GitAuth auth, GitOrganizationInfo organization, String searchQuery)
      throws HopException {
    return listRepositoriesForDropdown(
        apiBaseUrl, auth, organization, searchQuery, DROPDOWN_MAX_PAGES);
  }

  /** Paginated repositories for UI dropdowns, reading at most {@code maxPages} pages. */
  default List<GitRepositoryInfo> listRepositoriesForDropdown(
      String apiBaseUrl,
      GitAuth auth,
      GitOrganizationInfo organization,
      String searchQuery,
      int maxPages)
      throws HopException {
    List<GitRepositoryInfo> all = new ArrayList<>();
    for (int page = 1; page <= maxPages; page++) {
      GitRepositoryPage result =
          listRepositories(apiBaseUrl, auth, organization, searchQuery, page);
      all.addAll(result.getRepositories());
      if (!result.isHasMore()) {
        break;
      }
    }
    return all;
  }

  static GitRepositoryBrowser forProvider(GitProvider provider) {
    return switch (provider) {
      case GITHUB_CLOUD, GITHUB_ENTERPRISE -> new GitHubRepositoryBrowser();
      case GITLAB -> new GitLabRepositoryBrowser();
      case BITBUCKET -> new BitbucketRepositoryBrowser();
      case FORGEJO, GITEA -> new GiteaRepositoryBrowser();
    };
  }
}
