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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

class GitLabRepositoryBrowser implements GitRepositoryBrowser {

  private static final String PROVIDER = "GitLab";
  private static final String ACCEPT = "application/json";

  @Override
  public void verifyAnonymousAccess(String apiBaseUrl) throws HopException {
    // Public projects are readable without a token; /metadata and /user are not.
    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    GitApiHttp.parseArray(gitlabGet(base + "/projects?per_page=1", null), PROVIDER);
  }

  @Override
  public List<GitOrganizationInfo> listOrganizations(String apiBaseUrl, GitAuth auth)
      throws HopException {
    if (auth == null) {
      throw new HopException(
          "Listing groups needs a personal access token: GitLab only reports the groups of the"
              + " signed-in account. Enter the project namespace and name directly to read a"
              + " public project anonymously.");
    }
    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    List<GitOrganizationInfo> organizations = new ArrayList<>();

    JSONObject user = GitApiHttp.parseObject(gitlabGet(base + "/user", auth), PROVIDER);
    String username = GitApiHttp.getString(user, "username");
    if (!username.isBlank()) {
      organizations.add(new GitOrganizationInfo(username, "Personal (" + username + ")", true));
    }

    // One page used to be the lot, so an account in more than a hundred groups could not reach
    // the rest and was given no sign that anything was missing.
    JSONArray groups = new JSONArray();
    for (int page = 1; page <= DROPDOWN_MAX_PAGES; page++) {
      JSONArray pageGroups =
          GitApiHttp.parseArray(
              gitlabGet(base + "/groups?min_access_level=10&per_page=100&page=" + page, auth),
              PROVIDER);
      groups.addAll(pageGroups);
      if (pageGroups.size() < 100) {
        break;
      }
    }
    for (Object item : groups) {
      JSONObject group = (JSONObject) item;
      String path = GitApiHttp.getString(group, "full_path");
      String name = GitApiHttp.getString(group, "name");
      if (path.isBlank()) {
        continue;
      }
      if (name.isBlank()) {
        name = path;
      }
      organizations.add(new GitOrganizationInfo(path, name, false));
    }
    return organizations;
  }

  @Override
  public GitRepositoryPage listRepositories(
      String apiBaseUrl,
      GitAuth auth,
      GitOrganizationInfo organization,
      String searchQuery,
      int page)
      throws HopException {

    if (organization == null) {
      throw new HopException("Select a group or personal namespace first.");
    }

    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    StringBuilder url = new StringBuilder();
    if (organization.isPersonalAccount()) {
      url.append(base)
          .append("/users/")
          .append(GitApiHttp.urlEncode(organization.getSlug()))
          .append("/projects?order_by=last_activity_at&sort=desc&per_page=")
          .append(PAGE_SIZE)
          .append("&page=")
          .append(page);
    } else {
      url.append(base)
          .append("/groups/")
          .append(GitApiHttp.urlEncode(organization.getSlug()))
          .append("/projects?include_subgroups=true&order_by=last_activity_at&sort=desc&per_page=")
          .append(PAGE_SIZE)
          .append("&page=")
          .append(page);
    }

    if (searchQuery != null && !searchQuery.isBlank()) {
      url.append("&search=").append(GitApiHttp.urlEncode(searchQuery));
    }

    JSONArray array = GitApiHttp.parseArray(gitlabGet(url.toString(), auth), PROVIDER);
    List<GitRepositoryInfo> repos = parseRepos(array, organization.getSlug());
    boolean hasMore = repos.size() == PAGE_SIZE;
    return new GitRepositoryPage(repos, page, hasMore);
  }

  @Override
  public List<String> listBranches(
      String apiBaseUrl, GitAuth auth, String owner, String repository, int maxPages)
      throws HopException {
    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    String projectId = GitApiHttp.urlEncode(owner + "/" + repository);
    List<String> branches = new ArrayList<>();
    for (int page = 1; page <= maxPages; page++) {
      String url =
          base
              + "/projects/"
              + projectId
              + "/repository/branches?per_page="
              + PAGE_SIZE
              + "&page="
              + page;
      GitApiHttp.ApiResponse response = gitlabGetPaged(url, auth);
      JSONArray array = GitApiHttp.parseArray(response.getBody(), PROVIDER);
      if (array.isEmpty()) {
        break;
      }
      for (Object item : array) {
        JSONObject branch = (JSONObject) item;
        String name = GitApiHttp.getString(branch, "name");
        if (!name.isBlank()) {
          branches.add(name);
        }
      }
      if (!response.isHasNextPage()) {
        break;
      }
    }
    branches.sort(String.CASE_INSENSITIVE_ORDER);
    return branches;
  }

  private List<GitRepositoryInfo> parseRepos(JSONArray array, String owner) {
    List<GitRepositoryInfo> result = new ArrayList<>();
    for (Object item : array) {
      JSONObject repo = (JSONObject) item;
      // The API addresses a project by its path, not its display name: "Putki Test" lives at
      // putki-io/putki-test. Storing the name here produced a 404 the moment the two differed.
      String name = GitApiHttp.getString(repo, "path");
      if (name.isBlank()) {
        name = GitApiHttp.getString(repo, "name");
      }
      String description = GitApiHttp.getString(repo, "description");
      String visibility = GitApiHttp.getString(repo, "visibility");
      boolean isPrivate = "private".equalsIgnoreCase(visibility);
      String updatedAt = GitApiHttp.getString(repo, "last_activity_at");
      result.add(new GitRepositoryInfo(name, owner, description, isPrivate, updatedAt));
    }
    return result;
  }

  private String gitlabGet(String url, GitAuth auth) throws HopException {
    return gitlabGetPaged(url, auth).getBody();
  }

  private GitApiHttp.ApiResponse gitlabGetPaged(String url, GitAuth auth) throws HopException {
    return GitApiHttp.getWithPagination(
        url, auth, GitProvider.AuthStyle.GITLAB_PRIVATE_TOKEN, ACCEPT, PROVIDER, null);
  }

  private String resolveBase(String apiBaseUrl) {
    if (apiBaseUrl != null && !apiBaseUrl.isBlank()) {
      return apiBaseUrl;
    }
    return "https://gitlab.com/api/v4";
  }
}
