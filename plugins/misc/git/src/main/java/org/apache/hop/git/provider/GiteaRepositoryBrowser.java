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

/** Gitea and Forgejo repository browser ({@code /api/v1}). */
class GiteaRepositoryBrowser implements GitRepositoryBrowser {

  private static final String PROVIDER = "Gitea";
  private static final String ACCEPT = "application/json";

  @Override
  public void verifyAnonymousAccess(String apiBaseUrl) throws HopException {
    // Gitea and Forgejo both serve the version endpoint without a token.
    String base = GitApiHttp.trimBase(apiBaseUrl);
    GitApiHttp.parseObject(giteaGet(base + "/version", null), PROVIDER);
  }

  @Override
  public List<GitOrganizationInfo> listOrganizations(String apiBaseUrl, GitAuth auth)
      throws HopException {
    if (auth == null) {
      throw new HopException(
          "Listing organizations needs a personal access token: the server only reports the"
              + " organizations of the signed-in account. Enter the repository owner and name"
              + " directly to read a public repository anonymously.");
    }
    String base = GitApiHttp.trimBase(apiBaseUrl);
    List<GitOrganizationInfo> organizations = new ArrayList<>();

    JSONObject user = GitApiHttp.parseObject(giteaGet(base + "/user", auth), PROVIDER);
    String login = GitApiHttp.getString(user, "login");
    if (!login.isBlank()) {
      organizations.add(new GitOrganizationInfo(login, "Personal (" + login + ")", true));
    }

    JSONArray orgs = new JSONArray();
    for (int page = 1; page <= DROPDOWN_MAX_PAGES; page++) {
      JSONArray pageOrgs =
          GitApiHttp.parseArray(
              giteaGet(base + "/user/orgs?limit=100&page=" + page, auth), PROVIDER);
      orgs.addAll(pageOrgs);
      if (pageOrgs.size() < 100) {
        break;
      }
    }
    for (Object item : orgs) {
      JSONObject org = (JSONObject) item;
      String slug = GitApiHttp.getString(org, "username");
      if (slug.isBlank()) {
        slug = GitApiHttp.getString(org, "name");
      }
      String name = GitApiHttp.getString(org, "full_name");
      if (name.isBlank()) {
        name = slug;
      }
      if (!slug.isBlank()) {
        organizations.add(new GitOrganizationInfo(slug, name, false));
      }
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
      throw new HopException("Select an organization or personal account first.");
    }

    String base = GitApiHttp.trimBase(apiBaseUrl);
    String url;
    if (organization.isPersonalAccount()) {
      url =
          base
              + "/users/"
              + GitApiHttp.urlEncode(organization.getSlug())
              + "/repos?limit="
              + PAGE_SIZE
              + "&page="
              + page;
    } else {
      url =
          base
              + "/orgs/"
              + GitApiHttp.urlEncode(organization.getSlug())
              + "/repos?limit="
              + PAGE_SIZE
              + "&page="
              + page;
    }

    JSONArray array = GitApiHttp.parseArray(giteaGet(url, auth), PROVIDER);
    List<GitRepositoryInfo> repos = parseRepos(array, organization.getSlug(), searchQuery);
    // Whether more pages exist is a property of the API response, not of what survived the
    // client-side filter: a filter that excludes a whole page used to hide every later match.
    boolean hasMore = array.size() == PAGE_SIZE;
    return new GitRepositoryPage(repos, page, hasMore);
  }

  @Override
  public List<String> listBranches(
      String apiBaseUrl, GitAuth auth, String owner, String repository, int maxPages)
      throws HopException {
    String base = GitApiHttp.trimBase(apiBaseUrl);
    List<String> branches = new ArrayList<>();
    for (int page = 1; page <= maxPages; page++) {
      String url =
          base
              + "/repos/"
              + GitApiHttp.urlEncode(owner)
              + "/"
              + GitApiHttp.urlEncode(repository)
              + "/branches?limit="
              + PAGE_SIZE
              + "&page="
              + page;
      GitApiHttp.ApiResponse response = giteaGetPaged(url, auth);
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

  private List<GitRepositoryInfo> parseRepos(JSONArray array, String owner, String searchQuery) {
    String filter = searchQuery != null ? searchQuery.toLowerCase() : "";
    List<GitRepositoryInfo> result = new ArrayList<>();
    for (Object item : array) {
      JSONObject repo = (JSONObject) item;
      String name = GitApiHttp.getString(repo, "name");
      if (!filter.isEmpty() && !name.toLowerCase().contains(filter)) {
        continue;
      }
      String description = GitApiHttp.getString(repo, "description");
      boolean isPrivate = Boolean.TRUE.equals(repo.get("private"));
      String updatedAt = GitApiHttp.getString(repo, "updated_at");
      result.add(new GitRepositoryInfo(name, owner, description, isPrivate, updatedAt));
    }
    return result;
  }

  private String giteaGet(String url, GitAuth auth) throws HopException {
    return giteaGetPaged(url, auth).getBody();
  }

  private GitApiHttp.ApiResponse giteaGetPaged(String url, GitAuth auth) throws HopException {
    return GitApiHttp.getWithPagination(
        url, auth, GitProvider.AuthStyle.TOKEN_HEADER, ACCEPT, PROVIDER, null);
  }
}
