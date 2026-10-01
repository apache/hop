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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

class GitHubRepositoryBrowser implements GitRepositoryBrowser {

  private static final String PROVIDER = "GitHub";
  private static final String ACCEPT = "application/vnd.github+json";

  @Override
  public void verifyAnonymousAccess(String apiBaseUrl) throws HopException {
    // Reachable without a token and, unlike the endpoints below, not tied to a signed-in user.
    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    GitApiHttp.parseObject(githubGet(base + "/rate_limit", null), PROVIDER);
  }

  @Override
  public List<GitOrganizationInfo> listOrganizations(String apiBaseUrl, GitAuth auth)
      throws HopException {
    if (auth == null) {
      throw new HopException(
          "Listing organizations needs a personal access token: GitHub only reports the"
              + " organizations of the signed-in account. Enter the repository owner and name"
              + " directly to read a public repository anonymously.");
    }
    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    List<GitOrganizationInfo> organizations = new ArrayList<>();

    JSONObject user = GitApiHttp.parseObject(githubGet(base + "/user", auth), PROVIDER);
    String login = GitApiHttp.getString(user, "login");
    if (!login.isBlank()) {
      organizations.add(new GitOrganizationInfo(login, "Personal (" + login + ")", true));
    }

    // Past one page the remaining organizations were silently dropped, and an account that
    // belongs to more than a hundred simply could not reach the rest.
    JSONArray orgs = new JSONArray();
    for (int page = 1; page <= DROPDOWN_MAX_PAGES; page++) {
      JSONArray pageOrgs =
          GitApiHttp.parseArray(
              githubGet(base + "/user/orgs?per_page=100&page=" + page, auth), PROVIDER);
      orgs.addAll(pageOrgs);
      if (pageOrgs.size() < 100) {
        break;
      }
    }
    for (Object item : orgs) {
      JSONObject org = (JSONObject) item;
      String slug = GitApiHttp.getString(org, "login");
      String name = GitApiHttp.getString(org, "name");
      if (name.isBlank()) {
        name = slug;
      }
      organizations.add(new GitOrganizationInfo(slug, name, false));
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

    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    String url;
    if (organization.isPersonalAccount()) {
      url =
          base
              + "/users/"
              + GitApiHttp.urlEncode(organization.getSlug())
              + "/repos?per_page="
              + PAGE_SIZE
              + "&page="
              + page
              + "&sort=updated&direction=desc&type=owner";
    } else {
      url =
          base
              + "/orgs/"
              + GitApiHttp.urlEncode(organization.getSlug())
              + "/repos?per_page="
              + PAGE_SIZE
              + "&page="
              + page
              + "&sort=updated&direction=desc";
    }

    GitApiHttp.ApiResponse response = githubGetPaged(url, auth);
    JSONArray array = GitApiHttp.parseArray(response.getBody(), PROVIDER);
    List<GitRepositoryInfo> repos = parseRepos(array, organization.getSlug(), searchQuery);
    return new GitRepositoryPage(repos, page, response.isHasNextPage());
  }

  @Override
  public List<String> listBranches(
      String apiBaseUrl, GitAuth auth, String owner, String repository, int maxPages)
      throws HopException {
    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    List<String> branches = new ArrayList<>();
    for (int page = 1; page <= maxPages; page++) {
      String url =
          base
              + "/repos/"
              + GitApiHttp.urlEncode(owner)
              + "/"
              + GitApiHttp.urlEncode(repository)
              + "/git/matching-refs/heads/?per_page="
              + PAGE_SIZE
              + "&page="
              + page;
      GitApiHttp.ApiResponse response = githubGetPaged(url, auth);
      JSONArray array = GitApiHttp.parseArray(response.getBody(), PROVIDER);
      if (array.isEmpty()) {
        break;
      }
      for (Object item : array) {
        JSONObject ref = (JSONObject) item;
        String name = branchNameFromRef(GitApiHttp.getString(ref, "ref"));
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

  private static String branchNameFromRef(String ref) {
    String prefix = "refs/heads/";
    if (ref != null && ref.startsWith(prefix)) {
      return ref.substring(prefix.length());
    }
    return ref == null ? "" : ref;
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
      String updatedAt = GitApiHttp.getString(repo, "pushed_at");
      result.add(new GitRepositoryInfo(name, owner, description, isPrivate, updatedAt));
    }
    return result;
  }

  private String githubGet(String url, GitAuth auth) throws HopException {
    return githubGetPaged(url, auth).getBody();
  }

  private GitApiHttp.ApiResponse githubGetPaged(String url, GitAuth auth) throws HopException {
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("X-GitHub-Api-Version", "2022-11-28");
    return GitApiHttp.getWithPagination(
        url, auth, GitProvider.AuthStyle.BEARER, ACCEPT, PROVIDER, headers);
  }

  private String resolveBase(String apiBaseUrl) {
    if (apiBaseUrl != null && !apiBaseUrl.isBlank()) {
      return apiBaseUrl;
    }
    return "https://api.github.com";
  }
}
