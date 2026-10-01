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

class BitbucketRepositoryBrowser implements GitRepositoryBrowser {

  private static final String PROVIDER = "Bitbucket";
  private static final String ACCEPT = "application/json";

  @Override
  public void verifyAnonymousAccess(String apiBaseUrl) throws HopException {
    // Bitbucket has no anonymous entry point to check against: /2.0/repositories without a
    // workspace is gone, and everything else is scoped to the signed-in account.
    throw new HopException(
        "Bitbucket needs a username and app password. Its API does not offer an anonymous"
            + " endpoint to verify the connection against.");
  }

  @Override
  public List<GitOrganizationInfo> listOrganizations(String apiBaseUrl, GitAuth auth)
      throws HopException {
    if (auth == null || !auth.isBasicAuth()) {
      throw new HopException("Bitbucket requires a username and app password.");
    }

    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    // Every paginated Bitbucket endpoint answers with a {page, size, values: [...]} envelope, not
    // a bare array - the repository and branch calls below already unwrap it.
    JSONArray workspaces = new JSONArray();
    for (int page = 1; page <= DROPDOWN_MAX_PAGES; page++) {
      JSONObject root =
          GitApiHttp.parseObject(
              GitApiHttp.get(
                  base + "/workspaces?pagelen=100&role=member&page=" + page,
                  auth,
                  GitProvider.AuthStyle.BASIC,
                  ACCEPT,
                  PROVIDER),
              PROVIDER);
      JSONArray values = (JSONArray) root.get("values");
      if (values == null || values.isEmpty()) {
        break;
      }
      workspaces.addAll(values);
      if (root.get("next") == null) {
        break;
      }
    }

    List<GitOrganizationInfo> organizations = new ArrayList<>();
    for (Object item : workspaces) {
      JSONObject workspace = (JSONObject) item;
      JSONObject ws = (JSONObject) workspace.get("workspace");
      if (ws == null) {
        ws = workspace;
      }
      String slug = GitApiHttp.getString(ws, "slug");
      String name = GitApiHttp.getString(ws, "name");
      if (slug.isBlank()) {
        continue;
      }
      if (name.isBlank()) {
        name = slug;
      }
      organizations.add(new GitOrganizationInfo(slug, name, false));
    }

    if (organizations.isEmpty() && auth.getUsername() != null && !auth.getUsername().isBlank()) {
      organizations.add(new GitOrganizationInfo(auth.getUsername(), auth.getUsername(), true));
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

    if (auth == null || !auth.isBasicAuth()) {
      throw new HopException("Bitbucket requires a username and app password.");
    }
    if (organization == null) {
      throw new HopException("Select a workspace first.");
    }

    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    StringBuilder url =
        new StringBuilder(base)
            .append("/repositories/")
            .append(GitApiHttp.urlEncode(organization.getSlug()))
            .append("?pagelen=")
            .append(PAGE_SIZE)
            .append("&page=")
            .append(page)
            .append("&sort=-updated_on");

    if (searchQuery != null && !searchQuery.isBlank()) {
      url.append("&q=name+%7E+%22").append(GitApiHttp.urlEncode(searchQuery)).append("%22");
    }

    JSONObject root =
        GitApiHttp.parseObject(
            GitApiHttp.get(url.toString(), auth, GitProvider.AuthStyle.BASIC, ACCEPT, PROVIDER),
            PROVIDER);
    JSONArray values = (JSONArray) root.get("values");
    List<GitRepositoryInfo> repos = new ArrayList<>();
    if (values != null) {
      for (Object item : values) {
        JSONObject repo = (JSONObject) item;
        // Bitbucket addresses a repository by slug; the name is a display label and the two
        // differ as soon as a name has capitals or spaces.
        String name = GitApiHttp.getString(repo, "slug");
        if (name.isBlank()) {
          name = GitApiHttp.getString(repo, "name");
        }
        String description = GitApiHttp.getString(repo, "description");
        boolean isPrivate = Boolean.TRUE.equals(repo.get("is_private"));
        String updatedOn = GitApiHttp.getString(repo, "updated_on");
        repos.add(
            new GitRepositoryInfo(name, organization.getSlug(), description, isPrivate, updatedOn));
      }
    }

    boolean hasMore = root.get("next") != null;
    return new GitRepositoryPage(repos, page, hasMore);
  }

  @Override
  public List<String> listBranches(
      String apiBaseUrl, GitAuth auth, String owner, String repository, int maxPages)
      throws HopException {
    if (auth == null || !auth.isBasicAuth()) {
      throw new HopException("Bitbucket requires a username and app password.");
    }
    String base = GitApiHttp.trimBase(resolveBase(apiBaseUrl));
    List<String> branches = new ArrayList<>();
    for (int page = 1; page <= maxPages; page++) {
      String url =
          base
              + "/repositories/"
              + GitApiHttp.urlEncode(owner)
              + "/"
              + GitApiHttp.urlEncode(repository)
              + "/refs/branches?pagelen="
              + PAGE_SIZE
              + "&page="
              + page;
      JSONObject root =
          GitApiHttp.parseObject(
              GitApiHttp.get(url, auth, GitProvider.AuthStyle.BASIC, ACCEPT, PROVIDER), PROVIDER);
      JSONArray values = (JSONArray) root.get("values");
      if (values == null || values.isEmpty()) {
        break;
      }
      for (Object item : values) {
        JSONObject branch = (JSONObject) item;
        String name = GitApiHttp.getString(branch, "name");
        if (!name.isBlank()) {
          branches.add(name);
        }
      }
      if (root.get("next") == null) {
        break;
      }
    }
    branches.sort(String.CASE_INSENSITIVE_ORDER);
    return branches;
  }

  private String resolveBase(String apiBaseUrl) {
    if (apiBaseUrl != null && !apiBaseUrl.isBlank()) {
      return apiBaseUrl;
    }
    return "https://api.bitbucket.org/2.0";
  }
}
