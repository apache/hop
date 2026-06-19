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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.StringUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Fetches repository listings from GitHub.com or a GitHub Enterprise server.
 *
 * <p>Uses {@code GET /user/repos} sorted by {@code updated} to return the most recently active
 * repos first, matching the mental model users expect.
 */
class GitHubRepositoryBrowser implements GitRepositoryBrowser {

  private static final String DEFAULT_API = "https://api.github.com";
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  public GitRepositoryPage listRepositories(
      String apiBaseUrl, GitRepoAuth auth, String searchQuery, int page) throws HopException {

    String base =
        (apiBaseUrl != null && !apiBaseUrl.isBlank())
            ? StringUtil.trimEnd(apiBaseUrl, '/')
            : DEFAULT_API;
    String url =
        base
            + "/user/repos?per_page="
            + PAGE_SIZE
            + "&page="
            + page
            + "&sort=updated&direction=desc&affiliation=owner,collaborator,organization_member";

    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(15))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .GET();

    if (auth != null && auth.isTokenBased()) {
      requestBuilder.header("Authorization", "token " + auth.getToken());
    }

    HttpResponse<String> response;
    try {
      response = HTTP.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException(
          "Failed to connect to GitHub API at " + base + ": " + e.getMessage(), e);
    }

    if (response.statusCode() == 401) {
      throw new HopException(
          "GitHub authentication failed. Check that your personal access token is valid and has the 'repo' scope.");
    }
    if (response.statusCode() == 403) {
      throw new HopException(
          "GitHub access denied (HTTP 403). Your token may be missing the 'repo' scope or rate-limited.");
    }
    if (response.statusCode() != 200) {
      throw new HopException("GitHub API returned HTTP " + response.statusCode() + ". URL: " + url);
    }

    List<GitRepositoryInfo> repos = parseRepos(response.body(), searchQuery);

    // GitHub uses Link header for pagination; if we got a full page there is likely more.
    boolean hasMore = repos.size() == PAGE_SIZE;
    return new GitRepositoryPage(repos, page, hasMore);
  }

  private List<GitRepositoryInfo> parseRepos(String json, String searchQuery) throws HopException {
    JSONParser parser = new JSONParser();
    JSONArray array;
    try {
      array = (JSONArray) parser.parse(json);
    } catch (ParseException e) {
      throw new HopException("Failed to parse GitHub API response: " + e.getMessage(), e);
    }

    String filter = (searchQuery != null) ? searchQuery.toLowerCase() : "";
    List<GitRepositoryInfo> result = new ArrayList<>();

    for (Object item : array) {
      JSONObject repo = (JSONObject) item;
      String name = getString(repo, "name");
      String owner = "";
      JSONObject ownerObj = (JSONObject) repo.get("owner");
      if (ownerObj != null) {
        owner = getString(ownerObj, "login");
      }
      String description = getString(repo, "description");
      boolean isPrivate = Boolean.TRUE.equals(repo.get("private"));
      String updatedAt = getString(repo, "pushed_at");
      String httpsCloneUrl = getString(repo, "clone_url");
      String sshCloneUrl = getString(repo, "ssh_url");

      if (!filter.isEmpty()
          && !name.toLowerCase().contains(filter)
          && !owner.toLowerCase().contains(filter)) {
        continue;
      }

      result.add(
          new GitRepositoryInfo(
              name, owner, description, isPrivate, updatedAt, httpsCloneUrl, sshCloneUrl));
    }
    return result;
  }

  private static String getString(JSONObject obj, String key) {
    Object val = obj.get(key);
    return val != null ? val.toString() : "";
  }
}
