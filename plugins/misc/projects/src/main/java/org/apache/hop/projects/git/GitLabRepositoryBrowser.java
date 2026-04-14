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
import java.nio.charset.StandardCharsets;
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
 * Fetches repository (project) listings from GitLab.com or a self-hosted GitLab instance.
 *
 * <p>Uses {@code GET /projects?membership=true} so only repos accessible to the token are returned,
 * sorted by last activity.
 */
class GitLabRepositoryBrowser implements GitRepositoryBrowser {

  private static final String DEFAULT_API = "https://gitlab.com/api/v4";
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  public GitRepositoryPage listRepositories(
      String apiBaseUrl, GitRepoAuth auth, String searchQuery, int page) throws HopException {

    String base =
        (apiBaseUrl != null && !apiBaseUrl.isBlank())
            ? StringUtil.trimEnd(apiBaseUrl, '/')
            : DEFAULT_API;

    StringBuilder url =
        new StringBuilder(base)
            .append("/projects?membership=true&order_by=last_activity_at&sort=desc")
            .append("&per_page=")
            .append(PAGE_SIZE)
            .append("&page=")
            .append(page);

    if (searchQuery != null && !searchQuery.isBlank()) {
      url.append("&search=")
          .append(java.net.URLEncoder.encode(searchQuery, StandardCharsets.UTF_8));
    }

    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(url.toString()))
            .timeout(Duration.ofSeconds(15))
            .header("Accept", "application/json")
            .GET();

    if (auth != null && auth.isTokenBased()) {
      requestBuilder.header("PRIVATE-TOKEN", auth.getToken());
    }

    HttpResponse<String> response;
    try {
      response = HTTP.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException(
          "Failed to connect to GitLab API at " + base + ": " + e.getMessage(), e);
    }

    if (response.statusCode() == 401) {
      throw new HopException(
          "GitLab authentication failed. Check that your personal access token is valid and has the 'read_api' or 'read_repository' scope.");
    }
    if (response.statusCode() == 403) {
      throw new HopException(
          "GitLab access denied (HTTP 403). Your token may lack the required scope.");
    }
    if (response.statusCode() != 200) {
      throw new HopException("GitLab API returned HTTP " + response.statusCode() + ". URL: " + url);
    }

    List<GitRepositoryInfo> repos = parseRepos(response.body());

    // GitLab includes X-Next-Page header; if we got a full page assume there is more.
    boolean hasMore = repos.size() == PAGE_SIZE;
    return new GitRepositoryPage(repos, page, hasMore);
  }

  private List<GitRepositoryInfo> parseRepos(String json) throws HopException {
    JSONParser parser = new JSONParser();
    JSONArray array;
    try {
      array = (JSONArray) parser.parse(json);
    } catch (ParseException e) {
      throw new HopException("Failed to parse GitLab API response: " + e.getMessage(), e);
    }

    List<GitRepositoryInfo> result = new ArrayList<>();

    for (Object item : array) {
      JSONObject repo = (JSONObject) item;
      String name = getString(repo, "name");
      String owner = "";
      JSONObject ns = (JSONObject) repo.get("namespace");
      if (ns != null) {
        owner = getString(ns, "full_path");
      }
      String description = getString(repo, "description");
      String visibility = getString(repo, "visibility");
      boolean isPrivate = "private".equalsIgnoreCase(visibility);
      String updatedAt = getString(repo, "last_activity_at");
      String httpsCloneUrl = getString(repo, "http_url_to_repo");
      String sshCloneUrl = getString(repo, "ssh_url_to_repo");

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
