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
import java.util.Base64;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.StringUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Fetches repository listings from Bitbucket Cloud using the 2.0 API.
 *
 * <p>Bitbucket uses Basic auth with a username + app password (not an OAuth token). The API returns
 * all repos accessible to that user across their workspaces via {@code /repositories}.
 */
class BitbucketRepositoryBrowser implements GitRepositoryBrowser {

  private static final String DEFAULT_API = "https://api.bitbucket.org/2.0";
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @Override
  public GitRepositoryPage listRepositories(
      String apiBaseUrl, GitRepoAuth auth, String searchQuery, int page) throws HopException {

    if (auth == null || !auth.isBasicAuth()) {
      throw new HopException("Bitbucket requires a username and app password.");
    }

    String base =
        (apiBaseUrl != null && !apiBaseUrl.isBlank())
            ? StringUtil.trimEnd(apiBaseUrl, '/')
            : DEFAULT_API;

    // /repositories/{workspace} lists repos for one workspace; /repositories lists all the user
    // has access to (requires "account" scope on the app password).
    StringBuilder url =
        new StringBuilder(base)
            .append("/repositories/")
            .append(encode(auth.getUsername()))
            .append("?pagelen=")
            .append(PAGE_SIZE)
            .append("&page=")
            .append(page)
            .append("&sort=-updated_on");

    if (searchQuery != null && !searchQuery.isBlank()) {
      url.append("&q=name+%7E+%22")
          .append(java.net.URLEncoder.encode(searchQuery, StandardCharsets.UTF_8))
          .append("%22");
    }

    String credentials =
        Base64.getEncoder()
            .encodeToString(
                (auth.getUsername() + ":" + auth.getPassword()).getBytes(StandardCharsets.UTF_8));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url.toString()))
            .timeout(Duration.ofSeconds(15))
            .header("Accept", "application/json")
            .header("Authorization", "Basic " + credentials)
            .GET()
            .build();

    HttpResponse<String> response;
    try {
      response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException(
          "Failed to connect to Bitbucket API at " + base + ": " + e.getMessage(), e);
    }

    if (response.statusCode() == 401) {
      throw new HopException(
          "Bitbucket authentication failed. Check your username and app password.");
    }
    if (response.statusCode() == 403) {
      throw new HopException(
          "Bitbucket access denied (HTTP 403). Your app password may lack the 'Repositories: Read' permission.");
    }
    if (response.statusCode() != 200) {
      throw new HopException(
          "Bitbucket API returned HTTP " + response.statusCode() + ". URL: " + url);
    }

    return parseResponse(response.body(), page);
  }

  private GitRepositoryPage parseResponse(String json, int page) throws HopException {
    JSONParser parser = new JSONParser();
    JSONObject root;
    try {
      root = (JSONObject) parser.parse(json);
    } catch (ParseException e) {
      throw new HopException("Failed to parse Bitbucket API response: " + e.getMessage(), e);
    }

    JSONArray values = (JSONArray) root.get("values");
    if (values == null) {
      return new GitRepositoryPage(List.of(), page, false);
    }

    List<GitRepositoryInfo> result = new ArrayList<>();
    for (Object item : values) {
      JSONObject repo = (JSONObject) item;
      String name = getString(repo, "name");
      String owner = "";
      JSONObject ws = (JSONObject) repo.get("workspace");
      if (ws != null) {
        owner = getString(ws, "slug");
      }
      String description = getString(repo, "description");
      boolean isPrivate = Boolean.TRUE.equals(repo.get("is_private"));
      String updatedOn = getString(repo, "updated_on");

      String httpsCloneUrl = "";
      String sshCloneUrl = "";
      JSONObject links = (JSONObject) repo.get("links");
      if (links != null) {
        JSONArray cloneLinks = (JSONArray) links.get("clone");
        if (cloneLinks != null) {
          for (Object cl : cloneLinks) {
            JSONObject cloneLink = (JSONObject) cl;
            String linkName = getString(cloneLink, "name");
            String href = getString(cloneLink, "href");
            if ("https".equalsIgnoreCase(linkName)) {
              httpsCloneUrl = href;
            } else if ("ssh".equalsIgnoreCase(linkName)) {
              sshCloneUrl = href;
            }
          }
        }
      }

      result.add(
          new GitRepositoryInfo(
              name, owner, description, isPrivate, updatedOn, httpsCloneUrl, sshCloneUrl));
    }

    boolean hasMore = root.get("next") != null;
    return new GitRepositoryPage(result, page, hasMore);
  }

  private static String getString(JSONObject obj, String key) {
    Object val = obj.get(key);
    return val != null ? val.toString() : "";
  }

  private static String encode(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
