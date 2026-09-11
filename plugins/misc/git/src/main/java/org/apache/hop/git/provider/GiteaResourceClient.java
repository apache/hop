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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/** Gitea and Forgejo API client ({@code /api/v1}). */
class GiteaResourceClient implements GitResourceClient {

  private static final String ACCEPT = "application/json";
  private static final String PROVIDER = "gitea";
  private static final int GITEA_MAX_PER_PAGE = 50;

  private final String apiBaseUrl;
  private final GitAuth auth;

  GiteaResourceClient(String apiBaseUrl, GitAuth auth) {
    this.apiBaseUrl = GitApiHttp.trimBase(apiBaseUrl);
    this.auth = auth;
  }

  @Override
  public GitResourceReader openReader(
      GitResourceType resourceType, String owner, String repository, GitListOptions options)
      throws HopException {
    if (resourceType == GitResourceType.ISSUE_COMMENTS
        || resourceType == GitResourceType.PR_COMMENTS
        || resourceType == GitResourceType.ISSUE_EVENTS
        || resourceType == GitResourceType.COMMIT_FILES) {
      throw new HopException(
          resourceType.name() + " is not yet supported for Gitea/Forgejo in Git Input");
    }
    return new PageBufferGitResourceReader(
        options.getMaxRecords(GITEA_MAX_PER_PAGE),
        new PagedResourceLoader(resourceType, owner, repository, options));
  }

  private final class PagedResourceLoader implements GitResourcePageLoader {

    private final GitResourceType resourceType;
    private final String owner;
    private final String repository;
    private final GitListOptions options;
    private final String state;
    private final int pageSize;
    private int page = 1;
    private boolean exhausted;

    PagedResourceLoader(
        GitResourceType resourceType, String owner, String repository, GitListOptions options) {
      this.resourceType = resourceType;
      this.owner = owner;
      this.repository = repository;
      this.options = options;
      this.state = normalizeState(options.getState());
      this.pageSize = options.getEffectivePageSize(GITEA_MAX_PER_PAGE);
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted || !options.hasPageBudget(page)) {
        exhausted = true;
        return List.of();
      }

      String url = buildUrl(resourceType, owner, repository, options, state, page, pageSize);
      String body;
      try {
        body =
            GitApiHttp.get(url, auth, GitProvider.AuthStyle.TOKEN_HEADER, ACCEPT, "Gitea/Forgejo");
      } catch (GitApiException e) {
        if (page > 1 && e.isServerError()) {
          throw new GitPaginationException("Gitea", page, e);
        }
        throw e;
      }
      page++;

      JSONArray array = parseArray(body);
      if (array.isEmpty()) {
        exhausted = true;
        return List.of();
      }

      List<GitResourceRecord> pageRecords = new ArrayList<>(array.size());
      for (Object item : array) {
        JSONObject json = (JSONObject) item;
        GitResourceRecord record = mapRecord(resourceType, owner, repository, json);
        if (record != null) {
          pageRecords.add(record);
        }
      }

      if (options.isLastPage(array.size(), GITEA_MAX_PER_PAGE)) {
        exhausted = true;
      }
      return pageRecords;
    }

    @Override
    public boolean isExhausted() {
      return exhausted;
    }
  }

  private String buildUrl(
      GitResourceType resourceType,
      String owner,
      String repository,
      GitListOptions options,
      String state,
      int page,
      int pageSize) {

    String base =
        apiBaseUrl
            + "/repos/"
            + encode(owner)
            + "/"
            + encode(repository)
            + pathSuffix(resourceType)
            + "?limit="
            + pageSize
            + "&page="
            + page;

    return switch (resourceType) {
      case COMMITS -> {
        String u = base;
        if (options.getSince() != null && !options.getSince().isBlank()) {
          u += "&since=" + encode(options.getSince());
        }
        if (options.getBranch() != null && !options.getBranch().isBlank()) {
          u += "&sha=" + encode(options.getBranch());
        }
        yield u;
      }
      case ISSUES, PULL_REQUESTS -> base + "&state=" + encode(state);
      case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES ->
          throw new IllegalStateException();
    };
  }

  private static String pathSuffix(GitResourceType resourceType) {
    return switch (resourceType) {
      case COMMITS -> "/commits";
      case ISSUES -> "/issues";
      case PULL_REQUESTS -> "/pulls";
      case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES ->
          throw new IllegalStateException();
    };
  }

  private GitResourceRecord mapRecord(
      GitResourceType resourceType, String owner, String repository, JSONObject json) {

    if (resourceType == GitResourceType.ISSUES && json.get("pull_request") != null) {
      return null;
    }

    String entityType = resourceType.getEntityType();
    String rawJson = json.toJSONString();

    return switch (resourceType) {
      case COMMITS -> {
        JSONObject commit = (JSONObject) json.get("commit");
        if (commit == null) {
          commit = json;
        }
        JSONObject author = (JSONObject) commit.get("author");
        yield new GitResourceRecord(
            PROVIDER,
            entityType,
            owner,
            repository,
            GitApiHttp.getString(json, "sha"),
            0L,
            firstLine(GitApiHttp.getString(commit, "message")),
            "",
            author != null ? GitApiHttp.getString(author, "name") : "",
            author != null ? GitApiHttp.getString(author, "date") : "",
            "",
            "",
            GitApiHttp.getString(json, "html_url"),
            GitApiHttp.getString(commit, "message"),
            GitApiHttp.getString(json, "sha"),
            "",
            "",
            "",
            rawJson);
      }
      case ISSUES, PULL_REQUESTS -> {
        JSONObject user = (JSONObject) json.get("user");
        yield new GitResourceRecord(
            PROVIDER,
            entityType,
            owner,
            repository,
            GitApiHttp.getString(json, "id"),
            GitApiHttp.getLong(json, "number"),
            GitApiHttp.getString(json, "title"),
            GitApiHttp.getString(json, "state"),
            user != null ? GitApiHttp.getString(user, "login") : "",
            GitApiHttp.getString(json, "created_at"),
            GitApiHttp.getString(json, "updated_at"),
            GitApiHttp.getString(json, "closed_at"),
            GitApiHttp.getString(json, "html_url"),
            GitApiHttp.getString(json, "body"),
            "",
            nestedRef(json, "head"),
            nestedRef(json, "base"),
            Boolean.TRUE.equals(json.get("merged")) ? "Y" : "N",
            rawJson);
      }
      case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES ->
          throw new IllegalStateException();
    };
  }

  private static JSONArray parseArray(String body) throws HopException {
    JSONParser parser = new JSONParser();
    try {
      return (JSONArray) parser.parse(body);
    } catch (ParseException e) {
      throw new HopException("Failed to parse Gitea/Forgejo API response: " + e.getMessage(), e);
    }
  }

  /**
   * Reads a branch name from a pull request. Gitea and Forgejo nest it as {@code head.ref} and
   * {@code base.ref}; there are no flat head_branch/base_branch fields, so reading those gave every
   * pull request an empty source and target branch.
   */
  private static String nestedRef(JSONObject json, String side) {
    Object nested = json == null ? null : json.get(side);
    return nested instanceof JSONObject obj ? GitApiHttp.getString(obj, "ref") : "";
  }

  private static String normalizeState(String state) {
    if (state == null || state.isBlank() || "all".equalsIgnoreCase(state)) {
      return "all";
    }
    return state.toLowerCase();
  }

  private static String firstLine(String message) {
    if (message == null || message.isBlank()) {
      return "";
    }
    int idx = message.indexOf('\n');
    return idx >= 0 ? message.substring(0, idx) : message;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
