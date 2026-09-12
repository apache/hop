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

class GitLabResourceClient implements GitResourceClient {

  private static final String ACCEPT = "application/json";
  private static final String PROVIDER = "gitlab";
  private static final int GITLAB_MAX_PER_PAGE = 100;

  private final String apiBaseUrl;
  private final GitAuth auth;

  GitLabResourceClient(String apiBaseUrl, GitAuth auth) {
    this.apiBaseUrl = GitApiHttp.trimBase(apiBaseUrl);
    this.auth = auth;
  }

  @Override
  public GitResourceReader openReader(
      GitResourceType resourceType, String owner, String repository, GitListOptions options)
      throws HopException {
    return switch (resourceType) {
      case COMMIT_FILES ->
          throw new HopException(
              "COMMIT_FILES is only supported for local repository source; use Source LOCAL");
      case ISSUE_COMMENTS, PR_COMMENTS ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(GITLAB_MAX_PER_PAGE),
              new IssueNotesPageLoader(owner, repository, options, resourceType));
      case ISSUE_EVENTS ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(GITLAB_MAX_PER_PAGE),
              new IssueNotesPageLoader(owner, repository, options, GitResourceType.ISSUE_EVENTS));
      default ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(GITLAB_MAX_PER_PAGE),
              new PagedResourceLoader(resourceType, owner, repository, options));
    };
  }

  private final class IssueNotesPageLoader extends IssueNestedPageLoader {

    private final String owner;
    private final String repository;
    private final String projectId;
    private final String state;
    private final int pageSize;
    private final GitResourceType activityType;

    IssueNotesPageLoader(
        String owner, String repository, GitListOptions options, GitResourceType activityType) {
      super(options);
      this.owner = owner;
      this.repository = repository;
      this.projectId = encode(owner + "/" + repository);
      this.state = normalizeState(options.getState());
      this.pageSize = options.getEffectivePageSize(GITLAB_MAX_PER_PAGE);
      this.activityType = activityType;
    }

    @Override
    protected boolean issuesLimitedByMaxPages() {
      return true;
    }

    @Override
    protected int activityPageSize() {
      return pageSize;
    }

    /** Merge request notes hang off /merge_requests; issue notes off /issues. */
    private String parentPath() {
      return activityType == GitResourceType.PR_COMMENTS ? "/merge_requests" : "/issues";
    }

    @Override
    protected List<IssueRef> fetchIssuePage(int page) throws HopException {
      StringBuilder url =
          new StringBuilder(apiBaseUrl)
              .append("/projects/")
              .append(projectId)
              .append(parentPath())
              .append("?per_page=")
              .append(pageSize)
              .append("&page=")
              .append(page);
      if (!"all".equals(state)) {
        url.append("&state=").append(encode(state));
      }
      JSONArray array = parseArray(getGitLab(url.toString()));
      if (array.isEmpty()) {
        issuesRunOut();
      }
      List<IssueRef> issues = new ArrayList<>(array.size());
      for (Object item : array) {
        JSONObject json = (JSONObject) item;
        issues.add(
            new IssueRef(
                GitApiHttp.getString(json, "id"),
                GitApiHttp.getLong(json, "iid"),
                GitApiHttp.getString(json, "title"),
                GitApiHttp.getString(json, "state")));
      }
      return issues;
    }

    @Override
    protected List<GitResourceRecord> fetchActivityPage(IssueRef issue, int page)
        throws HopException {
      String filter =
          activityType == GitResourceType.ISSUE_EVENTS ? "only_activity" : "only_comments";
      String url =
          apiBaseUrl
              + "/projects/"
              + projectId
              + parentPath()
              + "/"
              + issue.number()
              + "/notes?per_page="
              + pageSize
              + "&page="
              + page
              + "&activity_filter="
              + filter;
      JSONArray array = parseArray(getGitLab(url));
      List<GitResourceRecord> records = new ArrayList<>(array.size());
      for (Object item : array) {
        JSONObject json = (JSONObject) item;
        if (activityType == GitResourceType.ISSUE_COMMENTS) {
          records.add(mapGitLabComment(owner, repository, issue, json, activityType));
        } else {
          records.add(mapGitLabEvent(owner, repository, issue, json));
        }
      }
      return records;
    }

    private String getGitLab(String url) throws HopException {
      return GitApiHttp.get(
          url, auth, GitProvider.AuthStyle.GITLAB_PRIVATE_TOKEN, ACCEPT, "GitLab");
    }
  }

  private GitResourceRecord mapGitLabComment(
      String owner,
      String repository,
      IssueNestedPageLoader.IssueRef issue,
      JSONObject json,
      GitResourceType commentType) {
    JSONObject author = (JSONObject) json.get("author");
    return GitIssueActivityMapper.comment(
        PROVIDER,
        commentType,
        owner,
        repository,
        issue.number(),
        issue.title(),
        issue.state(),
        GitApiHttp.getString(json, "id"),
        author != null ? GitApiHttp.getString(author, "username") : "",
        GitApiHttp.getString(json, "created_at"),
        GitApiHttp.getString(json, "updated_at"),
        "",
        GitApiHttp.getString(json, "body"),
        json);
  }

  private GitResourceRecord mapGitLabEvent(
      String owner, String repository, IssueNestedPageLoader.IssueRef issue, JSONObject json) {
    JSONObject author = (JSONObject) json.get("author");
    String type = GitApiHttp.getString(json, "type");
    if (type.isBlank()) {
      type = Boolean.TRUE.equals(json.get("system")) ? "system" : "note";
    }
    return GitIssueActivityMapper.event(
        PROVIDER,
        owner,
        repository,
        issue.number(),
        issue.title(),
        issue.state(),
        GitApiHttp.getString(json, "id"),
        type,
        GitApiHttp.getString(json, "body"),
        author != null ? GitApiHttp.getString(author, "username") : "",
        GitApiHttp.getString(json, "created_at"),
        "",
        json);
  }

  private final class PagedResourceLoader implements GitResourcePageLoader {

    private final GitResourceType resourceType;
    private final String owner;
    private final String repository;
    private final GitListOptions options;
    private final String projectId;
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
      this.projectId = encode(owner + "/" + repository);
      this.state = normalizeState(options.getState());
      this.pageSize = options.getEffectivePageSize(GITLAB_MAX_PER_PAGE);
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted || !options.hasPageBudget(page)) {
        exhausted = true;
        return List.of();
      }

      String url = buildUrl(resourceType, projectId, options, state, page, pageSize);
      String body;
      try {
        body =
            GitApiHttp.get(url, auth, GitProvider.AuthStyle.GITLAB_PRIVATE_TOKEN, ACCEPT, "GitLab");
      } catch (GitApiException e) {
        if (page > 1 && e.isServerError()) {
          throw new GitPaginationException("GitLab", page, e);
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
        pageRecords.add(mapRecord(resourceType, owner, repository, json));
      }

      if (options.isLastPage(array.size(), 100)) {
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
      String projectId,
      GitListOptions options,
      String state,
      int page,
      int pageSize) {

    String path =
        switch (resourceType) {
          case COMMITS -> "/projects/" + projectId + "/repository/commits";
          case ISSUES -> "/projects/" + projectId + "/issues";
          case PULL_REQUESTS -> "/projects/" + projectId + "/merge_requests";
          case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES ->
              throw new IllegalStateException();
        };

    StringBuilder url =
        new StringBuilder(apiBaseUrl)
            .append(path)
            .append("?per_page=")
            .append(pageSize)
            .append("&page=")
            .append(page);

    if (resourceType != GitResourceType.COMMITS && !"all".equals(state)) {
      url.append("&state=").append(encode(state));
    }
    if (resourceType == GitResourceType.COMMITS
        && options.getSince() != null
        && !options.getSince().isBlank()) {
      url.append("&since=").append(encode(options.getSince()));
    }
    if (resourceType == GitResourceType.COMMITS
        && options.getBranch() != null
        && !options.getBranch().isBlank()) {
      url.append("&ref_name=").append(encode(options.getBranch()));
    }
    return url.toString();
  }

  private GitResourceRecord mapRecord(
      GitResourceType resourceType, String owner, String repository, JSONObject json) {

    String entityType = resourceType.getEntityType();
    String rawJson = json.toJSONString();

    return switch (resourceType) {
      case COMMITS ->
          new GitResourceRecord(
              PROVIDER,
              entityType,
              owner,
              repository,
              GitApiHttp.getString(json, "id"),
              0L,
              firstLine(GitApiHttp.getString(json, "message")),
              "",
              GitApiHttp.getString(json, "author_name"),
              GitApiHttp.getString(json, "created_at"),
              "",
              "",
              GitApiHttp.getString(json, "web_url"),
              GitApiHttp.getString(json, "message"),
              GitApiHttp.getString(json, "id"),
              "",
              "",
              "",
              rawJson);
      case ISSUES, PULL_REQUESTS -> {
        JSONObject author = (JSONObject) json.get("author");
        yield new GitResourceRecord(
            PROVIDER,
            entityType,
            owner,
            repository,
            GitApiHttp.getString(json, "id"),
            GitApiHttp.getLong(json, "iid"),
            GitApiHttp.getString(json, "title"),
            GitApiHttp.getString(json, "state"),
            author != null ? GitApiHttp.getString(author, "username") : "",
            GitApiHttp.getString(json, "created_at"),
            GitApiHttp.getString(json, "updated_at"),
            GitApiHttp.getString(json, "closed_at"),
            GitApiHttp.getString(json, "web_url"),
            GitApiHttp.getString(json, "description"),
            "",
            GitApiHttp.getString(json, "source_branch"),
            GitApiHttp.getString(json, "target_branch"),
            "merged".equalsIgnoreCase(GitApiHttp.getString(json, "state")) ? "Y" : "N",
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
      throw new HopException("Failed to parse GitLab API response: " + e.getMessage(), e);
    }
  }

  private static String normalizeState(String state) {
    if (state == null || state.isBlank() || "all".equalsIgnoreCase(state)) {
      return "all";
    }
    if ("closed".equalsIgnoreCase(state)) {
      return "closed";
    }
    return "opened";
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
