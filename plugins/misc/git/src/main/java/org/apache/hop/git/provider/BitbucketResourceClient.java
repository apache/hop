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

class BitbucketResourceClient implements GitResourceClient {

  private static final String ACCEPT = "application/json";
  private static final String PROVIDER = "bitbucket";
  private static final int BITBUCKET_MAX_PAGE_LEN = 100;

  private final String apiBaseUrl;
  private final GitAuth auth;

  BitbucketResourceClient(String apiBaseUrl, GitAuth auth) {
    this.apiBaseUrl = GitApiHttp.trimBase(apiBaseUrl);
    this.auth = auth;
  }

  @Override
  public GitResourceReader openReader(
      GitResourceType resourceType, String owner, String repository, GitListOptions options)
      throws HopException {
    if (auth == null || !auth.isBasicAuth()) {
      throw new HopException("Bitbucket requires a username and app password on the connection.");
    }
    return switch (resourceType) {
      case COMMIT_FILES ->
          throw new HopException(
              "COMMIT_FILES is only supported for local repository source; use Source LOCAL");
      case ISSUE_COMMENTS, PR_COMMENTS ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(BITBUCKET_MAX_PAGE_LEN),
              new IssueActivityPageLoader(
                  owner, repository, options, GitResourceType.ISSUE_COMMENTS));
      case ISSUE_EVENTS ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(BITBUCKET_MAX_PAGE_LEN),
              new IssueActivityPageLoader(
                  owner, repository, options, GitResourceType.ISSUE_EVENTS));
      default ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(BITBUCKET_MAX_PAGE_LEN),
              new PagedResourceLoader(resourceType, owner, repository, options));
    };
  }

  private final class IssueActivityPageLoader extends IssueNestedPageLoader {

    private final String owner;
    private final String repository;
    private final String state;
    private final GitResourceType activityType;

    IssueActivityPageLoader(
        String owner, String repository, GitListOptions options, GitResourceType activityType) {
      super(options);
      this.owner = owner;
      this.repository = repository;
      this.state = options.getState();
      this.activityType = activityType;
    }

    @Override
    protected boolean issuesLimitedByMaxPages() {
      return true;
    }

    @Override
    protected int activityPageSize() {
      return options.getEffectivePageSize(BITBUCKET_MAX_PAGE_LEN);
    }

    /** The parent this activity hangs off: an issue, or a pull request for PR comments. */
    private GitResourceType parentType() {
      return activityType == GitResourceType.PR_COMMENTS
          ? GitResourceType.PULL_REQUESTS
          : GitResourceType.ISSUES;
    }

    @Override
    protected List<IssueRef> fetchIssuePage(int page) throws HopException {
      String url = buildUrl(parentType(), owner, repository, options, state, page);
      JSONObject response = parseObject(getBitbucket(url));
      JSONArray values = (JSONArray) response.get("values");
      if (values == null || values.isEmpty()) {
        issuesRunOut();
        return List.of();
      }
      List<IssueRef> issues = new ArrayList<>(values.size());
      for (Object item : values) {
        JSONObject json = (JSONObject) item;
        issues.add(
            new IssueRef(
                GitApiHttp.getString(json, "id"),
                GitApiHttp.getLong(json, "id"),
                GitApiHttp.getString(json, "title"),
                GitApiHttp.getString(json, "state")));
      }
      return issues;
    }

    @Override
    protected List<GitResourceRecord> fetchActivityPage(IssueRef issue, int page)
        throws HopException {
      String suffix = activityType == GitResourceType.ISSUE_EVENTS ? "/changes" : "/comments";
      String parentPath =
          activityType == GitResourceType.PR_COMMENTS ? "/pullrequests/" : "/issues/";
      String url =
          apiBaseUrl
              + "/repositories/"
              + encode(owner)
              + "/"
              + encode(repository)
              + parentPath
              + encode(issue.id())
              + suffix
              + "?pagelen="
              + options.getEffectivePageSize(BITBUCKET_MAX_PAGE_LEN)
              + "&page="
              + page;
      JSONObject response = parseObject(getBitbucket(url));
      JSONArray values = (JSONArray) response.get("values");
      if (values == null || values.isEmpty()) {
        return List.of();
      }
      List<GitResourceRecord> records = new ArrayList<>(values.size());
      for (Object item : values) {
        JSONObject json = (JSONObject) item;
        if (activityType != GitResourceType.ISSUE_EVENTS) {
          records.add(mapBitbucketComment(owner, repository, issue, json, activityType));
        } else {
          records.add(mapBitbucketChange(owner, repository, issue, json));
        }
      }
      return records;
    }

    private String getBitbucket(String url) throws HopException {
      return GitApiHttp.get(url, auth, GitProvider.AuthStyle.BASIC, ACCEPT, "Bitbucket");
    }
  }

  private GitResourceRecord mapBitbucketComment(
      String owner,
      String repository,
      IssueNestedPageLoader.IssueRef issue,
      JSONObject json,
      GitResourceType commentType) {
    JSONObject user = (JSONObject) json.get("user");
    return GitIssueActivityMapper.comment(
        PROVIDER,
        commentType,
        owner,
        repository,
        issue.number(),
        issue.title(),
        issue.state(),
        GitApiHttp.getString(json, "id"),
        user != null ? GitApiHttp.getString(user, "display_name") : "",
        GitApiHttp.getString(json, "created_on"),
        GitApiHttp.getString(json, "updated_on"),
        extractLink(json, "html"),
        renderedText(json, "content"),
        json);
  }

  private GitResourceRecord mapBitbucketChange(
      String owner, String repository, IssueNestedPageLoader.IssueRef issue, JSONObject json) {
    JSONObject user = (JSONObject) json.get("user");
    String changeType = GitApiHttp.getString(json, "type");
    return GitIssueActivityMapper.event(
        PROVIDER,
        owner,
        repository,
        issue.number(),
        issue.title(),
        issue.state(),
        GitApiHttp.getString(json, "id"),
        changeType,
        renderedText(json, "message"),
        user != null ? GitApiHttp.getString(user, "display_name") : "",
        GitApiHttp.getString(json, "created_on"),
        extractLink(json, "html"),
        json);
  }

  private final class PagedResourceLoader implements GitResourcePageLoader {

    private final GitResourceType resourceType;
    private final String owner;
    private final String repository;
    private final GitListOptions options;
    private final String state;
    private int page = 1;
    private boolean exhausted;

    PagedResourceLoader(
        GitResourceType resourceType, String owner, String repository, GitListOptions options) {
      this.resourceType = resourceType;
      this.owner = owner;
      this.repository = repository;
      this.options = options;
      this.state = options.getState();
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted || !options.hasPageBudget(page)) {
        exhausted = true;
        return List.of();
      }

      String url = buildUrl(resourceType, owner, repository, options, state, page);
      String body;
      try {
        body = GitApiHttp.get(url, auth, GitProvider.AuthStyle.BASIC, ACCEPT, "Bitbucket");
      } catch (GitApiException e) {
        if (page > 1 && e.isServerError()) {
          throw new GitPaginationException("Bitbucket", page, e);
        }
        throw e;
      }
      page++;

      JSONObject response = parseObject(body);
      JSONArray values = (JSONArray) response.get("values");
      if (values == null || values.isEmpty()) {
        exhausted = true;
        return List.of();
      }

      List<GitResourceRecord> pageRecords = new ArrayList<>(values.size());
      for (Object item : values) {
        JSONObject json = (JSONObject) item;
        pageRecords.add(mapRecord(resourceType, owner, repository, json));
      }

      if (!hasNextPage(response)) {
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
      String workspace,
      String repository,
      GitListOptions options,
      String state,
      int page) {

    String path =
        switch (resourceType) {
          case COMMITS ->
              "/repositories/" + encode(workspace) + "/" + encode(repository) + "/commits";
          case ISSUES ->
              "/repositories/" + encode(workspace) + "/" + encode(repository) + "/issues";
          case PULL_REQUESTS ->
              "/repositories/" + encode(workspace) + "/" + encode(repository) + "/pullrequests";
          case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES ->
              throw new IllegalStateException();
        };

    StringBuilder url =
        new StringBuilder(apiBaseUrl)
            .append(path)
            .append("?pagelen=")
            .append(options.getEffectivePageSize(BITBUCKET_MAX_PAGE_LEN))
            .append("&page=")
            .append(page);

    url.append(stateQuery(resourceType, state));
    return url.toString();
  }

  private GitResourceRecord mapRecord(
      GitResourceType resourceType, String owner, String repository, JSONObject json) {

    String entityType = resourceType.getEntityType();
    String rawJson = json.toJSONString();

    return switch (resourceType) {
      case COMMITS -> {
        JSONObject author = (JSONObject) json.get("author");
        JSONObject user = author != null ? (JSONObject) author.get("user") : null;
        yield new GitResourceRecord(
            PROVIDER,
            entityType,
            owner,
            repository,
            GitApiHttp.getString(json, "hash"),
            0L,
            firstLine(renderedText(json, "message")),
            "",
            user != null ? GitApiHttp.getString(user, "display_name") : "",
            GitApiHttp.getString(json, "date"),
            "",
            "",
            extractLink(json, "html"),
            renderedText(json, "message"),
            GitApiHttp.getString(json, "hash"),
            "",
            "",
            "",
            rawJson);
      }
      case ISSUES, PULL_REQUESTS -> {
        JSONObject reporter =
            resourceType == GitResourceType.ISSUES
                ? (JSONObject) json.get("reporter")
                : (JSONObject) json.get("author");
        JSONObject source =
            resourceType == GitResourceType.PULL_REQUESTS ? (JSONObject) json.get("source") : null;
        JSONObject destination =
            resourceType == GitResourceType.PULL_REQUESTS
                ? (JSONObject) json.get("destination")
                : null;
        yield new GitResourceRecord(
            PROVIDER,
            entityType,
            owner,
            repository,
            GitApiHttp.getString(json, "id"),
            GitApiHttp.getLong(json, "id"),
            GitApiHttp.getString(json, "title"),
            GitApiHttp.getString(json, "state"),
            reporter != null ? GitApiHttp.getString(reporter, "display_name") : "",
            GitApiHttp.getString(json, "created_on"),
            GitApiHttp.getString(json, "updated_on"),
            "",
            extractLink(json, "html"),
            renderedText(json, "content"),
            "",
            branchName(source),
            branchName(destination),
            "MERGED".equalsIgnoreCase(GitApiHttp.getString(json, "state")) ? "Y" : "N",
            rawJson);
      }
      case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES ->
          throw new IllegalStateException();
    };
  }

  private static String branchName(JSONObject ref) {
    if (ref == null) {
      return "";
    }
    JSONObject branch = (JSONObject) ref.get("branch");
    return branch != null ? GitApiHttp.getString(branch, "name") : "";
  }

  private static String extractLink(JSONObject json, String linkName) {
    JSONObject links = (JSONObject) json.get("links");
    if (links == null) {
      return "";
    }
    JSONObject link = (JSONObject) links.get(linkName);
    if (link == null) {
      return "";
    }
    return GitApiHttp.getString(link, "href");
  }

  private static boolean hasNextPage(JSONObject response) {
    return response.get("next") != null;
  }

  private static JSONObject parseObject(String body) throws HopException {
    JSONParser parser = new JSONParser();
    try {
      return (JSONObject) parser.parse(body);
    } catch (ParseException e) {
      throw new HopException("Failed to parse Bitbucket API response: " + e.getMessage(), e);
    }
  }

  /**
   * Bitbucket returns comment bodies and commit messages as rendered objects holding {@code raw}
   * and {@code html}. Reading the object itself put a JSON blob in the body output column.
   */
  private static String renderedText(JSONObject json, String key) {
    Object value = json == null ? null : json.get(key);
    if (value instanceof JSONObject rendered) {
      return GitApiHttp.getString(rendered, "raw");
    }
    return value != null ? value.toString() : "";
  }

  /**
   * Bitbucket has no "closed" or "all" pull request state, and it does not reject an unknown one:
   * {@code state=CLOSED} returns every pull request, so a user asking for closed ones silently got
   * the open ones too. The states are combined by repeating the parameter instead.
   *
   * @return the query fragment to append, or an empty string to accept the endpoint default
   */
  private static String stateQuery(GitResourceType resourceType, String state) {
    if (resourceType == GitResourceType.COMMITS) {
      return "";
    }
    boolean pullRequests = resourceType == GitResourceType.PULL_REQUESTS;
    String[] values;
    if (state == null || state.isBlank() || "all".equalsIgnoreCase(state)) {
      values =
          pullRequests
              ? new String[] {"OPEN", "MERGED", "DECLINED", "SUPERSEDED"}
              : new String[] {
                "new", "open", "resolved", "closed", "on hold", "invalid", "duplicate", "wontfix"
              };
    } else if ("closed".equalsIgnoreCase(state)) {
      values =
          pullRequests
              ? new String[] {"MERGED", "DECLINED", "SUPERSEDED"}
              : new String[] {"resolved", "closed", "invalid", "duplicate", "wontfix"};
    } else {
      values = pullRequests ? new String[] {"OPEN"} : new String[] {"new", "open"};
    }
    StringBuilder query = new StringBuilder();
    for (String value : values) {
      query.append("&state=").append(encode(value));
    }
    return query.toString();
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
