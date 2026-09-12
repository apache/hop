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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

class GitHubResourceClient implements GitResourceClient {

  private static final String ACCEPT = "application/vnd.github+json";
  private static final String PROVIDER = "github";
  private static final int GITHUB_MAX_PER_PAGE = 100;

  /**
   * GitHub returns HTTP 500 for deep page numbers; walk history with {@code sha} cursors instead.
   */
  private static final int GITHUB_PAGE_CHUNK = 10;

  /**
   * Smallest window to fall back to when GitHub fails to walk history from an anchor.
   *
   * <p>Some regions of a large repository's history make the commits endpoint answer 500 for a
   * given anchor and page size - reproducibly, and on the upstream repository as well as a fork, so
   * it is GitHub's traversal rather than anything about the request. The same anchor succeeds with
   * a smaller window, so the loader narrows its page size instead of abandoning the run.
   */
  private static final int GITHUB_MIN_PAGE_SIZE = 10;

  private final String apiBaseUrl;
  private final GitAuth auth;

  GitHubResourceClient(String apiBaseUrl, GitAuth auth) {
    this.apiBaseUrl = GitApiHttp.trimBase(apiBaseUrl);
    this.auth = auth;
  }

  @Override
  public GitResourceReader openReader(
      GitResourceType resourceType, String owner, String repository, GitListOptions options)
      throws HopException {

    return switch (resourceType) {
      case COMMITS ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(GITHUB_MAX_PER_PAGE),
              new CommitPageLoader(owner, repository, options));
      case COMMIT_FILES ->
          throw new HopException(
              "COMMIT_FILES is only supported for local repository source; use Source LOCAL");
      case ISSUE_COMMENTS, PR_COMMENTS ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(GITHUB_MAX_PER_PAGE),
              new IssueCommentsPageLoader(owner, repository, options, resourceType));
      case ISSUE_EVENTS ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(GITHUB_MAX_PER_PAGE),
              new IssueEventsPageLoader(owner, repository, options));
      default ->
          new PageBufferGitResourceReader(
              options.getMaxRecords(GITHUB_MAX_PER_PAGE),
              new PagedResourceLoader(resourceType, owner, repository, options));
    };
  }

  private final class CommitPageLoader implements GitResourcePageLoader {

    private final String owner;
    private final String repository;
    private final GitListOptions options;
    private final Set<String> seenShas = new HashSet<>();

    /** Narrowed when GitHub cannot walk history from the current anchor at the current size. */
    private int pageSize;

    private boolean narrowed;

    /** Newest commit already emitted, used to re-anchor past a region GitHub cannot traverse. */
    private String lastReadSha;

    private boolean reAnchored;

    private String cursorSha;
    private int page = 1;
    private int pagesLoadedInChunk;
    private boolean exhausted;

    CommitPageLoader(String owner, String repository, GitListOptions options) {
      this.owner = owner;
      this.repository = repository;
      this.options = options;
      this.pageSize = options.getEffectivePageSize(GITHUB_MAX_PER_PAGE);
      this.cursorSha = nonBlank(options.getBranch());
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted) {
        return List.of();
      }

      while (true) {
        JSONArray array = fetchCommitPage();
        if (array.isEmpty()) {
          exhausted = true;
          return List.of();
        }

        List<GitResourceRecord> pageRecords = new ArrayList<>(array.size());
        for (Object item : array) {
          JSONObject json = (JSONObject) item;
          GitResourceRecord record = mapRecord(GitResourceType.COMMITS, owner, repository, json);
          if (record == null) {
            continue;
          }
          // Re-anchoring on a sha re-reads the cursor commit itself, so the first record of a new
          // chunk repeats the last of the previous one.
          String sha = record.getSha();
          if (sha.isBlank() || !seenShas.add(sha)) {
            continue;
          }
          pageRecords.add(record);
          lastReadSha = sha;
        }

        String chunkOldestSha =
            GitApiHttp.getString((JSONObject) array.get(array.size() - 1), "sha");
        page++;
        pagesLoadedInChunk++;

        if (array.size() < pageSize) {
          exhausted = true;
          return pageRecords;
        }

        if (pagesLoadedInChunk >= GITHUB_PAGE_CHUNK) {
          // GitHub rejects deep page numbers on the commits endpoint, so walk history in chunks
          // anchored on the oldest sha seen instead of paging ever deeper.
          if (chunkOldestSha.isBlank() || chunkOldestSha.equals(cursorSha)) {
            exhausted = true;
          } else {
            cursorSha = chunkOldestSha;
            page = 1;
            pagesLoadedInChunk = 0;
          }
        }

        if (!pageRecords.isEmpty()) {
          return pageRecords;
        }
        if (exhausted) {
          return List.of();
        }
      }
    }

    /**
     * Reads one page of commits, narrowing the window if GitHub cannot traverse from this anchor.
     */
    private JSONArray fetchCommitPage() throws HopException {
      while (true) {
        String url = buildCommitsUrl(owner, repository, options, page, pageSize, cursorSha);
        try {
          return parseArray(
              GitApiHttp.get(url, auth, GitProvider.AuthStyle.BEARER, ACCEPT, "GitHub"));
        } catch (GitApiException e) {
          if (e.getStatusCode() != 500) {
            throw e;
          }
          // Re-anchor first. The failure is GitHub walking history from the current anchor, and a
          // deeper page from that same anchor keeps crossing whatever it cannot traverse - a
          // narrower window alone just fails a few pages later. Starting again from the newest
          // commit already read puts the request back on page 1 of a fresh anchor.
          if (lastReadSha != null && !lastReadSha.equals(cursorSha)) {
            cursorSha = lastReadSha;
            page = 1;
            pagesLoadedInChunk = 0;
            if (!reAnchored) {
              reAnchored = true;
              LogChannel.GENERAL.logBasic(
                  "GitHub could not continue reading the commit history of "
                      + owner
                      + "/"
                      + repository
                      + " from this point; continuing from the last commit read.");
            }
            continue;
          }
          // A fresh anchor still fails: try a smaller window before giving up on the run.
          if (pageSize > GITHUB_MIN_PAGE_SIZE) {
            pageSize = Math.max(GITHUB_MIN_PAGE_SIZE, pageSize / 2);
            if (!narrowed) {
              narrowed = true;
              LogChannel.GENERAL.logBasic(
                  "GitHub could not read the commit history of "
                      + owner
                      + "/"
                      + repository
                      + " in pages of the configured size. Continuing with "
                      + pageSize
                      + " commits per request.");
            }
            continue;
          }
          throw e;
        }
      }
    }

    @Override
    public boolean isExhausted() {
      return exhausted;
    }
  }

  private final class IssueCommentsPageLoader implements GitResourcePageLoader {

    /** Which half of the repository-wide comment feed this loader emits. */
    private final GitResourceType commentType;

    private final String owner;
    private final String repository;
    private final GitListOptions options;
    private final int pageSize;
    private int page = 1;
    private boolean exhausted;
    private String truncationNote;

    IssueCommentsPageLoader(
        String owner, String repository, GitListOptions options, GitResourceType commentType) {
      this.commentType = commentType;
      this.owner = owner;
      this.repository = repository;
      this.options = options;
      this.pageSize = options.getEffectivePageSize(GITHUB_MAX_PER_PAGE);
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted || !options.hasPageBudget(page)) {
        exhausted = true;
        return List.of();
      }

      StringBuilder url =
          new StringBuilder(apiBaseUrl)
              .append("/repos/")
              .append(encode(owner))
              .append("/")
              .append(encode(repository))
              .append("/issues/comments?per_page=")
              .append(pageSize)
              .append("&page=")
              .append(page);
      if (options.getSince() != null && !options.getSince().isBlank()) {
        url.append("&since=").append(encode(options.getSince()));
      }

      JSONArray array;
      try {
        array = fetchArray(url.toString(), page);
      } catch (GitProviderCapException e) {
        exhausted = true;
        truncationNote = e.getMessage();
        return List.of();
      }
      page++;

      if (array.isEmpty()) {
        exhausted = true;
        return List.of();
      }

      List<GitResourceRecord> pageRecords = new ArrayList<>(array.size());
      for (Object item : array) {
        JSONObject json = (JSONObject) item;
        // One feed carries comments on issues and on pull requests. They are told apart by the
        // comment's own URL: a pull request comment lives under /pull/, an issue one under
        // /issues/. The parent issue_url says /issues/ for both, so it cannot be used here.
        boolean onPullRequest = GitApiHttp.getString(json, "html_url").contains("/pull/");
        if (onPullRequest != (commentType == GitResourceType.PR_COMMENTS)) {
          continue;
        }
        pageRecords.add(mapIssueComment(owner, repository, json, commentType));
      }

      if (options.isLastPage(array.size(), GITHUB_MAX_PER_PAGE)) {
        exhausted = true;
      }
      return pageRecords;
    }

    @Override
    public boolean isExhausted() {
      return exhausted;
    }

    @Override
    public String getTruncationNote() {
      return truncationNote;
    }
  }

  private final class IssueEventsPageLoader extends IssueNestedPageLoader {

    private final String owner;
    private final String repository;
    private final String state;
    private final int pageSize;

    IssueEventsPageLoader(String owner, String repository, GitListOptions options) {
      super(options);
      this.owner = owner;
      this.repository = repository;
      this.state = normalizeState(options.getState());
      this.pageSize = options.getEffectivePageSize(GITHUB_MAX_PER_PAGE);
    }

    @Override
    protected boolean issuesLimitedByMaxPages() {
      return true;
    }

    @Override
    protected int activityPageSize() {
      return pageSize;
    }

    @Override
    protected List<IssueRef> fetchIssuePage(int page) throws HopException {
      String url =
          apiBaseUrl
              + "/repos/"
              + encode(owner)
              + "/"
              + encode(repository)
              + "/issues?per_page="
              + pageSize
              + "&page="
              + page
              + "&state="
              + encode(state);
      JSONArray array = fetchArray(url, page);
      if (array.isEmpty()) {
        issuesRunOut();
      }
      List<IssueRef> issues = new ArrayList<>(array.size());
      for (Object item : array) {
        JSONObject json = (JSONObject) item;
        // GitHub's issues feed carries pull requests too. ISSUES filters them out, so the event
        // walker has to as well or pull request events surface as issue events.
        if (json.get("pull_request") != null) {
          continue;
        }
        issues.add(
            new IssueRef(
                GitApiHttp.getString(json, "node_id"),
                GitApiHttp.getLong(json, "number"),
                GitApiHttp.getString(json, "title"),
                GitApiHttp.getString(json, "state")));
      }
      return issues;
    }

    @Override
    protected List<GitResourceRecord> fetchActivityPage(IssueRef issue, int page)
        throws HopException {
      String url =
          apiBaseUrl
              + "/repos/"
              + encode(owner)
              + "/"
              + encode(repository)
              + "/issues/"
              + issue.number()
              + "/events?per_page="
              + pageSize
              + "&page="
              + page;
      JSONArray array = fetchArray(url, page);
      List<GitResourceRecord> records = new ArrayList<>(array.size());
      for (Object item : array) {
        JSONObject json = (JSONObject) item;
        records.add(mapIssueEvent(owner, repository, issue, json));
      }
      return records;
    }
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
    private String truncationNote;

    PagedResourceLoader(
        GitResourceType resourceType, String owner, String repository, GitListOptions options) {
      this.resourceType = resourceType;
      this.owner = owner;
      this.repository = repository;
      this.options = options;
      this.state = normalizeState(options.getState());
      this.pageSize = options.getEffectivePageSize(GITHUB_MAX_PER_PAGE);
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted || !options.hasPageBudget(page)) {
        exhausted = true;
        return List.of();
      }

      String url = buildUrl(resourceType, owner, repository, options, state, page, pageSize, null);
      JSONArray array;
      try {
        array = fetchArray(url, page);
      } catch (GitProviderCapException e) {
        exhausted = true;
        truncationNote = e.getMessage();
        return List.of();
      }
      page++;

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

      if (options.isLastPage(array.size(), GITHUB_MAX_PER_PAGE)) {
        exhausted = true;
      }
      return pageRecords;
    }

    @Override
    public boolean isExhausted() {
      return exhausted;
    }

    @Override
    public String getTruncationNote() {
      return truncationNote;
    }
  }

  private JSONArray fetchArray(String url, int page) throws HopException {
    try {
      return parseArray(GitApiHttp.get(url, auth, GitProvider.AuthStyle.BEARER, ACCEPT, "GitHub"));
    } catch (GitApiException e) {
      if (page > 1 && e.getStatusCode() == 422) {
        // Documented GitHub behaviour rather than a failure: page-based pagination on this
        // endpoint is capped at 1,000 items (page 11 with per_page=100).
        throw new GitProviderCapException(
            "GitHub caps page-based pagination on this endpoint at 1,000 items; stopped at page "
                + page);
      }
      if (page > 1 && e.isServerError()) {
        throw new GitPaginationException("GitHub", page, e);
      }
      throw e;
    }
  }

  private String buildCommitsUrl(
      String owner,
      String repository,
      GitListOptions options,
      int page,
      int pageSize,
      String cursorSha) {
    return buildUrl(
        GitResourceType.COMMITS, owner, repository, options, "all", page, pageSize, cursorSha);
  }

  private String buildUrl(
      GitResourceType resourceType,
      String owner,
      String repository,
      GitListOptions options,
      String state,
      int page,
      int pageSize,
      String cursorSha) {

    String base =
        apiBaseUrl
            + "/repos/"
            + encode(owner)
            + "/"
            + encode(repository)
            + pathSuffix(resourceType)
            + "?per_page="
            + pageSize
            + "&page="
            + page;

    return switch (resourceType) {
      case COMMITS -> {
        String u = base;
        if (options.getSince() != null && !options.getSince().isBlank()) {
          u += "&since=" + encode(options.getSince());
        }
        if (cursorSha != null && !cursorSha.isBlank()) {
          u += "&sha=" + encode(cursorSha);
        }
        yield u;
      }
      case ISSUES, PULL_REQUESTS -> base + "&state=" + encode(state);
      case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES -> base;
    };
  }

  private static String pathSuffix(GitResourceType resourceType) {
    return switch (resourceType) {
      case COMMITS -> "/commits";
      case ISSUES -> "/issues";
      case PULL_REQUESTS -> "/pulls";
      case ISSUE_COMMENTS, PR_COMMENTS -> "/issues/comments";
      case ISSUE_EVENTS -> "/issues";
      case COMMIT_FILES -> throw new IllegalStateException();
    };
  }

  private GitResourceRecord mapIssueComment(
      String owner, String repository, JSONObject json, GitResourceType commentType) {
    JSONObject user = (JSONObject) json.get("user");
    long issueNumber = parseIssueNumberFromUrl(GitApiHttp.getString(json, "issue_url"));
    return GitIssueActivityMapper.comment(
        PROVIDER,
        commentType,
        owner,
        repository,
        issueNumber,
        "",
        "",
        GitApiHttp.getString(json, "id"),
        user != null ? GitApiHttp.getString(user, "login") : "",
        GitApiHttp.getString(json, "created_at"),
        GitApiHttp.getString(json, "updated_at"),
        GitApiHttp.getString(json, "html_url"),
        GitApiHttp.getString(json, "body"),
        json);
  }

  private GitResourceRecord mapIssueEvent(
      String owner, String repository, IssueNestedPageLoader.IssueRef issue, JSONObject json) {
    JSONObject actor = (JSONObject) json.get("actor");
    String eventType = GitApiHttp.getString(json, "event");
    return GitIssueActivityMapper.event(
        PROVIDER,
        owner,
        repository,
        issue.number(),
        issue.title(),
        issue.state(),
        GitApiHttp.getString(json, "id"),
        eventType,
        describeGitHubEvent(json, eventType),
        actor != null ? GitApiHttp.getString(actor, "login") : "",
        GitApiHttp.getString(json, "created_at"),
        "",
        json);
  }

  private static String describeGitHubEvent(JSONObject json, String eventType) {
    return switch (eventType) {
      case "labeled", "unlabeled" -> {
        JSONObject label = (JSONObject) json.get("label");
        yield label != null ? GitApiHttp.getString(label, "name") : "";
      }
      case "assigned", "unassigned" -> {
        JSONObject assignee = (JSONObject) json.get("assignee");
        yield assignee != null ? GitApiHttp.getString(assignee, "login") : "";
      }
      case "milestoned", "demilestoned" -> {
        JSONObject milestone = (JSONObject) json.get("milestone");
        yield milestone != null ? GitApiHttp.getString(milestone, "title") : "";
      }
      case "closed", "reopened" -> GitApiHttp.getString(json, "state_reason");
      case "renamed" -> {
        // The rename detail is an object holding the old and new title, not a string.
        Object rename = json.get("rename");
        if (rename instanceof org.json.simple.JSONObject renameObject) {
          String from = GitApiHttp.getString(renameObject, "from");
          String to = GitApiHttp.getString(renameObject, "to");
          yield from.isBlank() && to.isBlank() ? "" : from + " -> " + to;
        }
        yield "";
      }
      default -> GitApiHttp.getString(json, "commit_id");
    };
  }

  private static long parseIssueNumberFromUrl(String issueUrl) {
    if (issueUrl == null || issueUrl.isBlank()) {
      return 0L;
    }
    int slash = issueUrl.lastIndexOf('/');
    if (slash < 0 || slash == issueUrl.length() - 1) {
      return 0L;
    }
    try {
      return Long.parseLong(issueUrl.substring(slash + 1));
    } catch (NumberFormatException e) {
      return 0L;
    }
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
        JSONObject author = commit != null ? (JSONObject) commit.get("author") : null;
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
            commit != null ? GitApiHttp.getString(commit, "message") : "",
            GitApiHttp.getString(json, "sha"),
            "",
            "",
            "",
            rawJson);
      }
      case ISSUES, PULL_REQUESTS -> {
        JSONObject user = (JSONObject) json.get("user");
        JSONObject head =
            resourceType == GitResourceType.PULL_REQUESTS ? (JSONObject) json.get("head") : null;
        JSONObject base =
            resourceType == GitResourceType.PULL_REQUESTS ? (JSONObject) json.get("base") : null;
        yield new GitResourceRecord(
            PROVIDER,
            entityType,
            owner,
            repository,
            GitApiHttp.getString(json, "node_id"),
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
            head != null ? GitApiHttp.getString(head, "ref") : "",
            base != null ? GitApiHttp.getString(base, "ref") : "",
            mapGithubMerged(json),
            rawJson);
      }
      case ISSUE_COMMENTS, PR_COMMENTS, ISSUE_EVENTS, COMMIT_FILES ->
          throw new IllegalStateException();
    };
  }

  /** GitHub list-pulls responses often omit {@code merged}; {@code merged_at} is reliable. */
  static String mapGithubMerged(org.json.simple.JSONObject json) {
    Object merged = json.get("merged");
    if (merged instanceof Boolean booleanValue) {
      return booleanValue ? "Y" : "N";
    }
    String mergedAt = GitApiHttp.getString(json, "merged_at");
    return mergedAt.isBlank() ? "N" : "Y";
  }

  private static JSONArray parseArray(String body) throws HopException {
    JSONParser parser = new JSONParser();
    try {
      return (JSONArray) parser.parse(body);
    } catch (ParseException e) {
      throw new HopException("Failed to parse GitHub API response: " + e.getMessage(), e);
    }
  }

  private static String normalizeState(String state) {
    if (state == null || state.isBlank() || "all".equalsIgnoreCase(state)) {
      return "all";
    }
    return state.toLowerCase();
  }

  private static String nonBlank(String value) {
    return value == null || value.isBlank() ? null : value;
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
