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

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.exception.HopException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.util.io.DisabledOutputStream;
import org.json.simple.JSONObject;

/** Reads commit history from a local clone using JGit. */
public final class LocalGitResourceClient {

  private static final String PROVIDER = "local";

  /**
   * Matches {@code owner/repo} at the end of a remote URL, in both SSH ({@code
   * git@host:owner/repo}) and HTTPS form. The repository group allows dots so names like {@code
   * my.repo} survive; only a trailing {@code .git} is stripped.
   */
  private static final Pattern REMOTE_OWNER_REPO =
      Pattern.compile("[:/]([^/:]+)/([^/]+?)(?:\\.git)?/?$");

  private LocalGitResourceClient() {}

  public static GitResourceReader openReader(
      String repositoryPath, GitListOptions options, GitResourceType resourceType)
      throws HopException {
    LocalRepositoryInfo info = LocalRepositoryInfo.open(repositoryPath);
    GitResourcePageLoader loader =
        switch (resourceType) {
          case COMMITS -> new CommitPageLoader(info, options);
          case COMMIT_FILES -> new CommitFilesPageLoader(info, options);
          default ->
              throw new HopException(
                  "Local repository source does not support " + resourceType.name());
        };
    return new PageBufferGitResourceReader(GitListOptions.UNLIMITED_MAX_RECORDS, loader);
  }

  public static List<String> listBranches(String repositoryPath) throws HopException {
    LocalRepositoryInfo info = LocalRepositoryInfo.open(repositoryPath);
    try (Git git = Git.open(info.directory())) {
      List<String> branches = new ArrayList<>();
      for (Ref ref : git.branchList().call()) {
        branches.add(Repository.shortenRefName(ref.getName()));
      }
      Collections.sort(branches);
      return branches;
    } catch (GitAPIException | IOException e) {
      throw new HopException("Failed to list branches in local repository", e);
    }
  }

  public static LocalRepositoryInfo openRepositoryInfo(String repositoryPath) throws HopException {
    return LocalRepositoryInfo.open(repositoryPath);
  }

  public static final class LocalRepositoryInfo {

    private final File directory;
    private final String owner;
    private final String repositoryName;
    private final String remoteUrl;

    LocalRepositoryInfo(File directory, String owner, String repositoryName, String remoteUrl) {
      this.directory = directory;
      this.owner = owner;
      this.repositoryName = repositoryName;
      this.remoteUrl = remoteUrl;
    }

    static LocalRepositoryInfo open(String repositoryPath) throws HopException {
      if (repositoryPath == null || repositoryPath.isBlank()) {
        throw new HopException("Local repository path is required");
      }
      File path = new File(repositoryPath.trim());
      if (!path.exists()) {
        throw new HopException("Local repository path does not exist: " + path.getAbsolutePath());
      }
      try {
        File gitDir = path;
        if (path.isDirectory() && new File(path, ".git").exists()) {
          gitDir = path;
        } else if (path.getName().equals(".git") && path.isDirectory()) {
          gitDir = path.getParentFile();
        }
        try (Repository repo = Git.open(gitDir).getRepository()) {
          // A bare clone has no work tree and JGit throws rather than returning null, so the
          // repository name has to come from the git directory in that case.
          String repoName;
          try {
            File workTree = repo.getWorkTree();
            repoName = workTree != null ? workTree.getName() : gitDir.getName();
          } catch (org.eclipse.jgit.errors.NoWorkTreeException e) {
            String bareName = gitDir.getName();
            repoName =
                bareName.endsWith(".git")
                    ? bareName.substring(0, bareName.length() - ".git".length())
                    : bareName;
          }
          String remoteUrl = repo.getConfig().getString("remote", "origin", "url");
          String owner = "local";
          if (remoteUrl != null && !remoteUrl.isBlank()) {
            Matcher matcher = REMOTE_OWNER_REPO.matcher(remoteUrl.trim());
            if (matcher.find()) {
              owner = matcher.group(1);
              repoName = matcher.group(2);
            }
          }
          return new LocalRepositoryInfo(
              gitDir, owner, repoName, remoteUrl == null ? "" : remoteUrl);
        }
      } catch (Exception e) {
        throw new HopException(
            "Failed to open local Git repository at "
                + path.getAbsolutePath()
                + ": "
                + e.getMessage(),
            e);
      }
    }

    File directory() {
      return directory;
    }

    public String getOwner() {
      return owner;
    }

    public String getRepositoryName() {
      return repositoryName;
    }

    public String getRemoteUrl() {
      return remoteUrl;
    }
  }

  /** Shared commit walk for {@link CommitPageLoader} and {@link CommitFilesPageLoader}. */
  private abstract static class LocalCommitWalkLoader implements GitResourcePageLoader {

    protected final LocalRepositoryInfo repositoryInfo;
    protected final GitListOptions options;
    protected final int pageSize;
    protected final Instant sinceInstant;

    protected Git git;
    protected RevWalk walk;
    protected java.util.Iterator<RevCommit> commitIterator;
    protected boolean exhausted;

    @Override
    public boolean isExhausted() {
      return exhausted;
    }

    LocalCommitWalkLoader(LocalRepositoryInfo repositoryInfo, GitListOptions options)
        throws HopException {
      this.repositoryInfo = repositoryInfo;
      this.options = options;
      this.pageSize = options.getPageSize();
      this.sinceInstant = parseSince(options.getSince());
    }

    protected RevCommit nextMatchingCommit() {
      while (commitIterator.hasNext()) {
        RevCommit commit = commitIterator.next();
        if (sinceInstant != null) {
          Instant commitTime = Instant.ofEpochSecond(commit.getCommitTime());
          if (commitTime.isBefore(sinceInstant)) {
            continue;
          }
        }
        return commit;
      }
      return null;
    }

    protected void ensureOpen() throws HopException {
      if (walk != null) {
        return;
      }
      try {
        git = Git.open(repositoryInfo.directory());
        Repository repo = git.getRepository();
        walk = new RevWalk(repo);
        ObjectId start = resolveStart(repo, options.getBranch());
        walk.markStart(walk.parseCommit(start));
        commitIterator = walk.iterator();
      } catch (Exception e) {
        closeQuietly();
        throw new HopException("Failed to initialize local commit walk", e);
      }
    }

    protected void closeQuietly() {
      commitIterator = null;
      if (walk != null) {
        walk.close();
        walk = null;
      }
      if (git != null) {
        git.close();
        git = null;
      }
    }

    @Override
    public void close() {
      closeQuietly();
    }
  }

  private static final class CommitPageLoader extends LocalCommitWalkLoader {

    CommitPageLoader(LocalRepositoryInfo repositoryInfo, GitListOptions options)
        throws HopException {
      super(repositoryInfo, options);
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted) {
        return List.of();
      }

      ensureOpen();
      List<GitResourceRecord> pageRecords = new ArrayList<>(pageSize);

      try {
        while (pageRecords.size() < pageSize) {
          RevCommit commit = nextMatchingCommit();
          if (commit == null) {
            exhausted = true;
            break;
          }
          pageRecords.add(toCommitRecord(repositoryInfo, commit));
        }
      } catch (Exception e) {
        closeQuietly();
        throw new HopException("Failed to read commits from local repository", e);
      }

      if (exhausted) {
        closeQuietly();
      }
      return pageRecords;
    }
  }

  private static final class CommitFilesPageLoader extends LocalCommitWalkLoader {

    private List<GitResourceRecord> pendingFileRecords;
    private int pendingFileIndex;

    CommitFilesPageLoader(LocalRepositoryInfo repositoryInfo, GitListOptions options)
        throws HopException {
      super(repositoryInfo, options);
    }

    @Override
    public List<GitResourceRecord> loadNextPage() throws HopException {
      if (exhausted) {
        return List.of();
      }

      ensureOpen();
      List<GitResourceRecord> pageRecords = new ArrayList<>(pageSize);

      try {
        Repository repo = git.getRepository();
        while (pageRecords.size() < pageSize) {
          List<GitResourceRecord> fileRecords = pendingFileRecords;
          if (fileRecords == null) {
            RevCommit commit = nextMatchingCommit();
            if (commit == null) {
              exhausted = true;
              break;
            }
            fileRecords = diffCommitFiles(repositoryInfo, repo, walk, commit);
            pendingFileIndex = 0;
          } else {
            pendingFileRecords = null;
          }

          while (pendingFileIndex < fileRecords.size() && pageRecords.size() < pageSize) {
            pageRecords.add(fileRecords.get(pendingFileIndex++));
          }

          if (pendingFileIndex < fileRecords.size()) {
            pendingFileRecords = fileRecords;
            break;
          }
          pendingFileIndex = 0;
        }
      } catch (Exception e) {
        closeQuietly();
        throw new HopException("Failed to read commit file changes from local repository", e);
      }

      if (exhausted && pendingFileRecords == null) {
        closeQuietly();
      }
      return pageRecords;
    }
  }

  private static List<GitResourceRecord> diffCommitFiles(
      LocalRepositoryInfo info, Repository repo, RevWalk revWalk, RevCommit commit)
      throws IOException {

    AbstractTreeIterator oldTree = oldTreeIterator(repo, revWalk, commit);
    CanonicalTreeParser newTree = new CanonicalTreeParser();
    try (ObjectReader reader = repo.newObjectReader()) {
      newTree.reset(reader, commit.getTree());
      try (DiffFormatter formatter = new DiffFormatter(DisabledOutputStream.INSTANCE)) {
        formatter.setRepository(repo);
        // Off by default in JGit, which would report every rename as a delete plus an add and make
        // the "renamed" and "copied" change types unreachable.
        formatter.setDetectRenames(true);
        List<DiffEntry> entries = formatter.scan(oldTree, newTree);
        List<GitResourceRecord> records = new ArrayList<>(entries.size());
        for (DiffEntry entry : entries) {
          records.add(toFileRecord(info, commit, entry));
        }
        return records;
      }
    }
  }

  private static AbstractTreeIterator oldTreeIterator(
      Repository repo, RevWalk revWalk, RevCommit commit) throws IOException {
    if (commit.getParentCount() == 0) {
      return new EmptyTreeIterator();
    }
    RevCommit parent = revWalk.parseCommit(commit.getParent(0).getId());
    CanonicalTreeParser oldTree = new CanonicalTreeParser();
    try (ObjectReader reader = repo.newObjectReader()) {
      oldTree.reset(reader, parent.getTree());
    }
    return oldTree;
  }

  /** JGit uses {@code /dev/null} for the missing side of an add or a delete; Hop wants an empty. */
  private static String realPath(String path) {
    return path == null || DiffEntry.DEV_NULL.equals(path) ? "" : path;
  }

  private static GitResourceRecord toFileRecord(
      LocalRepositoryInfo info, RevCommit commit, DiffEntry entry) {
    String changeType = mapChangeType(entry.getChangeType());
    String oldPath = realPath(entry.getOldPath());
    String newPath = realPath(entry.getNewPath());
    String filePath = pickFilePath(entry.getChangeType(), oldPath, newPath);
    String author = commit.getAuthorIdent().getName();
    String createdAt = Instant.ofEpochSecond(commit.getCommitTime()).toString();
    String sha = commit.getName();
    String url =
        info.getRemoteUrl().isBlank() ? info.directory().getAbsolutePath() : info.getRemoteUrl();

    JSONObject raw = new JSONObject();
    raw.put("commit_sha", sha);
    raw.put("change_type", changeType);
    raw.put("old_path", oldPath);
    raw.put("new_path", newPath);

    return new GitResourceRecord(
        PROVIDER,
        GitResourceType.COMMIT_FILES.getEntityType(),
        info.getOwner(),
        info.getRepositoryName(),
        sha + ":" + filePath,
        0L,
        filePath,
        changeType,
        author,
        createdAt,
        "",
        "",
        url,
        oldPath,
        sha,
        "",
        "",
        "",
        raw.toJSONString());
  }

  static String mapChangeType(DiffEntry.ChangeType changeType) {
    return switch (changeType) {
      case ADD -> "added";
      case MODIFY -> "modified";
      case DELETE -> "deleted";
      case RENAME -> "renamed";
      case COPY -> "copied";
    };
  }

  private static String pickFilePath(
      DiffEntry.ChangeType changeType, String oldPath, String newPath) {
    if (changeType == DiffEntry.ChangeType.DELETE) {
      return oldPath;
    }
    return newPath;
  }

  private static GitResourceRecord toCommitRecord(LocalRepositoryInfo info, RevCommit commit) {
    String message = commit.getFullMessage() == null ? "" : commit.getFullMessage();
    String author = commit.getAuthorIdent().getName();
    String createdAt = Instant.ofEpochSecond(commit.getCommitTime()).toString();
    String sha = commit.getName();
    String url =
        info.getRemoteUrl().isBlank() ? info.directory().getAbsolutePath() : info.getRemoteUrl();

    JSONObject raw = new JSONObject();
    raw.put("sha", sha);
    raw.put("message", message);
    raw.put("author", author);
    raw.put("date", createdAt);

    return new GitResourceRecord(
        PROVIDER,
        GitResourceType.COMMITS.getEntityType(),
        info.getOwner(),
        info.getRepositoryName(),
        sha,
        0L,
        firstLine(message),
        "",
        author,
        createdAt,
        "",
        "",
        url,
        message,
        sha,
        "",
        "",
        "",
        raw.toJSONString());
  }

  private static ObjectId resolveStart(Repository repo, String branch) throws Exception {
    if (branch != null && !branch.isBlank()) {
      Ref ref = repo.exactRef("refs/heads/" + branch);
      if (ref == null) {
        ref = repo.exactRef(branch);
      }
      if (ref == null) {
        ref = repo.exactRef("refs/tags/" + branch);
      }
      if (ref != null && ref.getObjectId() != null) {
        return ref.getObjectId();
      }
      ObjectId resolved = repo.resolve(branch);
      if (resolved != null) {
        return resolved;
      }
      throw new HopException("Branch or ref not found in local repository: " + branch);
    }
    Ref head = repo.exactRef("HEAD");
    if (head != null && head.getObjectId() != null) {
      return head.getObjectId();
    }
    throw new HopException("Local repository has no HEAD commit");
  }

  private static Instant parseSince(String since) throws HopException {
    if (since == null || since.isBlank()) {
      return null;
    }
    try {
      return Instant.parse(since.trim());
    } catch (DateTimeParseException e) {
      throw new HopException(
          "Since must be an ISO-8601 timestamp for local repositories, for example"
              + " 2024-01-01T00:00:00Z",
          e);
    }
  }

  private static String firstLine(String message) {
    if (message == null || message.isBlank()) {
      return "";
    }
    int idx = message.indexOf('\n');
    return idx >= 0 ? message.substring(0, idx) : message;
  }
}
