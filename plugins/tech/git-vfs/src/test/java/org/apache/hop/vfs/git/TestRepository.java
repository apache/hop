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
package org.apache.hop.vfs.git;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.revwalk.RevCommit;

/**
 * A small git repository to read, built in a temporary folder.
 *
 * <p>Built with JGit rather than by shelling out to {@code git}, so the tests do not need git
 * installed to be worth anything, and rather than a repository checked into the test resources,
 * because what is in it has to be readable and editable by the tests themselves.
 *
 * <p>Files are committed onto branches and tags a test can name, which is what makes it possible to
 * ask for one revision of the same repository and get a different answer than for another.
 */
public class TestRepository implements AutoCloseable {

  private final Path folder;
  private final Git git;

  private static final String BRANCH = "main";
  private static final String RELEASE = "release";

  private TestRepository(Path folder, Git git) {
    this.folder = folder;
    this.git = git;
  }

  /**
   * A repository with a {@code main} branch and a {@code v1} tag, holding:
   *
   * <pre>
   *   workflows/daily.hwf     "first version"
   *   workflows/nested/step.hpl
   *   pipelines/readme.txt
   * </pre>
   *
   * @param folder where to build it
   */
  public static TestRepository create(Path folder) throws Exception {
    Files.createDirectories(folder);
    try (Git git = Git.init().setDirectory(folder.toFile()).setInitialBranch(BRANCH).call()) {
      write(folder.resolve("workflows/daily.hwf"), "first version");
      write(folder.resolve("workflows/nested/step.hpl"), "a step");
      write(folder.resolve("pipelines/readme.txt"), "nothing to see");
      write(folder.resolve(".gitignore"), "*.log\n");
      git.add().addFilepattern(".").call();
      RevCommit first = commit(git, "the first version");
      git.tag().setName("v1").setAnnotated(false).setObjectId(first).call();

      // A second commit on main, so a test can tell the two revisions apart.
      write(folder.resolve("workflows/daily.hwf"), "second version");
      git.add().addFilepattern(".").call();
      commit(git, "the second version");

      // A branch the default clone does not contain, so a test can tell a fetch from a checkout of
      // what the clone already had.
      git.checkout().setCreateBranch(true).setName(RELEASE).call();
      write(folder.resolve("workflows/daily.hwf"), "release version");
      git.add().addFilepattern(".").call();
      commit(git, "the release");
      git.checkout().setName(BRANCH).call();

      // A commit id is only fetchable when the server allows it. The file transport does; a git
      // daemon does not, unless the repository says so.
      org.eclipse.jgit.lib.StoredConfig config = git.getRepository().getConfig();
      config.setBoolean("uploadpack", null, "allowReachableSHA1InWant", true);
      config.setBoolean("uploadpack", null, "allowAnySHA1InWant", true);
      config.save();
    }
    return new TestRepository(folder, Git.open(folder.toFile()));
  }

  /** The branch the test revisions are named on. */
  public static String branch() {
    return BRANCH;
  }

  /** A branch which is not the default, so reading it has to fetch it. */
  public static String releaseBranch() {
    return RELEASE;
  }

  /** The {@code v1} tag, which points at the first commit. */
  public String firstCommitId() throws Exception {
    return git.getRepository().resolve("v1").getName();
  }

  /** The folder holding the {@code .git} directory: the "remote" to clone from. */
  public String url() {
    return folder.toUri().toString();
  }

  public Path getFolder() {
    return folder;
  }

  @Override
  public void close() {
    if (git != null) {
      git.close();
    }
  }

  private static RevCommit commit(Git git, String message) throws Exception {
    // A fixed identity and a fixed timestamp: the commit id of a test repository has to be the same
    // on every machine, or a test which names a commit by hand is testing the clock.
    PersonIdent author =
        new PersonIdent(
            "Hop Test",
            "hop@example.com",
            java.time.Instant.ofEpochSecond(1_000_000_000L),
            java.time.ZoneOffset.UTC);
    RevCommit commit =
        git.commit().setMessage(message).setAuthor(author).setCommitter(author).call();
    // Point the branch at the new commit, so the next clone sees it as the tip.
    RefUpdate update =
        git.getRepository().updateRef("refs/heads/" + git.getRepository().getBranch());
    update.setNewObjectId(commit.getId());
    update.forceUpdate();
    return commit;
  }

  private static void write(Path file, String content) throws Exception {
    Files.createDirectories(file.getParent());
    Files.writeString(file, content, StandardCharsets.UTF_8);
  }
}
