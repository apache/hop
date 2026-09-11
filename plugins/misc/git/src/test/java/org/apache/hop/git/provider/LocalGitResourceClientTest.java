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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalGitResourceClientTest {

  @TempDir File repoDir;

  @Test
  void commitFilesIncludesAddedAndModifiedPaths() throws Exception {
    initRepoWithTwoCommits();

    GitResourceReader reader =
        LocalGitResourceClient.openReader(
            repoDir.getAbsolutePath(),
            new GitListOptions("all", null, null, 100, 10),
            GitResourceType.COMMIT_FILES);

    Set<String> changeTypes = new HashSet<>();
    Set<String> paths = new HashSet<>();
    while (reader.hasNext()) {
      GitResourceRecord record = reader.next();
      changeTypes.add(record.getState());
      paths.add(record.getTitle());
    }

    assertTrue(changeTypes.contains("added"));
    assertTrue(changeTypes.contains("modified"));
    assertTrue(paths.contains("hello.txt"));
    assertTrue(paths.contains("docs/readme.md"));
  }

  @Test
  void commitFilesPaginatesWithinLargeCommit() throws Exception {
    for (int i = 0; i < 75; i++) {
      writeFile("bulk/file-" + i + ".txt", "v1");
    }
    try (Git git = Git.init().setDirectory(repoDir).call()) {
      git.add().addFilepattern(".").call();
      git.commit().setMessage("bulk add").setAuthor("tester", "tester@example.com").call();
    }

    GitResourceReader reader =
        LocalGitResourceClient.openReader(
            repoDir.getAbsolutePath(),
            new GitListOptions("all", null, null, 30, 1),
            GitResourceType.COMMIT_FILES);

    int count = 0;
    while (reader.hasNext()) {
      reader.next();
      count++;
    }

    assertEquals(75, count);
  }

  @Test
  void commitFilesIgnoresRemoteRowCap() throws Exception {
    for (int i = 0; i < 1005; i++) {
      writeFile("many/file-" + i + ".txt", "v1");
    }
    try (Git git = Git.init().setDirectory(repoDir).call()) {
      git.add().addFilepattern(".").call();
      git.commit().setMessage("many files").setAuthor("tester", "tester@example.com").call();
    }

    GitResourceReader reader =
        LocalGitResourceClient.openReader(
            repoDir.getAbsolutePath(),
            new GitListOptions("all", null, null, 50, 20),
            GitResourceType.COMMIT_FILES);

    int count = 0;
    while (reader.hasNext()) {
      reader.next();
      count++;
    }

    assertEquals(1005, count);
  }

  @Test
  void mapChangeTypeCoversJGitChangeTypes() {
    assertEquals(
        "added",
        LocalGitResourceClient.mapChangeType(org.eclipse.jgit.diff.DiffEntry.ChangeType.ADD));
    assertEquals(
        "deleted",
        LocalGitResourceClient.mapChangeType(org.eclipse.jgit.diff.DiffEntry.ChangeType.DELETE));
  }

  @Test
  void detectsRenamesInsteadOfReportingADeleteAndAnAdd() throws Exception {
    // Content has to be substantial enough for JGit's similarity scoring to pair the two paths.
    String content = "alpha\nbravo\ncharlie\ndelta\necho\nfoxtrot\ngolf\nhotel\n";
    writeFile("original.txt", content);
    try (Git git = Git.init().setDirectory(repoDir).call()) {
      git.add().addFilepattern(".").call();
      git.commit().setMessage("add").setAuthor("tester", "tester@example.com").call();

      Files.move(
          new File(repoDir, "original.txt").toPath(), new File(repoDir, "renamed.txt").toPath());
      git.rm().addFilepattern("original.txt").call();
      git.add().addFilepattern("renamed.txt").call();
      git.commit().setMessage("rename").setAuthor("tester", "tester@example.com").call();
    }

    List<GitResourceRecord> records = readAll(GitResourceType.COMMIT_FILES);

    GitResourceRecord rename =
        records.stream()
            .filter(r -> "renamed".equals(r.getState()))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "no renamed row; got "
                            + records.stream().map(GitResourceRecord::getState).toList()));
    assertEquals("renamed.txt", rename.getTitle());
    assertEquals("original.txt", rename.getBody(), "the previous path belongs in body");
  }

  @Test
  void addedFilesReportNoPreviousPathRatherThanJGitsSentinel() throws Exception {
    initRepoWithTwoCommits();

    List<GitResourceRecord> records = readAll(GitResourceType.COMMIT_FILES);

    for (GitResourceRecord record : records) {
      assertFalse(
          record.getBody().contains("/dev/null"),
          "JGit's /dev/null sentinel leaked into the output row: " + record.getBody());
      if ("added".equals(record.getState())) {
        assertEquals("", record.getBody());
      }
    }
  }

  @Test
  void commitRowsCarryParsedTimestamps() throws Exception {
    initRepoWithTwoCommits();

    List<GitResourceRecord> records = readAll(GitResourceType.COMMITS);

    int createdAt = List.of(GitInputFields.FIELD_NAMES).indexOf("created_at");
    Object value = records.get(0).toRow(true)[createdAt];
    assertInstanceOf(Date.class, value);
  }

  @Test
  void closingAReaderEarlyReleasesTheRepository() throws Exception {
    initRepoWithTwoCommits();

    GitResourceReader reader =
        LocalGitResourceClient.openReader(
            repoDir.getAbsolutePath(),
            new GitListOptions("all", null, null, 1, 10),
            GitResourceType.COMMITS);

    // Stop after a single row, the way a preview or a failing downstream transform would.
    assertTrue(reader.hasNext());
    reader.next();
    reader.close();

    // Closing twice must be safe: dispose() runs even when iteration already closed the walk.
    reader.close();

    // With the JGit handles released the repository directory can be replaced on any platform.
    assertTrue(new File(repoDir, ".git").exists());
  }

  @Test
  void branchesAreListedFromTheLocalClone() throws Exception {
    initRepoWithTwoCommits();
    try (Git git = Git.init().setDirectory(repoDir).call()) {
      git.branchCreate().setName("feature").call();
    }

    List<String> branches = LocalGitResourceClient.listBranches(repoDir.getAbsolutePath());

    assertTrue(branches.contains("feature"), "got " + branches);
  }

  @Test
  void sinceMustBeAnIso8601Timestamp() throws Exception {
    initRepoWithTwoCommits();

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                LocalGitResourceClient.openReader(
                    repoDir.getAbsolutePath(),
                    new GitListOptions("all", "last tuesday", null, 10, 1),
                    GitResourceType.COMMITS));

    assertTrue(e.getMessage().contains("ISO-8601"));
  }

  private List<GitResourceRecord> readAll(GitResourceType type) throws Exception {
    List<GitResourceRecord> records = new ArrayList<>();
    try (GitResourceReader reader =
        LocalGitResourceClient.openReader(
            repoDir.getAbsolutePath(), new GitListOptions("all", null, null, 100, 10), type)) {
      while (reader.hasNext()) {
        records.add(reader.next());
      }
    }
    return records;
  }

  private void initRepoWithTwoCommits() throws Exception {
    writeFile("hello.txt", "v1");
    writeFile("docs/readme.md", "docs");
    try (Git git = Git.init().setDirectory(repoDir).call()) {
      git.add().addFilepattern(".").call();
      git.commit().setMessage("initial").setAuthor("tester", "tester@example.com").call();

      writeFile("hello.txt", "v2");
      git.add().addFilepattern("hello.txt").call();
      git.commit().setMessage("update hello").setAuthor("tester", "tester@example.com").call();
    }
  }

  private void writeFile(String relativePath, String content) throws IOException {
    File file = new File(repoDir, relativePath);
    File parent = file.getParentFile();
    if (parent != null) {
      Files.createDirectories(parent.toPath());
    }
    Files.writeString(file.toPath(), content);
  }
}
