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

package org.apache.hop.git;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.git.model.UIFile;
import org.apache.hop.git.model.UIGit;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** An editor click must not cost a git status of the whole repository. */
class GitGuiPluginFileStatusTest {

  @TempDir Path repositoryFolder;

  private Path pipeline;
  private String filename;
  private UIGit uiGit;
  private GitGuiPlugin plugin;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    pipeline = repositoryFolder.resolve("pipeline.hpl");
    Files.writeString(pipeline, "<pipeline/>", StandardCharsets.UTF_8);
    try (Git git = Git.init().setDirectory(repositoryFolder.toFile()).call()) {
      git.add().addFilepattern("pipeline.hpl").call();
      commit(git, "initial commit");
    }
    filename = pipeline.toString();

    uiGit = new UIGit();
    uiGit.openRepo(repositoryFolder.toString());

    // The branch label lives in the status toolbar of Hop GUI, which a unit test doesn't have
    plugin = spy(new GitGuiPlugin());
    doNothing().when(plugin).setBranchLabel(any());
    plugin.setGit(uiGit);
    plugin.refreshChangedFiles();
  }

  @AfterEach
  void tearDown() {
    uiGit.closeRepo();
  }

  private static void commit(Git git, String message) throws Exception {
    git.commit()
        .setMessage(message)
        .setAuthor("Hop", "hop@example.com")
        .setCommitter("Hop", "hop@example.com")
        .setSign(false)
        .call();
  }

  /** Only git itself changed something, in the index or HEAD: the file is as old as it was. */
  private void makeFileOld() throws Exception {
    Files.setLastModifiedTime(pipeline, FileTime.fromMillis(System.currentTimeMillis() - 60_000L));
  }

  @Test
  void aFileUnchangedSinceTheStatusIsNotReadAgain() throws Exception {
    makeFileOld();

    assertFalse(plugin.refreshFileStatus(filename));
    assertNull(plugin.getChangedFiles().get(filename));
  }

  @Test
  void aFileSavedAfterTheStatusIsReadAgain() throws Exception {
    Files.writeString(pipeline, "<pipeline><info/></pipeline>", StandardCharsets.UTF_8);

    assertTrue(plugin.refreshFileStatus(filename));
    UIFile file = plugin.getChangedFiles().get(filename);
    assertNotNull(file);
    assertEquals(ChangeType.MODIFY, file.getChangeType());
    assertFalse(file.isStaged());
  }

  @Test
  void aGitAddOutsideHopGuiIsPickedUp() throws Exception {
    Files.writeString(pipeline, "<pipeline><info/></pipeline>", StandardCharsets.UTF_8);
    plugin.refreshChangedFiles();
    assertFalse(plugin.getChangedFiles().get(filename).isStaged());

    makeFileOld();
    try (Git git = Git.open(repositoryFolder.toFile())) {
      git.add().addFilepattern("pipeline.hpl").call();
    }

    assertTrue(plugin.refreshFileStatus(filename));
    assertTrue(plugin.getChangedFiles().get(filename).isStaged());
  }

  @Test
  void aGitCommitOutsideHopGuiIsPickedUp() throws Exception {
    Files.writeString(pipeline, "<pipeline><info/></pipeline>", StandardCharsets.UTF_8);
    try (Git git = Git.open(repositoryFolder.toFile())) {
      git.add().addFilepattern("pipeline.hpl").call();
    }
    plugin.refreshChangedFiles();
    assertNotNull(plugin.getChangedFiles().get(filename));

    makeFileOld();
    try (Git git = Git.open(repositoryFolder.toFile())) {
      commit(git, "second commit");
    }

    assertTrue(plugin.refreshFileStatus(filename));
    assertNull(plugin.getChangedFiles().get(filename));
  }

  @Test
  void aFileOutsideTheRepositoryIsLeftAlone(@TempDir Path otherFolder) throws Exception {
    Path other = otherFolder.resolve("other.hpl");
    Files.writeString(other, "<pipeline/>", StandardCharsets.UTF_8);

    assertFalse(plugin.refreshFileStatus(other.toString()));
  }
}
