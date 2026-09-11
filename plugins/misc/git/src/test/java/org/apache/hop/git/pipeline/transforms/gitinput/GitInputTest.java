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

package org.apache.hop.git.pipeline.transforms.gitinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.git.provider.GitInputFields;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * End-to-end behaviour of the {@link GitInput} transform against a real local clone, which keeps
 * the test free of HTTP while still exercising the full open-read-dispose lifecycle.
 */
class GitInputTest {

  @TempDir File repoDir;

  private TransformMockHelper<GitInputMeta, GitInputData> helper;
  private GitInputMeta meta;
  private GitInputData data;

  @BeforeAll
  static void initEnv() throws Exception {
    if (!HopClientEnvironment.isInitialized()) {
      HopClientEnvironment.init();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    helper = new TransformMockHelper<>("GitInput", GitInputMeta.class, GitInputData.class);
    Mockito.when(helper.logChannelFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(helper.iLogChannel);
    Mockito.when(helper.pipeline.isRunning()).thenReturn(true);

    meta = new GitInputMeta();
    meta.setSource(GitInputSource.LOCAL.name());
    meta.setLocalRepositoryPath(repoDir.getAbsolutePath());
    meta.setResourceType("COMMITS");
    data = new GitInputData();

    initRepoWithCommits(3);
  }

  @AfterEach
  void tearDown() {
    if (helper != null) {
      helper.cleanUp();
    }
  }

  private GitInput newTransform() {
    GitInput transform =
        new GitInput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.initializeFrom(new Variables());
    return transform;
  }

  private void initRepoWithCommits(int count) throws Exception {
    try (Git git = Git.init().setDirectory(repoDir).call()) {
      for (int i = 0; i < count; i++) {
        Files.writeString(new File(repoDir, "file" + i + ".txt").toPath(), "v" + i);
        git.add().addFilepattern(".").call();
        git.commit().setMessage("commit " + i).setAuthor("tester", "tester@example.com").call();
      }
    }
  }

  @Test
  void readsEveryCommitFromALocalCloneAndSignalsDone() throws Exception {
    GitInput transform = newTransform();
    List<Object[]> rows = new ArrayList<>();
    transform.addRowListener(rowListener(rows));

    assertTrue(transform.init());
    while (transform.processRow()) {
      // drain
    }

    assertEquals(3, rows.size());
    assertEquals(3, data.reader.count());
  }

  @Test
  void outputRowsCarryTypedTimestamps() throws Exception {
    GitInput transform = newTransform();
    List<Object[]> rows = new ArrayList<>();
    transform.addRowListener(rowListener(rows));

    assertTrue(transform.init());
    while (transform.processRow()) {
      // drain
    }

    IRowMeta rowMeta = data.outputRowMeta;
    int createdAt = rowMeta.indexOfValue("created_at");
    assertInstanceOf(Date.class, rows.get(0)[createdAt]);
    assertEquals("local", rows.get(0)[rowMeta.indexOfValue("repo_owner")]);
  }

  @Test
  void disposeReleasesTheReaderWhenTheTransformStopsEarly() throws Exception {
    GitInput transform = newTransform();
    transform.addRowListener(rowListener(new ArrayList<>()));

    assertTrue(transform.init());
    // A single row, then abandon the transform the way a failing downstream step would.
    assertTrue(transform.processRow());
    assertNotNull(data.reader, "the reader should be open after the first row");

    transform.dispose();

    // Leaving the JGit RevWalk open keeps pack files mapped and blocks deleting the clone.
    assertNull(data.reader, "dispose() must release the reader");
  }

  @Test
  void disposeIsSafeWhenTheTransformNeverOpenedAReader() {
    GitInput transform = newTransform();

    assertNull(data.reader);
    transform.dispose();

    assertNull(data.reader);
  }

  @Test
  void initFailsWhenALocalPathIsMissing() {
    meta.setLocalRepositoryPath("");
    assertFalse(newTransform().init());
  }

  @Test
  void initFailsWhenARemoteSourceHasNoConnection() {
    meta.setSource(GitInputSource.REMOTE.name());
    meta.setConnectionName("");
    assertFalse(newTransform().init());
  }

  @Test
  void commitFilesAreRejectedForARemoteSource() throws Exception {
    meta.setSource(GitInputSource.REMOTE.name());
    meta.setResourceType("COMMIT_FILES");
    meta.setConnectionName("github");
    meta.setOwner("apache");
    meta.setRepository("hop");

    GitInput transform = newTransform();
    // init() is skipped: the guard belongs in processRow, where the resource type is resolved.
    HopException e = assertThrows(HopException.class, transform::processRow);
    assertTrue(e.getMessage().contains("COMMIT_FILES"));
  }

  @Test
  void issuesAreRejectedForALocalSource() {
    meta.setResourceType("ISSUES");

    GitInput transform = newTransform();
    assertTrue(transform.init());

    HopException e = assertThrows(HopException.class, transform::processRow);
    assertTrue(e.getMessage().contains("COMMITS and COMMIT_FILES"));
  }

  @Test
  void theBranchSettingSelectsWhichHistoryIsRead() throws Exception {
    try (Git git = Git.init().setDirectory(repoDir).call()) {
      git.checkout().setCreateBranch(true).setName("side").call();
      Files.writeString(new File(repoDir, "side.txt").toPath(), "side");
      git.add().addFilepattern(".").call();
      git.commit().setMessage("side commit").setAuthor("t", "t@e.com").call();
    }
    meta.setBranch("side");

    GitInput transform = newTransform();
    List<Object[]> rows = new ArrayList<>();
    transform.addRowListener(rowListener(rows));

    assertTrue(transform.init());
    while (transform.processRow()) {
      // drain
    }

    int title = data.outputRowMeta.indexOfValue("title");
    assertEquals(4, rows.size());
    assertEquals("side commit", rows.get(0)[title]);
  }

  @Test
  void rawJsonIsOmittedWhenTurnedOff() throws Exception {
    meta.setIncludeRawJson(false);

    GitInput transform = newTransform();
    List<Object[]> rows = new ArrayList<>();
    transform.addRowListener(rowListener(rows));

    assertTrue(transform.init());
    while (transform.processRow()) {
      // drain
    }

    assertEquals(GitInputFields.FIELD_NAMES.length - 1, data.outputRowMeta.size());
    assertEquals(-1, data.outputRowMeta.indexOfValue("raw_json"));
  }

  private static org.apache.hop.pipeline.transform.RowAdapter rowListener(List<Object[]> rows) {
    return new org.apache.hop.pipeline.transform.RowAdapter() {
      @Override
      public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
        rows.add(row);
      }
    };
  }
}
