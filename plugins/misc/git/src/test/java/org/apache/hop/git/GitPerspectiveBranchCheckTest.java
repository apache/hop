/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.junit.RepositoryTestCase;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.Test;

/**
 * Headless checks for {@link GitPerspective#isCommitInCurrentBranch(Repository,
 * org.eclipse.jgit.lib.AnyObjectId)} (issue #7520). Uses pure JGit temp repositories — no native
 * git binary and no SWT.
 */
public class GitPerspectiveBranchCheckTest extends RepositoryTestCase {

  @Test
  public void commitOnCurrentBranchIsDetected() throws Exception {
    try (Git git = new Git(db)) {
      writeTrashFile("a.txt", "hello");
      git.add().addFilepattern("a.txt").call();
      RevCommit commit = git.commit().setMessage("initial").call();

      assertTrue(GitPerspective.isCommitInCurrentBranch(db, commit));
    }
  }

  @Test
  public void commitFromOtherRepositoryReturnsFalse() throws Exception {
    // Repo A: commit that will be "stale selection" when checking against B
    RevCommit commitFromA;
    try (Git gitA = new Git(db)) {
      writeTrashFile("a.txt", "from-A");
      gitA.add().addFilepattern("a.txt").call();
      commitFromA = gitA.commit().setMessage("commit in A").call();
    }

    // Repo B: different history; does not contain A's objects
    try (Repository dbB = createWorkRepository();
        Git gitB = new Git(dbB)) {
      File bFile = new File(dbB.getWorkTree(), "b.txt");
      FileUtils.writeStringToFile(bFile, "from-B", StandardCharsets.UTF_8);
      gitB.add().addFilepattern("b.txt").call();
      gitB.commit().setMessage("commit in B").call();

      // Core #7520 regression: foreign object id must not throw MissingObjectException
      assertFalse(GitPerspective.isCommitInCurrentBranch(dbB, commitFromA));
    }
  }

  @Test
  public void nullArgumentsReturnFalse() throws Exception {
    assertFalse(GitPerspective.isCommitInCurrentBranch(null, null));
    try (Git git = new Git(db)) {
      writeTrashFile("a.txt", "x");
      git.add().addFilepattern("a.txt").call();
      RevCommit commit = git.commit().setMessage("c").call();
      assertFalse(GitPerspective.isCommitInCurrentBranch(null, commit));
      assertFalse(GitPerspective.isCommitInCurrentBranch(db, null));
    }
  }
}
