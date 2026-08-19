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
 *
 */

package org.apache.hop.git.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RemoteAddCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.junit.RepositoryTestCase;
import org.eclipse.jgit.lib.ConfigConstants;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryState;
import org.eclipse.jgit.merge.MergeStrategy;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.URIish;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class UIGitTest extends RepositoryTestCase {
  private Git git;
  private UIGit uiGit;
  Repository db2;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    git = new Git(db);
    uiGit = spy(new UIGit());
    doNothing().when(uiGit).showMessageBox(anyString(), anyString());
    uiGit.setGit(git);
    uiGit.setDirectory(git.getRepository().getDirectory().getParent());

    // create another repository
    db2 = createWorkRepository();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    this.db.close();
    if (!System.getProperty("os.name").contains("Windows")) {
      super.tearDown();
    } else {
      int lastBackslashIndex = uiGit.getDirectory().lastIndexOf('\\');
      String updatedPath = uiGit.getDirectory().substring(0, lastBackslashIndex);

      File f = new File(updatedPath);
      FileUtils.forceDeleteOnExit(f);
    }
    db2.close();
  }

  @Test
  public void testGetBranch() {
    assertEquals("master", uiGit.getBranch());
  }

  @Test
  public void testGetBranches() throws Exception {
    initialCommit();

    assertEquals(Constants.MASTER, uiGit.getLocalBranches().get(0));
  }

  @Test
  public void testAddRemoveRemote() throws Exception {
    URIish uri = new URIish(db2.getDirectory().toURI().toURL().toString());
    uiGit.addRemote(uri.toString());
    assertEquals(uri.toString(), uiGit.getRemote());

    uiGit.removeRemote();
    // assert that there are no remotes left
    assertTrue(RemoteConfig.getAllRemoteConfigs(db.getConfig()).isEmpty());
  }

  private RemoteConfig setupRemote() throws Exception {
    URIish uri = new URIish(db2.getDirectory().toURI().toURL());
    RemoteAddCommand cmd = git.remoteAdd();
    cmd.setName(Constants.DEFAULT_REMOTE_NAME);
    cmd.setUri(uri);
    return cmd.call();
  }

  @Test
  public void testCommit() throws Exception {
    assertFalse(uiGit.hasStagedFiles());

    writeTrashFile("Test.txt", "Hello world");
    uiGit.add("Test.txt");
    PersonIdent author = new PersonIdent("author", "author@example.com");
    String message = "Initial commit";

    assertTrue(uiGit.hasStagedFiles());

    uiGit.commit(author.toExternalString(), message);
    String commitId = uiGit.getCommitId(Constants.HEAD);

    assertTrue(uiGit.isClean());
    assertTrue(author.toExternalString().contains(uiGit.getAuthorName(commitId)));
    assertEquals(message, uiGit.getCommitMessage(commitId));

    // Test commit with amend flag
    String amendedMessage = "Initial commit (amended)";

    uiGit.commit(author.toExternalString(), amendedMessage, true);
    String amendedCommitId = uiGit.getCommitId(Constants.HEAD);

    assertEquals(amendedMessage, uiGit.getCommitMessage(amendedCommitId));
    List<ObjectRevision> revisions = uiGit.getRevisions();
    assertEquals(revisions.size(), 1L);
  }

  @Test
  public void shouldNotCommitWhenAuthorNameMalformed() throws Exception {
    writeTrashFile("Test.txt", "Hello world");
    uiGit.add("Test.txt");

    assertThrows(NullPointerException.class, () -> uiGit.commit("random author", "Initial commit"));
  }

  @Test
  public void testGetRevisions() throws Exception {
    initialCommit();
    List<ObjectRevision> revisions = uiGit.getRevisions();
    assertEquals(1, revisions.size());
  }

  @Test
  public void testGetUnstagedAndStagedObjects() throws Exception {
    // Create files
    File a = writeTrashFile("a.hpl", "1234567");
    File b = writeTrashFile("b.hwf", "content");
    File c = writeTrashFile("c.hwf", "abcdefg");

    // Test for unstaged
    List<UIFile> unStagedObjects = uiGit.getUnstagedFiles();
    assertEquals(3, unStagedObjects.size());
    assertTrue(unStagedObjects.stream().anyMatch(obj -> obj.getName().equals("a.hpl")));

    // Test for staged
    git.add().addFilepattern(".").call();
    List<UIFile> stagedObjects = uiGit.getStagedFiles();
    assertEquals(3, stagedObjects.size());
    assertTrue(stagedObjects.stream().anyMatch(obj -> obj.getName().equals("a.hpl")));

    // Make a commit
    RevCommit commit = git.commit().setMessage("initial commit").call();
    stagedObjects = uiGit.getStagedFiles(commit.getId().name() + "~", commit.getId().name());
    assertEquals(3, stagedObjects.size());
    assertTrue(stagedObjects.stream().anyMatch(obj -> obj.getName().equals("b.hwf")));

    // Change
    a.renameTo(new File(git.getRepository().getWorkTree(), "a2.hpl"));
    b.delete();
    FileUtils.writeStringToFile(c, "A change", StandardCharsets.UTF_8);

    // Test for unstaged
    unStagedObjects = uiGit.getUnstagedFiles();
    assertEquals(
        ChangeType.DELETE,
        unStagedObjects.stream()
            .filter(obj -> obj.getName().equals("b.hwf"))
            .findFirst()
            .get()
            .getChangeType());

    // Test for staged
    git.add().addFilepattern(".").call();
    git.rm().addFilepattern(a.getName()).call();
    git.rm().addFilepattern(b.getName()).call();
    stagedObjects = uiGit.getStagedFiles();
    assertEquals(4, stagedObjects.size());
    assertEquals(
        ChangeType.DELETE,
        stagedObjects.stream()
            .filter(obj -> obj.getName().equals("b.hwf"))
            .findFirst()
            .get()
            .getChangeType());
    assertEquals(
        ChangeType.ADD,
        stagedObjects.stream()
            .filter(obj -> obj.getName().equals("a2.hpl"))
            .findFirst()
            .get()
            .getChangeType());
    assertEquals(
        ChangeType.MODIFY,
        stagedObjects.stream()
            .filter(obj -> obj.getName().equals("c.hwf"))
            .findFirst()
            .get()
            .getChangeType());
  }

  @Test
  public void testPull() throws Exception {
    // source: db2, target: db
    setupRemote();
    Git git2 = new Git(db2);

    // put some file in the source repo and sync
    File sourceFile = new File(db2.getWorkTree(), "SomeFile.txt");
    FileUtils.writeStringToFile(sourceFile, "Hello world", StandardCharsets.UTF_8);
    git2.add().addFilepattern("SomeFile.txt").call();
    git2.commit().setMessage("Initial commit for source").call();
    git.pull().call();

    // change the source file
    FileUtils.writeStringToFile(sourceFile, "Another change", StandardCharsets.UTF_8);
    git2.add().addFilepattern("SomeFile.txt").call();
    git2.commit().setMessage("Some change in remote").call();
    git2.close();

    assertTrue(uiGit.pull());
  }

  @Test
  public void testPullMerge() throws Exception {
    // source: db2, target: db
    setupRemote();
    Git git2 = new Git(db2);

    // put some file in the source repo and sync
    File sourceFile = new File(db2.getWorkTree(), "SomeFile.txt");
    FileUtils.writeStringToFile(sourceFile, "Hello world", StandardCharsets.UTF_8);
    git2.add().addFilepattern("SomeFile.txt").call();
    git2.commit().setMessage("Initial commit for source").call();
    git.pull().call();

    // change the source file
    FileUtils.writeStringToFile(sourceFile, "Another change", StandardCharsets.UTF_8);
    git2.add().addFilepattern("SomeFile.txt").call();
    git2.commit().setMessage("Some change in remote").call();

    File targetFile = new File(db.getWorkTree(), "OtherFile.txt");
    FileUtils.writeStringToFile(targetFile, "Unconflicting change", StandardCharsets.UTF_8);
    git.add().addFilepattern("OtherFile.txt").call();
    git.commit().setMessage("Unconflicting change in local").call();

    assertTrue(uiGit.pull());

    //  Change at local
    targetFile = new File(db.getWorkTree(), "SomeFile.txt");
    FileUtils.writeStringToFile(targetFile, "Another change\nChange A", StandardCharsets.UTF_8);
    git.add().addFilepattern("SomeFile.txt").call();
    git.commit().setMessage("Change A at local").call();

    //  Change the source file in a way that conflicts with the change at local
    FileUtils.writeStringToFile(sourceFile, "Another change\nChange B", StandardCharsets.UTF_8);
    git2.add().addFilepattern("SomeFile.txt").call();
    git2.commit().setMessage("Change B at remote").call();

    uiGit.pull();

    // Cannot commit b/c of unresolved conflicts
    assertFalse(uiGit.hasStagedFiles());

    // Accept ours
    uiGit.add("SomeFile.txt.ours");
    assertTrue(uiGit.hasStagedFiles());
    git.commit().setMessage("Merged").call();
    git2.close();
  }

  @Test
  public void testPush() throws Exception {
    // Set remote
    Git git2 = new Git(db2);
    UIGit uiGit2 = new UIGit();
    uiGit2.setGit(git2);
    URIish uri = new URIish(db2.getDirectory().toURI().toURL());
    RemoteAddCommand cmd = git.remoteAdd();
    cmd.setName(Constants.DEFAULT_REMOTE_NAME);
    cmd.setUri(uri);
    cmd.call();

    assertTrue(uiGit.hasRemote());

    // create some refs via commits and tag
    RevCommit commit = git.commit().setMessage("initial commit").call();
    Ref tagRef = git.tag().setName("tag").call();

    try {
      db2.resolve(commit.getId().getName() + "^{commit}");
      fail("id shouldn't exist yet");
    } catch (MissingObjectException e) {
      // we should get here
    }

    boolean success = uiGit.push();
    assertTrue(success);
    assertEquals(commit.getId(), db2.resolve(commit.getId().getName() + "^{commit}"));
    assertEquals(tagRef.getObjectId(), db2.resolve(tagRef.getObjectId().getName()));

    // Push a tag
    EnterSelectionDialog esd = mock(EnterSelectionDialog.class);
    doReturn("tag").when(esd).open();
    doReturn(esd).when(uiGit).getEnterSelectionDialog(any(), anyString(), anyString());
    uiGit.push(VCS.TYPE_TAG);
    assertTrue(success);
    assertTrue(uiGit2.getTags().contains("tag"));

    // Another commit and push a branch again
    writeTrashFile("Test2.txt", "Hello world");
    git.add().addFilepattern("Test2.txt").call();
    commit = git.commit().setMessage("second commit").call();
    doReturn(Constants.MASTER).when(esd).open();
    uiGit.push(VCS.TYPE_BRANCH);
    assertTrue(success);
    assertEquals(commit.getId(), db2.resolve(commit.getId().getName() + "^{commit}"));

    assertEquals("refs/remotes/origin/master", uiGit.getExpandedName("origin/master", "branch"));
  }

  @Test
  public void testPushAndDeleteTagByName() throws Exception {
    // Set remote
    Git git2 = new Git(db2);
    UIGit uiGit2 = new UIGit();
    uiGit2.setGit(git2);
    setupRemote();

    git.commit().setMessage("initial commit").call();
    git.tag().setName("v1").call();

    // Push the tag itself, a default push doesn't include tags
    assertTrue(uiGit.push());
    assertFalse(uiGit2.getTags().contains("v1"));

    assertTrue(uiGit.push(VCS.TYPE_TAG, "v1"));
    assertTrue(uiGit2.getTags().contains("v1"));

    // Delete the tag on the remote
    assertTrue(uiGit.deleteRemoteTag("v1"));
    assertFalse(uiGit2.getTags().contains("v1"));

    // Deleting a tag that is already gone on the remote is not an error
    assertTrue(uiGit.deleteRemoteTag("v1"));

    // The name is known, so no selection dialog is opened
    verify(uiGit, never()).getEnterSelectionDialog(any(), anyString(), anyString());
    git2.close();
  }

  @Test
  public void testDeleteRemoteBranch() throws Exception {
    Git git2 = new Git(db2);
    UIGit uiGit2 = new UIGit();
    uiGit2.setGit(git2);
    setupRemote();

    git.commit().setMessage("initial commit").call();
    git.branchCreate().setName("feature/test").call();

    // A branch name with a slash in it: the remote is origin, the branch is feature/test
    assertTrue(uiGit.push(VCS.TYPE_BRANCH, "feature/test"));
    git.fetch().call();
    assertTrue(uiGit2.getLocalBranches().contains("feature/test"));
    assertNotNull(db.findRef("refs/remotes/origin/feature/test"));

    assertTrue(uiGit.deleteRemoteBranch("refs/remotes/origin/feature/test"));
    assertFalse(uiGit2.getLocalBranches().contains("feature/test"));

    // The tracking ref is removed as well, deleting on the remote doesn't prune it
    assertNull(db.findRef("refs/remotes/origin/feature/test"));

    git2.close();
  }

  @Test
  public void testDeleteRemoteBranchWithoutRemote() throws Exception {
    git.commit().setMessage("initial commit").call();

    assertThrows(HopException.class, () -> uiGit.deleteRemoteBranch("refs/remotes/origin/feature"));
  }

  @Test
  public void testRenameRemoteBranch() throws Exception {
    Git git2 = new Git(db2);
    UIGit uiGit2 = new UIGit();
    uiGit2.setGit(git2);
    setupRemote();

    RevCommit commit = git.commit().setMessage("initial commit").call();
    git.branchCreate().setName("old").call();
    assertTrue(uiGit.push(VCS.TYPE_BRANCH, "old"));
    git.fetch().call();

    assertTrue(uiGit.renameRemoteBranch("refs/remotes/origin/old", "new"));

    // The branch is created under its new name and removed under the old one, pointing at the
    // same commit
    assertTrue(uiGit2.getLocalBranches().contains("new"));
    assertFalse(uiGit2.getLocalBranches().contains("old"));
    assertEquals(commit.getId(), db2.resolve("refs/heads/new"));

    // The tracking refs follow along, without needing a fetch
    assertNull(db.findRef("refs/remotes/origin/old"));
    assertNotNull(db.findRef("refs/remotes/origin/new"));

    git2.close();
  }

  @Test
  public void testIsRemoteHead() throws Exception {
    setupRemote();

    git.commit().setMessage("initial commit").call();
    git.branchCreate().setName("feature").call();
    assertTrue(uiGit.push(VCS.TYPE_BRANCH, "feature"));
    git.fetch().call();

    // Without a remote HEAD there is nothing to protect
    assertFalse(uiGit.isRemoteHead("refs/remotes/origin/feature"));

    db.updateRef("refs/remotes/origin/HEAD").link("refs/remotes/origin/feature");
    assertTrue(uiGit.isRemoteHead("refs/remotes/origin/feature"));

    // Local branches and tags are never a remote HEAD
    assertFalse(uiGit.isRemoteHead("refs/heads/master"));
  }

  @Test
  public void testShouldPushOnlyToOrigin() throws Exception {
    // origin for db2
    URIish uri = new URIish(db2.getDirectory().toURI().toURL());
    RemoteAddCommand cmd = git.remoteAdd();
    cmd.setName(Constants.DEFAULT_REMOTE_NAME);
    cmd.setUri(uri);
    cmd.call();

    // upstream for db3
    Repository db3 = createWorkRepository();
    uri = new URIish(db3.getDirectory().toURI().toURL());
    cmd = git.remoteAdd();
    cmd.setName("upstream");
    cmd.setUri(uri);
    cmd.call();

    // create some refs via commits and tag
    RevCommit commit = git.commit().setMessage("initial commit").call();
    Ref tagRef = git.tag().setName("tag").call();

    try {
      db3.resolve(commit.getId().getName() + "^{commit}");
      fail("id shouldn't exist yet");
    } catch (MissingObjectException e) {
      // we should get here
    }

    uiGit.push();

    assertThrows(
        MissingObjectException.class, () -> db3.resolve(commit.getId().getName() + "^{commit}"));

    db3.resolve(tagRef.getObjectId().getName());
  }

  @Test
  public void testDiff() throws Exception {
    File file = writeTrashFile("Test.txt", "Hello world");

    String diff = uiGit.diff(VCS.INDEX, uiGit.getShortenedName(VCS.WORKINGTREE), "Test.txt");
    assertTrue(diff.contains("+Hello world"));

    git.add().addFilepattern("Test.txt").call();
    RevCommit commit1 = git.commit().setMessage("initial commit").call();

    // git show the first commit
    diff = uiGit.diff(null, commit1.getName(), "Test.txt");
    assertTrue(diff.contains("+Hello world"));

    // abbreviated commit id should work
    String diff2 = uiGit.diff(null, uiGit.getShortenedName(commit1.getName()), "Test.txt");
    assertEquals(diff, diff2);

    // Add another line
    FileUtils.writeStringToFile(file, "second commit", StandardCharsets.UTF_8);
    git.add().addFilepattern("Test.txt").call();
    RevCommit commit2 = git.commit().setMessage("second commit").call();

    diff = uiGit.diff(commit1.getName(), VCS.WORKINGTREE);
    assertTrue(diff.contains("-Hello world"));
    assertTrue(diff.contains("+second commit"));
    diff = uiGit.diff(commit1.getName(), commit2.getName());
    assertTrue(diff.contains("+second commit"));
  }

  @Test
  public void testOpen() throws Exception {
    RevCommit commit = initialCommit();

    InputStream inputStream = uiGit.open("Test.txt", commit.getName());
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, StandardCharsets.UTF_8);
    assertEquals("Hello world", writer.toString());
    inputStream.close();
    writer.close();

    inputStream = uiGit.open("Test.txt", VCS.WORKINGTREE);
    writer = new StringWriter();
    IOUtils.copy(inputStream, writer, StandardCharsets.UTF_8);
    assertEquals("Hello world", writer.toString());
    inputStream.close();
    writer.close();
  }

  @Test
  public void testCheckout() throws Exception {
    initialCommit();

    git.branchCreate().setName("develop").call();
    uiGit.checkout(uiGit.getExpandedName("master", VCS.TYPE_BRANCH));
    assertEquals("master", uiGit.getBranch());
    uiGit.checkout(uiGit.getExpandedName("develop", VCS.TYPE_BRANCH));
    assertEquals("develop", uiGit.getBranch());
  }

  @Test
  public void testRevertPath() throws Exception {
    // commit something
    File file = writeTrashFile("Test.txt", "Hello world");
    git.add().addFilepattern("Test.txt").call();
    git.commit().setMessage("initial commit").call();

    // Add some change
    FileUtils.writeStringToFile(file, "Change", StandardCharsets.UTF_8);
    assertEquals("Change", FileUtils.readFileToString(file, StandardCharsets.UTF_8));

    uiGit.revertPath(file.getName());
    assertEquals("Hello world", FileUtils.readFileToString(file, StandardCharsets.UTF_8));
  }

  @Test
  public void testRevertPathOnlyUnstagesAddedFile() throws Exception {
    initialCommit();

    // A new file which was staged with "add"
    File file = writeTrashFile("New.txt", "Hello world");
    git.add().addFilepattern("New.txt").call();
    assertTrue(git.status().call().getAdded().contains("New.txt"));

    uiGit.revertPath("New.txt");

    // The file is unstaged but is still on disk with its content intact
    assertTrue(file.exists());
    assertEquals("Hello world", FileUtils.readFileToString(file, StandardCharsets.UTF_8));
    Status status = git.status().call();
    assertTrue(status.getAdded().isEmpty());
    assertTrue(status.getUntracked().contains("New.txt"));
  }

  @Test
  public void testRevertPathKeepsUntrackedFile() throws Exception {
    initialCommit();

    File file = writeTrashFile("Untracked.txt", "Hello world");

    uiGit.revertPath("Untracked.txt");

    assertTrue(file.exists());
    assertEquals("Hello world", FileUtils.readFileToString(file, StandardCharsets.UTF_8));
    assertTrue(git.status().call().getUntracked().contains("Untracked.txt"));
  }

  @Test
  public void testRevertPathRestoresDeletedFile() throws Exception {
    File file = writeTrashFile("Test.txt", "Hello world");
    git.add().addFilepattern("Test.txt").call();
    git.commit().setMessage("initial commit").call();

    assertTrue(file.delete());

    uiGit.revertPath("Test.txt");

    assertTrue(file.exists());
    assertEquals("Hello world", FileUtils.readFileToString(file, StandardCharsets.UTF_8));
  }

  @Test
  public void testGetNewRevertPathFiles() throws Exception {
    File tracked = writeTrashFile("folder/Tracked.txt", "Hello world");
    git.add().addFilepattern("folder/Tracked.txt").call();
    git.commit().setMessage("initial commit").call();

    FileUtils.writeStringToFile(tracked, "Change", StandardCharsets.UTF_8);
    writeTrashFile("folder/Untracked.txt", "Untracked");
    writeTrashFile("folder/Added.txt", "Added");
    git.add().addFilepattern("folder/Added.txt").call();

    Set<String> newFiles = uiGit.getNewRevertPathFiles("folder");

    assertEquals(2, newFiles.size());
    assertTrue(newFiles.contains("folder/Untracked.txt"));
    assertTrue(newFiles.contains("folder/Added.txt"));
  }

  @Test
  public void testModifiedFilesAreUnstagedUntilTheyAreAdded() throws Exception {
    File file = writeTrashFile("Test.txt", "Hello world");
    git.add().addFilepattern("Test.txt").call();
    git.commit().setMessage("initial commit").call();

    // A change in the working tree is not staged, only "git add" stages it
    FileUtils.writeStringToFile(file, "Change", StandardCharsets.UTF_8);

    List<UIFile> unstaged = uiGit.getUnstagedFiles();
    assertEquals(1, unstaged.size());
    assertEquals("Test.txt", unstaged.get(0).getName());
    assertEquals(ChangeType.MODIFY, unstaged.get(0).getChangeType());
    assertFalse(unstaged.get(0).isStaged());
    assertTrue(uiGit.getStagedFiles().isEmpty());

    uiGit.add("Test.txt");

    List<UIFile> staged = uiGit.getStagedFiles();
    assertEquals(1, staged.size());
    assertEquals("Test.txt", staged.get(0).getName());
    assertEquals(ChangeType.MODIFY, staged.get(0).getChangeType());
    assertTrue(staged.get(0).isStaged());
    assertTrue(uiGit.getUnstagedFiles().isEmpty());
  }

  @Test
  public void testGetUntrackedPathFiles() throws Exception {
    File tracked = writeTrashFile("folder/Tracked.txt", "Hello world");
    writeTrashFile("folder/Ignored.txt", "Ignored");
    writeTrashFile(".gitignore", "Ignored.txt");
    git.add().addFilepattern("folder/Tracked.txt").addFilepattern(".gitignore").call();
    git.commit().setMessage("initial commit").call();

    FileUtils.writeStringToFile(tracked, "Change", StandardCharsets.UTF_8);
    writeTrashFile("folder/Untracked.txt", "Untracked");
    writeTrashFile("folder/Added.txt", "Added");
    git.add().addFilepattern("folder/Added.txt").call();
    writeTrashFile("outside/Other.txt", "Outside the folder");

    // Only the untracked file: not the tracked, added, ignored or out of scope ones
    assertEquals(List.of("folder/Untracked.txt"), uiGit.getUntrackedPathFiles("folder"));

    // A single file works as well, as does the whole repository
    assertEquals(
        List.of("folder/Untracked.txt"), uiGit.getUntrackedPathFiles("folder/Untracked.txt"));
    assertTrue(uiGit.getUntrackedPathFiles("folder/Tracked.txt").isEmpty());
    assertEquals(
        List.of("folder/Untracked.txt", "outside/Other.txt"), uiGit.getUntrackedPathFiles(null));
  }

  @Test
  public void testCleanPathsOnlyRemovesUntrackedFiles() throws Exception {
    File tracked = writeTrashFile("folder/Tracked.txt", "Hello world");
    git.add().addFilepattern("folder/Tracked.txt").call();
    git.commit().setMessage("initial commit").call();
    FileUtils.writeStringToFile(tracked, "Change", StandardCharsets.UTF_8);

    File untracked = writeTrashFile("folder/Untracked.txt", "Untracked");
    File nested = writeTrashFile("folder/new-folder/Nested.txt", "Nested");

    uiGit.cleanPaths(
        List.of("folder/Untracked.txt", "folder/new-folder/Nested.txt", "folder/Tracked.txt"));

    // Untracked files are removed, along with the folder they left behind empty
    assertFalse(untracked.exists());
    assertFalse(nested.exists());
    assertFalse(nested.getParentFile().exists());

    // A tracked file is never removed by a clean, even when it's passed in
    assertTrue(tracked.exists());
    assertEquals("Change", FileUtils.readFileToString(tracked, StandardCharsets.UTF_8));
  }

  @Test
  public void testCleanPathsWithoutPathsDoesNotRemoveAnything() throws Exception {
    initialCommit();
    File untracked = writeTrashFile("Untracked.txt", "Untracked");

    uiGit.cleanPaths(List.of());
    uiGit.cleanPaths(null);

    assertTrue(untracked.exists());
  }

  @Test
  public void testCreateDeleteBranchTag() throws Exception {
    initialCommit();

    // create a tag
    uiGit.createTag("test");
    List<String> tags = uiGit.getTags();
    assertTrue(tags.contains("test"));

    // create a branch (and checkout that branch)
    uiGit.createBranch("test");
    List<String> branches = uiGit.getLocalBranches();
    assertTrue(branches.contains("test"));
    assertEquals("test", uiGit.getBranch());

    // Checkout master
    uiGit.checkout(Constants.MASTER);

    // delete the branch
    uiGit.deleteBranch("test", true);
    branches = uiGit.getLocalBranches();
    assertEquals(1, branches.size());
    assertFalse(branches.contains("test"));

    uiGit.checkout(uiGit.getExpandedName("test", VCS.TYPE_TAG));
    assertTrue(uiGit.getBranch().contains(Constants.HEAD));

    // delete the tag
    uiGit.deleteTag("test");
    tags = uiGit.getTags();
    assertEquals(0, tags.size());
    assertFalse(tags.contains("test"));
  }

  @Test
  public void testUnstageFilesWithResetPath() throws Exception {
    initialCommit();

    // Stage a change to a tracked file and a brand new file
    writeTrashFile("Test.txt", "Changed");
    uiGit.add("Test.txt");
    writeTrashFile("New.txt", "New file");
    uiGit.add("New.txt");
    assertEquals(2, uiGit.getStagedFiles().size());

    for (UIFile file : uiGit.getStagedFiles()) {
      uiGit.resetPath(file.getName());
    }

    // Nothing staged anymore, but both changes are still there
    assertTrue(uiGit.getStagedFiles().isEmpty());
    assertEquals(2, uiGit.getUnstagedFiles().size());
    assertEquals("Changed", read(new File(db.getWorkTree(), "Test.txt")));
  }

  @Test
  public void testUntrackedAndModifiedFileFlags() throws Exception {
    initialCommit();

    // A file git doesn't know about yet and a change to a file it does know about
    writeTrashFile("New.txt", "New file");
    writeTrashFile("Test.txt", "Changed");

    List<UIFile> unstagedFiles = uiGit.getUnstagedFiles();
    UIFile untracked = findFile(unstagedFiles, "New.txt");
    UIFile modified = findFile(unstagedFiles, "Test.txt");

    // The commit perspective tells both apart by change type to decide what it offers for the
    // next commit: an unstaged ADD is a file git doesn't track yet, which is never checked for you
    assertEquals(ChangeType.ADD, untracked.getChangeType());
    assertFalse(untracked.isStaged());
    assertEquals(ChangeType.MODIFY, modified.getChangeType());
    assertFalse(modified.isStaged());

    // Once added, the same new file is staged
    uiGit.add("New.txt");
    UIFile staged = findFile(uiGit.getStagedFiles(), "New.txt");
    assertEquals(ChangeType.ADD, staged.getChangeType());
    assertTrue(staged.isStaged());
  }

  private UIFile findFile(List<UIFile> files, String name) {
    return files.stream()
        .filter(file -> file.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("File '" + name + "' not found"));
  }

  @Test
  public void testIgnoreRulesMatchedWithoutRegardToCase() throws Exception {
    initialCommit();

    // A rule in lower case, folders on disk in another case: git catches these when it is told the
    // file system doesn't care about case, JGit doesn't
    writeTrashFile(".gitignore", "output/\n*.LOG\n");
    writeTrashFile("Output/generated.txt", "generated");
    writeTrashFile("Deep/OUTPUT/generated.txt", "generated");
    writeTrashFile("run.Log", "log");
    writeTrashFile("keep.txt", "keep");

    // Case sensitive, the way git behaves on Linux: JGit is right, nothing is filtered
    List<String> unstaged = getUnstagedFileNames();
    assertTrue(unstaged.contains("Output/generated.txt"));
    assertTrue(unstaged.contains("run.Log"));

    db.getConfig()
        .setBoolean(
            ConfigConstants.CONFIG_CORE_SECTION,
            null,
            CaseInsensitiveIgnores.CONFIG_KEY_IGNORECASE,
            true);
    db.getConfig().save();

    unstaged = getUnstagedFileNames();
    assertTrue(unstaged.contains("keep.txt"));
    assertFalse(unstaged.contains("Output/generated.txt"));
    assertFalse(unstaged.contains("Deep/OUTPUT/generated.txt"));
    assertFalse(unstaged.contains("run.Log"));

    // What is kept out of the unstaged files is reported as ignored instead
    Set<String> ignored = uiGit.getIgnored(null);
    assertTrue(ignored.contains("Output/generated.txt"));
    assertTrue(ignored.contains("run.Log"));
    assertFalse(ignored.contains("keep.txt"));
  }

  @Test
  public void testIgnoreRuleCanBeNegatedWithoutRegardToCase() throws Exception {
    initialCommit();

    writeTrashFile(".gitignore", "output/\n!Output/keep.txt\n");
    writeTrashFile("output/keep.txt", "keep");
    writeTrashFile("output/generated.txt", "generated");

    db.getConfig()
        .setBoolean(
            ConfigConstants.CONFIG_CORE_SECTION,
            null,
            CaseInsensitiveIgnores.CONFIG_KEY_IGNORECASE,
            true);
    db.getConfig().save();

    // git never descends into an ignored folder, so the negated file stays ignored as well
    List<String> unstaged = getUnstagedFileNames();
    assertFalse(unstaged.contains("output/keep.txt"));
    assertFalse(unstaged.contains("output/generated.txt"));
  }

  private List<String> getUnstagedFileNames() {
    return uiGit.getUnstagedFiles().stream().map(UIFile::getName).toList();
  }

  @Test
  public void testCreateBranchFromTag() throws Exception {
    RevCommit tagged = initialCommit();

    uiGit.createTag("lightweight");
    Ref annotatedTag = git.tag().setName("annotated").setMessage("Annotated tag").call();

    // Move the current branch forward, a branch created from a tag has to start at the tagged
    // commit and not at HEAD
    writeTrashFile("Test2.txt", "Hello world");
    uiGit.add("Test2.txt");
    RevCommit head = git.commit().setMessage("second commit").call();
    assertNotEquals(tagged.getId(), head.getId());

    assertTrue(
        uiGit.createBranch("from-lightweight", uiGit.getExpandedName("lightweight", VCS.TYPE_TAG)));
    assertEquals("from-lightweight", uiGit.getBranch());
    assertEquals(tagged.getId(), db.resolve(Constants.HEAD));

    // An annotated tag points at a tag object, it has to be peeled to the commit
    uiGit.checkout(Constants.MASTER);
    assertTrue(uiGit.createBranch("from-annotated", annotatedTag.getName()));
    assertEquals("from-annotated", uiGit.getBranch());
    assertEquals(tagged.getId(), db.resolve(Constants.HEAD));
  }

  @Test
  public void testCloneShouldFail() throws Exception {
    // WhenDirAlreadyExists
    boolean success = uiGit.cloneRepo(db.getDirectory().getPath(), db.getDirectory().getPath());
    assertFalse(success);

    // WhenURLNotFound
    File file = createTempFile();
    success = uiGit.cloneRepo(file.getPath(), "fakeURL");
    assertFalse(success);
    assertFalse(file.exists());
  }

  @Test
  public void testMergeBranchWithUncommittedChangesExplainsWhatToDo() throws Exception {
    initialCommit();
    commitOnBranch("develop", "Test.txt", "Hello from develop");

    // Let master move on as well, so merging develop is a real merge and not a fast-forward
    //
    git.checkout().setName(Constants.MASTER).call();
    writeTrashFile("Other.txt", "Hello master");
    git.add().addFilepattern("Other.txt").call();
    git.commit().setMessage("master commit").call();

    // Leave an uncommitted change in the file the merge needs to touch
    //
    writeTrashFile("Test.txt", "Uncommitted work");

    assertFalse(uiGit.mergeBranch("develop", MergeStrategy.RECURSIVE));
    assertMergeFailureExplained();
  }

  @Test
  public void testFastForwardMergeWithUncommittedChangesExplainsWhatToDo() throws Exception {
    initialCommit();
    commitOnBranch("develop", "Test.txt", "Hello from develop");

    // Master is untouched, so merging develop fast-forwards. The checkout of Test.txt can't happen
    // while it holds uncommitted changes.
    //
    git.checkout().setName(Constants.MASTER).call();
    writeTrashFile("Test.txt", "Uncommitted work");

    assertFalse(uiGit.mergeBranch("develop", MergeStrategy.RECURSIVE));
    assertMergeFailureExplained();
  }

  @Test
  public void testMergeBranchWithStagedButUncommittedWorkIsNotReportedAsSuccess() throws Exception {
    initialCommit();

    // Stage a file on the branch but never commit it, so the branch holds no commits of its own
    //
    git.branchCreate().setName("test-branch").call();
    git.checkout().setName("test-branch").call();
    writeTrashFile("bogus-pipeline.hpl", "not committed");
    git.add().addFilepattern("bogus-pipeline.hpl").call();

    git.checkout().setName(Constants.MASTER).call();

    // There is nothing to merge, so this is not a successful merge
    //
    assertFalse(uiGit.mergeBranch("test-branch", MergeStrategy.RECURSIVE));

    ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
    verify(uiGit).showMessageBox(anyString(), message.capture());

    assertTrue(message.getValue(), message.getValue().contains("is already up to date with"));
    assertTrue(message.getValue(), message.getValue().contains("You have uncommitted changes"));
    assertTrue(message.getValue(), message.getValue().contains("bogus-pipeline.hpl"));
  }

  @Test
  public void testMergeBranchUpToDateWithCleanWorkingTreeDoesNotMentionUncommittedChanges()
      throws Exception {
    initialCommit();
    git.branchCreate().setName("test-branch").call();

    assertFalse(uiGit.mergeBranch("test-branch", MergeStrategy.RECURSIVE));

    ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
    verify(uiGit).showMessageBox(anyString(), message.capture());

    assertTrue(message.getValue(), message.getValue().contains("is already up to date with"));
    assertFalse(message.getValue().contains("You have uncommitted changes"));
  }

  /** The user needs to know which file is in the way and how to get past it. */
  private void assertMergeFailureExplained() {
    ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
    verify(uiGit).showMessageBox(anyString(), message.capture());

    assertTrue(message.getValue(), message.getValue().contains("Test.txt"));
    assertTrue(
        message.getValue(),
        message.getValue().contains("Please commit or revert your changes before you merge"));
  }

  /**
   * A merge has to be recorded as a merge. Committing the resolved conflict used to reset the whole
   * index first, which cleared MERGE_HEAD and left an ordinary commit behind: git no longer
   * considered the branch merged and replayed everything on the next merge.
   */
  @Test
  public void testCommitPathsAfterAMergeRecordsBothParents() throws Exception {
    RevCommit base = initialCommit();
    commitOnBranch("develop", "Test.txt", "Hello from develop");
    RevCommit develop = git.getRepository().parseCommit(git.getRepository().resolve("develop"));

    // Let master change the same file, so merging develop conflicts
    //
    git.checkout().setName(Constants.MASTER).call();
    writeTrashFile("Test.txt", "Hello from master");
    git.add().addFilepattern("Test.txt").call();
    RevCommit master = git.commit().setMessage("master commit").call();

    assertTrue(uiGit.mergeBranch("develop", MergeStrategy.RECURSIVE));
    assertEquals(RepositoryState.MERGING, uiGit.getRepositoryState());

    // Resolve the conflict the way the commit perspective does: accept a side, then commit
    //
    uiGit.add("Test.txt.ours");
    assertEquals(RepositoryState.MERGING_RESOLVED, uiGit.getRepositoryState());

    assertTrue(
        uiGit.commitPaths(List.of("Test.txt"), "John Doe <john@example.com>", "Merged", false));

    RevCommit merged = git.getRepository().parseCommit(git.getRepository().resolve(Constants.HEAD));
    assertEquals("The merge has to be recorded as a merge commit", 2, merged.getParentCount());
    assertEquals(master, merged.getParent(0));
    assertEquals(develop, merged.getParent(1));
    assertEquals(RepositoryState.SAFE, uiGit.getRepositoryState());
    assertNotEquals(base, merged);
  }

  /**
   * The commit records the paths which were asked for and nothing else, whether or not the caller
   * knew about everything that was staged.
   */
  @Test
  public void testCommitPathsCommitsOnlyTheGivenPaths() throws Exception {
    initialCommit();

    writeTrashFile("Committed.txt", "in the commit");
    writeTrashFile("Unchecked.txt", "left out of the commit");
    git.add().addFilepattern("Committed.txt").call();
    git.add().addFilepattern("Unchecked.txt").call();

    // Staged after the caller read its file list, so it is not in the selection either
    //
    writeTrashFile("StagedInTheMeantime.txt", "staged behind the GUI's back");
    git.add().addFilepattern("StagedInTheMeantime.txt").call();

    assertTrue(
        uiGit.commitPaths(
            List.of("Committed.txt"), "John Doe <john@example.com>", "One file only", false));

    String head = uiGit.getCommitId(Constants.HEAD);
    List<UIFile> committed = uiGit.getStagedFiles(uiGit.getParentCommitId(head), head);
    assertEquals(1, committed.size());
    assertEquals("Committed.txt", committed.get(0).getName());

    // Both of the others are out of the index and still on disk, nothing was thrown away
    //
    Status status = git.status().call();
    assertTrue(status.getUntracked().contains("Unchecked.txt"));
    assertTrue(status.getUntracked().contains("StagedInTheMeantime.txt"));
    assertTrue(new File(db.getWorkTree(), "Unchecked.txt").exists());
    assertTrue(new File(db.getWorkTree(), "StagedInTheMeantime.txt").exists());
  }

  /**
   * Committing part of a merge is not possible, so the whole index goes in and the resolution the
   * user staged is never quietly dropped.
   */
  @Test
  public void testCommitPathsDuringAMergeKeepsTheRestOfTheIndexStaged() throws Exception {
    initialCommit();
    commitOnBranch("develop", "Test.txt", "Hello from develop");

    git.checkout().setName(Constants.MASTER).call();
    writeTrashFile("Test.txt", "Hello from master");
    git.add().addFilepattern("Test.txt").call();
    git.commit().setMessage("master commit").call();

    assertTrue(uiGit.mergeBranch("develop", MergeStrategy.RECURSIVE));
    uiGit.add("Test.txt.ours");

    // Another file staged during the merge has to survive into the merge commit
    //
    writeTrashFile("AlsoResolved.txt", "resolved as well");
    git.add().addFilepattern("AlsoResolved.txt").call();

    assertTrue(
        uiGit.commitPaths(List.of("Test.txt"), "John Doe <john@example.com>", "Merged", false));

    String head = uiGit.getCommitId(Constants.HEAD);
    List<UIFile> committed = uiGit.getStagedFiles(uiGit.getParentCommitId(head), head);
    assertTrue(committed.stream().anyMatch(file -> file.getName().equals("AlsoResolved.txt")));
    assertTrue(uiGit.isClean());
  }

  /**
   * A commit that fails must leave the files it was asked to commit staged, so nothing has to be
   * staged a second time to retry.
   */
  @Test
  public void testCommitPathsLeavesTheSelectionStagedWhenTheCommitFails() throws Exception {
    initialCommit();

    writeTrashFile("Staged.txt", "staged content");

    // A malformed author name (no e-mail address) makes the commit itself fail
    //
    assertThrows(
        Exception.class,
        () -> uiGit.commitPaths(List.of("Staged.txt"), "no email address", "Nope", false));

    assertTrue(git.status().call().getAdded().contains("Staged.txt"));
  }

  private void commitOnBranch(String branch, String file, String content) throws Exception {
    git.branchCreate().setName(branch).call();
    git.checkout().setName(branch).call();
    writeTrashFile(file, content);
    git.add().addFilepattern(file).call();
    git.commit().setMessage(branch + " commit").call();
  }

  private RevCommit initialCommit() throws Exception {
    writeTrashFile("Test.txt", "Hello world");
    git.add().addFilepattern("Test.txt").call();
    return git.commit().setMessage("initial commit").call();
  }
}
