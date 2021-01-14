/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.model;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hop.git.model.revision.ObjectRevision;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.api.RemoteAddCommand;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.junit.RepositoryTestCase;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.URIish;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class UIGitTest extends RepositoryTestCase {
  private Git git;
  private UIGit uiGit;
  Repository db2;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    git = new Git( db );
    uiGit = spy( new UIGit() );
    doNothing().when( uiGit ).showMessageBox( anyString(), anyString() );
    uiGit.setGit( git );
    uiGit.setDirectory( git.getRepository().getDirectory().getParent() );

    // create another repository
    db2 = createWorkRepository();
  }

  @Test
  public void testGetBranch() {
    assertEquals( "master", uiGit.getBranch() );
  }

  @Test
  public void testGetBranches() throws Exception {
    initialCommit();

    assertEquals( Constants.MASTER, uiGit.getLocalBranches().get( 0 ) );
  }

  @Test
  public void testAddRemoveRemote() throws Exception {
    URIish uri = new URIish( db2.getDirectory().toURI().toURL().toString() );
    uiGit.addRemote( uri.toString() );
    assertEquals( uri.toString(), uiGit.getRemote() );

    uiGit.removeRemote();
    // assert that there are no remotes left
    assertTrue( RemoteConfig.getAllRemoteConfigs( db.getConfig() ).isEmpty() );
  }

  private RemoteConfig setupRemote() throws Exception {
    URIish uri = new URIish(
        db2.getDirectory().toURI().toURL() );
    RemoteAddCommand cmd = git.remoteAdd();
    cmd.setName( Constants.DEFAULT_REMOTE_NAME );
    cmd.setUri( uri );
    return cmd.call();
  }

  @Test
  public void testCommit() throws Exception {
    assertFalse( uiGit.hasStagedFiles() );

    writeTrashFile( "Test.txt", "Hello world" );
    uiGit.add( "Test.txt" );
    PersonIdent author = new PersonIdent( "author", "author@example.com" );
    String message = "Initial commit";

    assertTrue( uiGit.hasStagedFiles() );

    uiGit.commit( author.toExternalString(), message );
    String commitId = uiGit.getCommitId( Constants.HEAD );

    assertTrue( uiGit.isClean() );
    assertTrue( author.toExternalString().contains( uiGit.getAuthorName( commitId ) ) );
    assertEquals( message, uiGit.getCommitMessage( commitId ) );
  }

  @Test
  public void shouldNotCommitWhenAuthorNameMalformed() throws Exception {
    writeTrashFile( "Test.txt", "Hello world" );
    uiGit.add( "Test.txt" );

    thrown.expect( NullPointerException.class );
    uiGit.commit( "random author", "Initial commit" );
  }

  @Test
  public void testGetRevisions() throws Exception {
    initialCommit();
    List<ObjectRevision> revisions = uiGit.getRevisions();
    assertEquals( 1, revisions.size() );
  }

  @Test
  public void testGetUnstagedAndStagedObjects() throws Exception {
    // Create files
    File a = writeTrashFile( "a.hpl", "1234567" );
    File b = writeTrashFile( "b.hwf", "content" );
    File c = writeTrashFile( "c.hwf", "abcdefg" );

    // Test for unstaged
    List<UIFile> unStagedObjects = uiGit.getUnstagedFiles();
    assertEquals( 3, unStagedObjects.size() );
    assertTrue( unStagedObjects.stream().anyMatch( obj -> obj.getName().equals( "a.hpl" ) ) );

    // Test for staged
    git.add().addFilepattern( "." ).call();
    List<UIFile> stagedObjects = uiGit.getStagedFiles();
    assertEquals( 3, stagedObjects.size() );
    assertTrue( stagedObjects.stream().anyMatch( obj -> obj.getName().equals( "a.hpl" ) ) );

    // Make a commit
    RevCommit commit = git.commit().setMessage( "initial commit" ).call();
    stagedObjects = uiGit.getStagedFiles( commit.getId().name() + "~", commit.getId().name() );
    assertEquals( 3, stagedObjects.size() );
    assertTrue( stagedObjects.stream().anyMatch( obj -> obj.getName().equals( "b.hwf" ) ) );

    // Change
    a.renameTo( new File( git.getRepository().getWorkTree(), "a2.hpl" ) );
    b.delete();
    FileUtils.writeStringToFile( c, "A change" );

    // Test for unstaged
    unStagedObjects = uiGit.getUnstagedFiles();
    assertEquals( ChangeType.DELETE, unStagedObjects.stream().filter( obj -> obj.getName().equals( "b.hwf" ) ).findFirst().get().getChangeType() );

    // Test for staged
    git.add().addFilepattern( "." ).call();
    git.rm().addFilepattern( a.getName() ).call();
    git.rm().addFilepattern( b.getName() ).call();
    stagedObjects = uiGit.getStagedFiles();
    assertEquals( 4, stagedObjects.size() );
    assertEquals( ChangeType.DELETE, stagedObjects.stream().filter( obj -> obj.getName().equals( "b.hwf" ) ).findFirst().get().getChangeType() );
    assertEquals( ChangeType.ADD, stagedObjects.stream().filter( obj -> obj.getName().equals( "a2.hpl" ) ).findFirst().get().getChangeType() );
    assertEquals( ChangeType.MODIFY, stagedObjects.stream().filter( obj -> obj.getName().equals( "c.hwf" ) ).findFirst().get().getChangeType() );
  }

  @Test
  public void testPull() throws Exception {
    // source: db2, target: db
    setupRemote();
    Git git2 = new Git( db2 );

    // put some file in the source repo and sync
    File sourceFile = new File( db2.getWorkTree(), "SomeFile.txt" );
    FileUtils.writeStringToFile( sourceFile, "Hello world" );
    git2.add().addFilepattern( "SomeFile.txt" ).call();
    git2.commit().setMessage( "Initial commit for source" ).call();
    PullResult pullResult = git.pull().call();

    // change the source file
    FileUtils.writeStringToFile( sourceFile, "Another change" );
    git2.add().addFilepattern( "SomeFile.txt" ).call();
    git2.commit().setMessage( "Some change in remote" ).call();
    git2.close();

    assertTrue( uiGit.pull() );
  }

  @Test
  public void testPullMerge() throws Exception {
    // source: db2, target: db
    setupRemote();
    Git git2 = new Git( db2 );

    // put some file in the source repo and sync
    File sourceFile = new File( db2.getWorkTree(), "SomeFile.txt" );
    FileUtils.writeStringToFile( sourceFile, "Hello world" );
    git2.add().addFilepattern( "SomeFile.txt" ).call();
    git2.commit().setMessage( "Initial commit for source" ).call();
    git.pull().call();

    // change the source file
    FileUtils.writeStringToFile( sourceFile, "Another change" );
    git2.add().addFilepattern( "SomeFile.txt" ).call();
    git2.commit().setMessage( "Some change in remote" ).call();

    File targetFile = new File( db.getWorkTree(), "OtherFile.txt" );
    FileUtils.writeStringToFile( targetFile, "Unconflicting change" );
    git.add().addFilepattern( "OtherFile.txt" ).call();
    git.commit().setMessage( "Unconflicting change in local" ).call();

    assertTrue( uiGit.pull() );

    //  Change at local
    targetFile = new File( db.getWorkTree(), "SomeFile.txt" );
    FileUtils.writeStringToFile( targetFile, "Another change\nChange A" );
    git.add().addFilepattern( "SomeFile.txt" ).call();
    git.commit().setMessage( "Change A at local" ).call();

    //  Change the source file in a way that conflicts with the change at local
    FileUtils.writeStringToFile( sourceFile, "Another change\nChange B" );
    git2.add().addFilepattern( "SomeFile.txt" ).call();
    git2.commit().setMessage( "Change B at remote" ).call();

    uiGit.pull();

    // Cannot commit b/c of unresolved conflicts
    assertFalse( uiGit.hasStagedFiles() );

    // Accept ours
    uiGit.add( "SomeFile.txt.ours" );
    assertTrue( uiGit.hasStagedFiles() );
    git.commit().setMessage( "Merged" ).call();
    git2.close();
  }

  @Test
  public void testPush() throws Exception {
    // Set remote
    Git git2 = new Git( db2 );
    UIGit uiGit2 = new UIGit();
    uiGit2.setGit( git2 );
    URIish uri = new URIish(
      db2.getDirectory().toURI().toURL() );
    RemoteAddCommand cmd = git.remoteAdd();
    cmd.setName( Constants.DEFAULT_REMOTE_NAME );
    cmd.setUri( uri );
    cmd.call();

    assertTrue( uiGit.hasRemote() );

    // create some refs via commits and tag
    RevCommit commit = git.commit().setMessage( "initial commit" ).call();
    Ref tagRef = git.tag().setName( "tag" ).call();

    try {
      db2.resolve( commit.getId().getName() + "^{commit}" );
      fail( "id shouldn't exist yet" );
    } catch ( MissingObjectException e ) {
      // we should get here
    }

    boolean success = uiGit.push();
    assertTrue( success );
    assertEquals( commit.getId(),
        db2.resolve( commit.getId().getName() + "^{commit}" ) );
    assertEquals( tagRef.getObjectId(),
        db2.resolve( tagRef.getObjectId().getName() ) );

    // Push a tag
    EnterSelectionDialog esd = mock( EnterSelectionDialog.class );
    doReturn( "tag" ).when( esd ).open();
    doReturn( esd ).when( uiGit ).getEnterSelectionDialog( any(), anyString(), anyString() );
    uiGit.push( IVCS.TYPE_TAG );
    assertTrue( success );
    assertTrue( uiGit2.getTags().contains( "tag" ) );

    // Another commit and push a branch again
    writeTrashFile( "Test2.txt", "Hello world" );
    git.add().addFilepattern( "Test2.txt" ).call();
    commit = git.commit().setMessage( "second commit" ).call();
    doReturn( Constants.MASTER ).when( esd ).open();
    uiGit.push( IVCS.TYPE_BRANCH );
    assertTrue( success );
    assertEquals( commit.getId(),
        db2.resolve( commit.getId().getName() + "^{commit}" ) );

    assertEquals( "refs/remotes/origin/master", uiGit.getExpandedName( "origin/master", "branch" ) );
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testShouldPushOnlyToOrigin() throws Exception {
    // origin for db2
    URIish uri = new URIish(
      db2.getDirectory().toURI().toURL() );
    RemoteAddCommand cmd = git.remoteAdd();
    cmd.setName( Constants.DEFAULT_REMOTE_NAME );
    cmd.setUri( uri );
    cmd.call();

    // upstream for db3
    Repository db3 = createWorkRepository();
    uri = new URIish(
        db3.getDirectory().toURI().toURL() );
    cmd = git.remoteAdd();
    cmd.setName( "upstream" );
    cmd.setUri( uri );
    cmd.call();

    // create some refs via commits and tag
    RevCommit commit = git.commit().setMessage( "initial commit" ).call();
    Ref tagRef = git.tag().setName( "tag" ).call();

    try {
      db3.resolve( commit.getId().getName() + "^{commit}" );
      fail( "id shouldn't exist yet" );
    } catch ( MissingObjectException e ) {
      // we should get here
    }

    uiGit.push();

    // The followings should throw MissingObjectException
    thrown.expect( MissingObjectException.class );
    db3.resolve( commit.getId().getName() + "^{commit}" );
    db3.resolve( tagRef.getObjectId().getName() );
  }

  @Test
  public void testDiff() throws Exception {
    File file = writeTrashFile( "Test.txt", "Hello world" );

    String diff = uiGit.diff( IVCS.INDEX, uiGit.getShortenedName( IVCS.WORKINGTREE, IVCS.TYPE_COMMIT ), "Test.txt" );
    assertTrue( diff.contains( "+Hello world" ) );

    git.add().addFilepattern( "Test.txt" ).call();
    RevCommit commit1 = git.commit().setMessage( "initial commit" ).call();

    // git show the first commit
    diff = uiGit.diff( null, commit1.getName(), "Test.txt" );
    assertTrue( diff.contains( "+Hello world" ) );

    // abbreviated commit id should work
    String diff2 = uiGit.diff( null, uiGit.getShortenedName( commit1.getName(), IVCS.TYPE_COMMIT ), "Test.txt" );
    assertEquals( diff, diff2 );

    // Add another line
    FileUtils.writeStringToFile( file, "second commit" );
    git.add().addFilepattern( "Test.txt" ).call();
    RevCommit commit2 = git.commit().setMessage( "second commit" ).call();

    diff = uiGit.diff( commit1.getName(), IVCS.WORKINGTREE );
    assertTrue( diff.contains( "-Hello world" ) );
    assertTrue( diff.contains( "+second commit" ) );
    diff = uiGit.diff( commit1.getName(), commit2.getName() );
    assertTrue( diff.contains( "+second commit" ) );
  }

  @Test
  public void testOpen() throws Exception {
    RevCommit commit = initialCommit();

    InputStream inputStream = uiGit.open( "Test.txt", commit.getName() );
    StringWriter writer = new StringWriter();
    IOUtils.copy( inputStream, writer, "UTF-8" );
    assertEquals( "Hello world", writer.toString() );

    inputStream = uiGit.open( "Test.txt", IVCS.WORKINGTREE );
    writer = new StringWriter();
    IOUtils.copy( inputStream, writer, "UTF-8" );
    assertEquals( "Hello world", writer.toString() );
  }

  @Test
  public void testCheckout() throws Exception {
    initialCommit();

    git.branchCreate().setName( "develop" ).call();
    uiGit.checkout( uiGit.getExpandedName( "master", IVCS.TYPE_BRANCH ) );
    assertEquals( "master", uiGit.getBranch() );
    uiGit.checkout( uiGit.getExpandedName( "develop", IVCS.TYPE_BRANCH ) );
    assertEquals( "develop", uiGit.getBranch() );
  }

  @Test
  public void testRevertPath() throws Exception {
    // commit something
    File file = writeTrashFile( "Test.txt", "Hello world" );
    git.add().addFilepattern( "Test.txt" ).call();
    RevCommit commit = git.commit().setMessage( "initial commit" ).call();

    // Add some change
    FileUtils.writeStringToFile( file, "Change" );
    assertEquals( "Change", FileUtils.readFileToString( file ) );

    uiGit.revertPath( file.getName() );
    assertEquals( "Hello world", FileUtils.readFileToString( file ) );
  }

  @Test
  public void testCreateDeleteBranchTag() throws Exception {
    initialCommit();

    // create a tag
    uiGit.createTag( "test" );
    List<String> tags = uiGit.getTags();
    assertTrue( tags.contains( "test" ) );

    // create a branch (and checkout that branch)
    uiGit.createBranch( "test" );
    List<String> branches = uiGit.getLocalBranches();
    assertTrue( branches.contains( "test" ) );
    assertEquals( "test", uiGit.getBranch() );

    // Checkout master
    uiGit.checkout( Constants.MASTER );

    // delete the branch
    uiGit.deleteBranch( "test", true );
    branches = uiGit.getLocalBranches();
    assertEquals( 1, branches.size() );
    assertFalse( branches.contains( "test" ) );

    uiGit.checkout( uiGit.getExpandedName( "test", IVCS.TYPE_TAG ) );
    assertTrue( uiGit.getBranch().contains( Constants.HEAD ) );

    // delete the tag
    uiGit.deleteTag( "test" );
    tags = uiGit.getTags();
    assertEquals( 0, tags.size() );
    assertFalse( tags.contains( "test" ) );
  }


  @Test
  public void testCloneShouldFail() throws Exception {
    // WhenDirAlreadyExists
    boolean success = uiGit.cloneRepo( db.getDirectory().getPath(), db.getDirectory().getPath() );
    assertFalse( success );

    // WhenURLNotFound
    File file = createTempFile();
    success = uiGit.cloneRepo( file.getPath(), "fakeURL" );
    assertFalse( success );
    assertFalse( file.exists() );
  }

  private RevCommit initialCommit() throws Exception {
    writeTrashFile( "Test.txt", "Hello world" );
    git.add().addFilepattern( "Test.txt" ).call();
    return git.commit().setMessage( "initial commit" ).call();
  }
}
