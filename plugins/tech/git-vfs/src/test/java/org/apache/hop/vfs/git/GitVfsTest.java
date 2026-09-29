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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.git.metadata.GitConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What the driver is for: reading the files of a repository through the VFS as if they were an
 * ordinary folder, so that a pipeline or a workflow can be run straight out of a URI.
 *
 * <p>Against a repository built in a temporary folder, which JGit clones over its own file
 * transport. That is the same code path a real remote takes - clone, resolve the revision, check it
 * out - with none of the network in it. {@code GitVfsContainerIT} covers the same ground against a
 * git daemon in a container, which is where the transport gets exercised over a real socket.
 */
class GitVfsTest {

  @TempDir private Path tempFolder;

  private TestRepository repository;
  private DefaultFileSystemManager manager;

  @BeforeEach
  void setUp() throws Exception {
    repository = TestRepository.create(tempFolder.resolve("remote"));
    register("ops", connection("ops", TestRepository.branch(), ""));
  }

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
      manager = null;
    }
    repository.close();
  }

  @Nested
  @DisplayName("A file behind a connection reads as an ordinary file")
  class Reading {

    @Test
    @DisplayName("The content of a file at the default branch is the content of the repository")
    void theContentOfAFileIsTheContentOfTheRepository() throws Exception {
      FileObject file = resolve("ops:///workflows/daily.hwf");

      assertTrue(file.exists(), "the file should exist in the checkout");
      assertEquals("second version", read(file));
    }

    @Test
    @DisplayName("A file in a subfolder is found")
    void aFileInASubfolderIsFound() throws Exception {
      FileObject file = resolve("ops:///workflows/nested/step.hpl");

      assertTrue(file.exists());
      assertEquals("a step", read(file));
    }

    @Test
    @DisplayName("A file which is not in the repository does not exist")
    void aFileWhichIsNotInTheRepositoryDoesNotExist() throws Exception {
      FileObject file = resolve("ops:///workflows/nowhere.hwf");

      assertFalse(file.exists());
    }

    @Test
    @DisplayName("A folder lists the files in it")
    void aFolderListsTheFilesInIt() throws Exception {
      FileObject folder = resolve("ops:///workflows/");

      assertTrue(folder.isFolder());
      List<String> names = namesOf(folder);
      assertTrue(names.contains("daily.hwf"), names.toString());
      assertTrue(names.contains("nested"), names.toString());
    }

    @Test
    @DisplayName("A file reports the size of the file in the working copy")
    void aFileReportsItsSize() throws Exception {
      FileObject file = resolve("ops:///pipelines/readme.txt");

      assertEquals("nothing to see".length(), file.getContent().getSize());
    }

    @Test
    @DisplayName("The .git folder of the working copy is hidden")
    void theGitFolderOfTheWorkingCopyIsHidden() throws Exception {
      FileObject gitFolder = resolve("ops:///.git");

      assertTrue(gitFolder.exists(), ".git is in the working copy");
      assertTrue(gitFolder.isHidden(), ".git should be hidden from a browse dialog");
    }

    @Test
    @DisplayName("The root listing does not offer the git metadata")
    void theRootListingDoesNotOfferTheGitMetadata() throws Exception {
      List<String> names = namesOf(resolve("ops:///"));

      assertFalse(names.contains(".git"), names.toString());
      assertFalse(names.contains("hop-git-vfs-ready"), names.toString());
      assertTrue(names.contains("workflows"), names.toString());
      assertTrue(names.contains(".gitignore"), names.toString());
    }

    @Test
    @DisplayName("A gitignore file is a file of the project, not the git folder")
    void aGitignoreFileIsNotHidden() throws Exception {
      FileObject ignore = resolve("ops:///.gitignore");

      assertTrue(ignore.exists());
      assertFalse(ignore.isHidden());
      assertEquals("*.log\n", read(ignore));
    }
  }

  @Nested
  @DisplayName("The revision decides what is read")
  class Revisions {

    @Test
    @DisplayName("A tag reads the commit it points at, not the tip of the branch")
    void aTagReadsItsOwnCommit() throws Exception {
      useConnection("ops", "v1", "");

      assertEquals("first version", read(resolve("ops:///workflows/daily.hwf")));
    }

    @Test
    @DisplayName("A commit id reads that commit")
    void aCommitIdReadsThatCommit() throws Exception {
      useConnection("ops", repository.firstCommitId(), "");

      assertEquals("first version", read(resolve("ops:///workflows/daily.hwf")));
    }

    @Test
    @DisplayName("A branch other than the default one is fetched and checked out")
    void aBranchOtherThanTheDefaultIsFetched() throws Exception {
      useConnection("ops", TestRepository.releaseBranch(), "");

      assertEquals("release version", read(resolve("ops:///workflows/daily.hwf")));
    }

    @Test
    @DisplayName("A branch which is not there is an error, not an empty repository")
    void aBranchWhichIsNotThereIsAnError() throws Exception {
      useConnection("ops", "no-such-branch", "");

      FileSystemException e =
          assertThrows(FileSystemException.class, () -> resolve("ops:///workflows/daily.hwf"));
      assertTrue(
          e.getMessage().contains("no-such-branch"),
          "the error should name the branch which was not found: " + e.getMessage());
    }
  }

  @Nested
  @DisplayName("A base path moves the root of the connection")
  class BasePaths {

    @Test
    @DisplayName("A base path serves what is under it as the root of the connection")
    void aBasePathServesWhatIsUnderIt() throws Exception {
      useConnection("ops", TestRepository.branch(), "workflows");

      assertEquals("second version", read(resolve("ops:///daily.hwf")));
    }

    @Test
    @DisplayName("With a base path, a file outside it is not reachable through the connection")
    void aFileOutsideTheBasePathIsNotReachable() throws Exception {
      useConnection("ops", TestRepository.branch(), "workflows");

      assertFalse(resolve("ops:///pipelines/readme.txt").exists());
    }

    @Test
    @DisplayName("A base path written with a backslash works as well as one with a slash")
    void aBasePathWithABackslashWorksToo() throws Exception {
      useConnection("ops", TestRepository.branch(), "\\workflows");

      assertEquals("second version", read(resolve("ops:///daily.hwf")));
    }
  }

  @Nested
  @DisplayName("The checkout is cached, not rebuilt for every read")
  class Caching {

    @Test
    @DisplayName("Reading the same revision twice reuses the first checkout")
    void readingTwiceReusesTheCheckout() throws Exception {
      Path checkoutRoot = tempFolder.resolve("cache");
      GitConnection cached = connection("ops", TestRepository.branch(), "");
      cached.setCacheFolder(checkoutRoot.toString());

      Path first = new GitCheckout(new Variables(), cached).getWorkingCopy();
      assertTrue(Files.isRegularFile(first.resolve(GitCheckout.READY_MARKER)));

      // Remove the source repository. A second read of the same settings cannot clone it again, so
      // it can only succeed by finding the checkout the first read left behind.
      repository.close();
      Files.walk(tempFolder.resolve("remote"))
          .sorted((a, b) -> b.getNameCount() - a.getNameCount())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

      Path second = new GitCheckout(new Variables(), cached).getWorkingCopy();

      assertEquals(first, second, "the second read should have reused the first checkout");
      assertTrue(
          Files.isRegularFile(second.resolve("workflows/daily.hwf")),
          "the reused checkout should still hold the repository");
    }
  }

  @Nested
  @DisplayName("A read only connection refuses to write")
  class ReadOnly {

    @Test
    @DisplayName("A read only connection reports a file as not writeable")
    void aReadOnlyConnectionReportsNotWriteable() throws Exception {
      assertFalse(resolve("ops:///workflows/daily.hwf").isWriteable());
    }

    @Test
    @DisplayName("A read only connection refuses the write rather than silently dropping it")
    void aReadOnlyConnectionRefusesTheWrite() throws Exception {
      FileObject file = resolve("ops:///workflows/daily.hwf");

      assertThrows(Exception.class, () -> file.getContent().getOutputStream(false));
    }

    @Test
    @DisplayName("A writable connection writes into the working copy")
    void aWritableConnectionWritesIntoTheWorkingCopy() throws Exception {
      GitConnection writable = connection("ops", TestRepository.branch(), "");
      writable.setReadOnly(false);
      register("ops", writable);

      FileObject file = resolve("ops:///workflows/daily.hwf");
      assertTrue(file.isWriteable());
      try (var out = file.getContent().getOutputStream(false)) {
        out.write("written by hop".getBytes(StandardCharsets.UTF_8));
      }

      assertEquals("written by hop", read(file));
    }
  }

  @Nested
  @DisplayName("The connection can be asked whether the repository answers")
  class Probing {

    @Test
    @DisplayName("A probe names the branch it was asked for")
    void aProbeNamesTheBranch() throws Exception {
      String message =
          new GitCheckout(new Variables(), connection("ops", TestRepository.branch(), "")).probe();

      assertTrue(message.contains("main"), message);
    }

    @Test
    @DisplayName("A missing deploy key fails before any fetch")
    void aMissingDeployKeyFailsBeforeAnyFetch() {
      GitConnection connection = connection("ops", TestRepository.branch(), "");
      connection.setAuthType(GitAuthType.DEPLOY_KEY);
      connection.setRepositoryUrl("ssh://git@example.invalid/hop.git");
      connection.setPrivateKeyFile(tempFolder.resolve("no-such-key").toString());

      GitCheckoutException e =
          assertThrows(
              GitCheckoutException.class,
              () -> new GitCheckout(new Variables(), connection).probe());

      assertTrue(e.getMessage().contains("not readable"), e.getMessage());
    }
  }

  @Nested
  @DisplayName("A path cannot walk out of the repository")
  class Escaping {

    @Test
    @DisplayName("A base path which points outside the repository is refused")
    void aBasePathOutsideTheRepositoryIsRefused() throws Exception {
      useConnection("ops", TestRepository.branch(), "../..");

      FileSystemException e =
          assertThrows(FileSystemException.class, () -> resolve("ops:///workflows/daily.hwf"));
      assertNotNull(e.getMessage());
    }

    @Test
    @DisplayName("A symlink which stays inside the checkout is followed")
    void aSymlinkInsideTheCheckoutIsFollowed() throws Exception {
      Path root = checkedOut();
      Files.createSymbolicLink(root.resolve("workflows/alias.hwf"), Path.of("daily.hwf"));

      assertEquals("second version", read(resolve("ops:///workflows/alias.hwf")));
    }

    @Test
    @DisplayName("A symlink which points outside the checkout is refused")
    void aSymlinkOutsideTheCheckoutIsRefused() throws Exception {
      Path root = checkedOut();
      Files.createSymbolicLink(root.resolve("workflows/outside.txt"), Path.of("/etc/hostname"));

      FileSystemException e =
          assertThrows(
              FileSystemException.class,
              () -> resolve("ops:///workflows/outside.txt").getContent().getInputStream());
      assertNotNull(e.getMessage());
    }
  }

  // --- helpers --------------------------------------------------------------------------------

  private GitConnection connection(String name, String revision, String basePath) {
    GitConnection connection = new GitConnection();
    connection.setName(name);
    connection.setRepositoryUrl(repository.url());
    connection.setRevision(revision);
    connection.setBasePath(basePath);
    connection.setCacheFolder(tempFolder.resolve("cache").toString());
    return connection;
  }

  private void useConnection(String name, String revision, String basePath) throws Exception {
    register(name, connection(name, revision, basePath));
  }

  /**
   * A manager with one connection on it, replacing any manager a previous call left behind.
   *
   * <p>Deliberately replaces rather than reuses: a file system manager caches the file system of a
   * scheme, so a test which swapped the settings of a connection under the same scheme would keep
   * reading the checkout of the first one.
   */
  private void register(String scheme, GitConnection connection) throws Exception {
    if (manager != null) {
      manager.close();
      manager = null;
    }
    manager = new DefaultFileSystemManager();
    manager.addProvider(scheme, new GitFileProvider(new Variables(), connection));
    manager.init();
  }

  private FileObject resolve(String uri) throws Exception {
    return manager.resolveFile(uri);
  }

  /**
   * The working copy of the connection the test registered, so a test can plant a symlink in it.
   */
  private Path checkedOut() throws Exception {
    // Resolving a file is what builds the checkout. The symlink tests then edit that folder.
    assertTrue(resolve("ops:///workflows/daily.hwf").exists());
    return new GitCheckout(new Variables(), connection("ops", TestRepository.branch(), ""))
        .getWorkingCopy();
  }

  private String read(FileObject file) throws Exception {
    try (InputStream in = file.getContent().getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private List<String> namesOf(FileObject folder) throws Exception {
    List<String> names = new ArrayList<>();
    for (FileObject child : folder.getChildren()) {
      names.add(child.getName().getBaseName());
    }
    return names;
  }
}
