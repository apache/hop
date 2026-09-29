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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

/**
 * The driver against a real git server, over a real socket.
 *
 * <p>{@code GitVfsTest} covers the same behaviour against a repository in a temporary folder, which
 * exercises the code but not the transport: JGit clones it through its own file transport, and
 * nothing here would notice a connection which never leaves the machine. These tests close that
 * gap. They are the reason a {@code my-git://} connection works from a container rather than only
 * in a developer's unit test suite.
 *
 * <p>Skipped when there is no Docker daemon, so a build without Docker is unaffected.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GitVfsContainerIT {

  @TempDir private static Path repositoriesFolder;

  private static GitDaemonContainer daemon;
  private static TestRepository repository;
  private static String repositoryName;

  private DefaultFileSystemManager manager;

  @BeforeAll
  static void startTheServer() throws Exception {
    Assumptions.assumeTrue(dockerIsAvailable(), "Docker is not available");
    repositoryName = "hop-it-" + System.nanoTime();
    Path served = repositoriesFolder.resolve(repositoryName);
    repository = TestRepository.create(served);
    // The repository has to be complete before the daemon is asked about it: a clone of a folder
    // git has not finished writing is a clone of nothing.
    repository.close();
    daemon = GitDaemonContainer.start(repositoriesFolder, repositoryName);
  }

  @AfterAll
  static void stopTheServer() {
    if (daemon != null) {
      daemon.close();
    }
  }

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
      manager = null;
    }
  }

  @Test
  @Order(1)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A file is read out of a repository served over the network")
  void aFileIsReadOutOfARepositoryServedOverTheNetwork() throws Exception {
    connect("main", "");

    FileObject file = resolve("ops:///workflows/daily.hwf");

    assertTrue(file.exists(), "the file should be in the checkout of the served repository");
    assertEquals("second version", read(file));
  }

  @Test
  @Order(2)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A folder of the served repository is listed over the network")
  void aFolderOfTheServedRepositoryIsListed() throws Exception {
    connect("main", "");

    List<String> names = new ArrayList<>();
    for (FileObject child : resolve("ops:///workflows/").getChildren()) {
      names.add(child.getName().getBaseName());
    }

    assertTrue(names.contains("daily.hwf"), names.toString());
    assertTrue(names.contains("nested"), names.toString());
  }

  @Test
  @Order(3)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A tag of the served repository is fetched and checked out")
  void aTagOfTheServedRepositoryIsFetched() throws Exception {
    connect("v1", "");

    assertEquals("first version", read(resolve("ops:///workflows/daily.hwf")));
  }

  @Test
  @Order(4)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A commit id of the served repository is fetched and checked out")
  void aCommitIdOfTheServedRepositoryIsFetched() throws Exception {
    connect(revisionOfFirstCommit(), "");

    assertEquals("first version", read(resolve("ops:///workflows/daily.hwf")));
  }

  @Test
  @Order(5)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A base path of the served repository moves the root of the connection")
  void aBasePathOfTheServedRepositoryMovesTheRoot() throws Exception {
    connect("main", "workflows");

    assertEquals("second version", read(resolve("ops:///daily.hwf")));
  }

  @Test
  @Order(6)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A revision which is not in the served repository is an error")
  void aRevisionWhichIsNotThereIsAnError() throws Exception {
    connect("no-such-branch", "");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> resolve("ops:///workflows/daily.hwf"));
    assertTrue(
        e.getMessage().contains("no-such-branch"),
        "the error should name the branch which was not found: " + e.getMessage());
  }

  @Test
  @Order(7)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A second read of the same revision does not clone again")
  void aSecondReadDoesNotCloneAgain() throws Exception {
    GitConnection connection = connection("main", "");
    GitCheckout checkout = new GitCheckout(new Variables(), connection);
    Path first = checkout.getWorkingCopy();
    var writtenAt = Files.getLastModifiedTime(first.resolve(GitCheckout.READY_MARKER));

    // The cache key includes the URL, so this has to be the same connection. A second checkout
    // which fetched again would rewrite the ready marker.
    Path second = checkout.getWorkingCopy();

    assertEquals(first, second);
    assertEquals(writtenAt, Files.getLastModifiedTime(second.resolve(GitCheckout.READY_MARKER)));
    assertTrue(Files.isRegularFile(second.resolve("workflows/daily.hwf")));
  }

  @Test
  @Order(8)
  @EnabledIf("dockerIsAvailable")
  @DisplayName("A read only connection served over the network refuses a write")
  void aReadOnlyConnectionRefusesAWrite() throws Exception {
    connect("main", "");

    FileObject file = resolve("ops:///workflows/daily.hwf");

    assertFalse(file.isWriteable());
    assertThrows(Exception.class, () -> file.getContent().getOutputStream(false));
  }

  // --- helpers --------------------------------------------------------------------------------

  static boolean dockerIsAvailable() {
    try {
      return org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
    } catch (Throwable e) {
      return false;
    }
  }

  /** The commit the {@code v1} tag points at, read from the repository the daemon serves. */
  private static String revisionOfFirstCommit() throws Exception {
    try (org.eclipse.jgit.api.Git git =
        org.eclipse.jgit.api.Git.open(repositoriesFolder.resolve(repositoryName).toFile())) {
      return git.getRepository().resolve("v1").getName();
    }
  }

  private GitConnection connection(String revision, String basePath) {
    GitConnection connection = new GitConnection();
    connection.setName("ops");
    connection.setRepositoryUrl(daemon.url());
    connection.setRevision(revision);
    connection.setBasePath(basePath);
    connection.setCacheFolder(repositoriesFolder.resolve("cache").toString());
    // A deployment talks to a server which may be slow to answer; a test which does not would fail
    // on a loaded machine rather than on anything to do with git.
    connection.setTimeoutSeconds("120");
    return connection;
  }

  private void connect(String revision, String basePath) throws Exception {
    GitConnection connection = connection(revision, basePath);
    manager = new DefaultFileSystemManager();
    manager.addProvider("ops", new GitFileProvider(new Variables(), connection));
    manager.init();
  }

  private FileObject resolve(String uri) throws Exception {
    return manager.resolveFile(uri);
  }

  private String read(FileObject file) throws Exception {
    try (InputStream in = file.getContent().getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
