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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.git.metadata.GitConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The checkout folder of a connection is the cache key, so two connections which would produce
 * different working copies must never land in the same folder, and one connection must always land
 * in the same one.
 */
class GitCheckoutFolderTest {

  @TempDir private java.nio.file.Path cacheRoot;

  @Test
  @DisplayName("The same connection always resolves to the same folder")
  void theSameConnectionResolvesToTheSameFolder() {
    GitCheckout checkout = new GitCheckout(new Variables(), connection("ops", "main", "/"));

    assertEquals(
        checkout.checkoutFolder("https://example.com/hop.git"),
        checkout.checkoutFolder("https://example.com/hop.git"));
  }

  @Test
  @DisplayName("Two connections of two repositories do not share a checkout")
  void twoRepositoriesDoNotShareACheckout() {
    GitCheckout a = new GitCheckout(new Variables(), connection("ops", "main", "/"));
    GitCheckout b = new GitCheckout(new Variables(), connection("ops", "main", "/"));

    assertNotEquals(
        a.checkoutFolder("https://example.com/one.git"),
        b.checkoutFolder("https://example.com/two.git"));
  }

  @Test
  @DisplayName("Two revisions of one repository do not share a checkout")
  void twoRevisionsDoNotShareACheckout() {
    GitCheckout a = new GitCheckout(new Variables(), connection("ops", "main", "/"));
    GitCheckout b = new GitCheckout(new Variables(), connection("ops", "release", "/"));

    assertNotEquals(
        a.checkoutFolder("https://example.com/hop.git"),
        b.checkoutFolder("https://example.com/hop.git"));
  }

  @Test
  @DisplayName("Two base paths of one revision do not share a checkout")
  void twoBasePathsDoNotShareACheckout() {
    GitCheckout a = new GitCheckout(new Variables(), connection("ops", "main", "/workflows"));
    GitCheckout b = new GitCheckout(new Variables(), connection("ops", "main", "/pipelines"));

    assertNotEquals(
        a.checkoutFolder("https://example.com/hop.git"),
        b.checkoutFolder("https://example.com/hop.git"));
  }

  @Test
  @DisplayName("The checkout lives under the folder the connection names")
  void theCheckoutLivesUnderTheConfiguredFolder() {
    GitCheckout checkout = new GitCheckout(new Variables(), connection("ops", "main", "/"));

    assertTrue(
        checkout.checkoutFolder("https://example.com/hop.git").startsWith(cacheRoot.resolve("ops")),
        checkout.checkoutFolder("https://example.com/hop.git").toString());
  }

  @Test
  @DisplayName("A connection name which is a path cannot escape the cache")
  void aConnectionNameCannotEscapeTheCache() {
    // The name of a connection becomes the scheme, so it is whatever the user typed. A name with a
    // path separator in it must not be able to write outside the cache root.
    GitCheckout checkout =
        new GitCheckout(new Variables(), connection("../../escape", "main", "/"));

    java.nio.file.Path resolved =
        checkout.checkoutFolder("https://example.com/hop.git").normalize();

    assertTrue(
        resolved.startsWith(cacheRoot.normalize()),
        "the checkout escaped the cache root: " + resolved);
  }

  @Test
  @DisplayName("A full SHA-1 or SHA-256 hex string is a commit id, and nothing else is")
  void onlyAFullCommitIdIsACommitId() {
    assertTrue(GitCheckout.looksLikeCommitId("0".repeat(40)));
    assertTrue(GitCheckout.looksLikeCommitId("a1b2c3d4e5f6".repeat(3) + "abcd"));
    assertTrue(GitCheckout.looksLikeCommitId("ab".repeat(32)), "SHA-256 is 64 hex digits");
    org.junit.jupiter.api.Assertions.assertFalse(
        GitCheckout.looksLikeCommitId("main"), "a branch name is not a commit id");
    org.junit.jupiter.api.Assertions.assertFalse(
        GitCheckout.looksLikeCommitId("0".repeat(39)), "39 characters is not a commit id");
    org.junit.jupiter.api.Assertions.assertFalse(
        GitCheckout.looksLikeCommitId("0".repeat(63)), "63 characters is not a commit id");
    org.junit.jupiter.api.Assertions.assertFalse(
        GitCheckout.looksLikeCommitId("z".repeat(40)), "'z' is not a hex digit");
    org.junit.jupiter.api.Assertions.assertFalse(GitCheckout.looksLikeCommitId(null));
  }

  @Test
  @DisplayName("An SSH user is written into an SSH URL which does not name one")
  void anSshUserIsWrittenIntoAnSshUrl() {
    GitConnection connection = connection("ops", "main", "/");
    connection.setAuthType(GitAuthType.DEPLOY_KEY);
    connection.setSshUser("git");

    assertEquals(
        "ssh://git@github.com/apache/hop.git",
        new GitCheckout(new Variables(), connection)
            .transportUrl("ssh://github.com/apache/hop.git"));
  }

  @Test
  @DisplayName("A URL which already names its user is left alone")
  void aUrlWhichNamesItsUserIsLeftAlone() {
    GitConnection connection = connection("ops", "main", "/");
    connection.setAuthType(GitAuthType.DEPLOY_KEY);
    connection.setSshUser("git");

    assertEquals(
        "git@github.com:apache/hop.git",
        new GitCheckout(new Variables(), connection).transportUrl("git@github.com:apache/hop.git"));
  }

  private GitConnection connection(String name, String revision, String basePath) {
    GitConnection connection = new GitConnection();
    connection.setName(name);
    connection.setRepositoryUrl("https://example.com/hop.git");
    connection.setRevision(revision);
    connection.setBasePath(basePath);
    connection.setCacheFolder(cacheRoot.toString());
    return connection;
  }
}
