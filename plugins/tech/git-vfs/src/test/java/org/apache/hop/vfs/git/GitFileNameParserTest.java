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

import java.nio.file.Path;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.git.metadata.GitConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A URI behind a git connection has to survive the round trip: what the user types is what the
 * connection serves, and what a file object hands back is something VFS can resolve again.
 */
class GitFileNameParserTest {

  @TempDir private Path tempFolder;

  private TestRepository repository;
  private DefaultFileSystemManager manager;
  private GitFileProvider provider;

  @BeforeEach
  void setUp() throws Exception {
    repository = TestRepository.create(tempFolder.resolve("remote"));
    manager = new DefaultFileSystemManager();
    provider = new GitFileProvider(new Variables(), connection("ops", TestRepository.branch()));
    manager.addProvider("ops", provider);
    manager.init();
  }

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
      manager = null;
    }
    repository.close();
  }

  @Test
  @DisplayName("Any number of slashes after the scheme names the same file")
  void anyNumberOfSlashesNamesTheSameFile() throws Exception {
    assertEquals("/workflows/daily.hwf", pathOf("ops://workflows/daily.hwf"));
    assertEquals("/workflows/daily.hwf", pathOf("ops:///workflows/daily.hwf"));
    assertEquals("/workflows/daily.hwf", pathOf("ops:////workflows/daily.hwf"));
  }

  @Test
  @DisplayName("The scheme is the name of the connection and nothing else")
  void theSchemeIsTheNameOfTheConnection() throws Exception {
    FileSystemException e =
        assertThrows(
            FileSystemException.class, () -> manager.resolveFile("other://workflows/daily.hwf"));
    assertTrue(e.getMessage().contains("other"), "the error should name the wrong scheme: " + e);
  }

  @Test
  @DisplayName("The URI carries no repository, no revision and no credentials")
  void theUriCarriesNothingSecret() throws Exception {
    FileObject file = manager.resolveFile("ops:///workflows/daily.hwf");

    String uri = file.getName().getURI();
    assertEquals("ops:///workflows/daily.hwf", uri);
    assertFalse(uri.contains("remote"), "the repository leaked into the URI: " + uri);
    assertFalse(uri.contains("main"), "the revision leaked into the URI: " + uri);
  }

  @Test
  @DisplayName("A URI taken from a file object can be handed straight back to VFS")
  void aUriFromAFileObjectResolvesAgain() throws Exception {
    FileObject file = manager.resolveFile("ops:///workflows/daily.hwf");

    FileObject again = manager.resolveFile(file.getName().getURI());

    assertEquals(file.getName(), again.getName());
  }

  @Test
  @DisplayName("A path relative to a file resolves against it")
  void aRelativePathResolvesAgainstItsBase() throws Exception {
    FileObject file = manager.resolveFile("ops:///pipelines/readme.txt");

    FileObject child = file.resolveFile("sub/step.hpl");

    assertEquals("/pipelines/readme.txt/sub/step.hpl", child.getName().getPath());
  }

  @Test
  @DisplayName("A read only connection marks its names read only")
  void aReadOnlyConnectionMarksItsNamesReadOnly() throws Exception {
    FileName name = provider.parseUri(null, "ops:///workflows/daily.hwf");

    assertTrue(((GitFileName) name).isReadOnly());
  }

  @Test
  @DisplayName("A writable connection does not mark its names read only")
  void aWritableConnectionDoesNotMarkItsNamesReadOnly() throws Exception {
    GitConnection connection = connection("ops", TestRepository.branch());
    connection.setReadOnly(false);

    // A provider which was never added to a file system manager has no context to parse a URI
    // with, so give it one rather than calling the parser directly.
    try (DefaultFileSystemManager writableManager = new DefaultFileSystemManager()) {
      writableManager.addProvider("ops", new GitFileProvider(new Variables(), connection));
      writableManager.init();

      FileName name = writableManager.resolveFile("ops:///workflows/daily.hwf").getName();

      assertNotNull(name);
      assertFalse(((GitFileName) name).isReadOnly());
    }
  }

  private String pathOf(String uri) throws Exception {
    return provider.parseUri(null, uri).getPath();
  }

  private GitConnection connection(String name, String revision) {
    GitConnection connection = new GitConnection();
    connection.setName(name);
    connection.setRepositoryUrl(repository.url());
    connection.setRevision(revision);
    connection.setCacheFolder(tempFolder.resolve("cache").toString());
    return connection;
  }
}
