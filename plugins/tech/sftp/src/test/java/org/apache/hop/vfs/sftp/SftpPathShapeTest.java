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
package org.apache.hop.vfs.sftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.vfs.VfsPathShapes;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.apache.hop.vfs.sftp.provider.SftpFileProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * What the two SFTP file name parsers make of the awkward path shapes in {@link VfsPathShapes}.
 *
 * <p>Neither parser needs a server: this is name parsing only, so the tables below say what a path
 * turns into before anything is ever sent over the wire.
 */
class SftpPathShapeTest {

  private static DefaultFileSystemManager manager;

  @BeforeAll
  static void setUp() throws Exception {
    manager = new DefaultFileSystemManager();
    manager.addProvider("sftp", new SftpFileProvider());
    manager.addProvider("file", new DefaultLocalFileProvider());
    manager.init();
  }

  @AfterAll
  static void tearDown() {
    if (manager != null) {
      manager.close();
    }
  }

  /**
   * The parser behind a plain {@code sftp://} URI. The path of a name is always an ordinary
   * absolute path: a path rooted at a second slash keeps it in the <em>root</em>, so none of the
   * rows below move when one is used.
   */
  @Nested
  @DisplayName("sftp:// URIs")
  class RawUris {

    private static final String BASE = "sftp://user@host:22";
    private final VfsPathShapes.NameParser parser = uri -> manager.resolveURI(uri);

    @Test
    void paths() {
      VfsPathShapes.assertPaths(
          BASE,
          parser,
          """
          plain                   /dir/file.txt
          leading-double-slash    /dir/file.txt    # the data set: the extra slash is in the root
          leading-triple-slash    /dir/file.txt
          interior-double-slash   /dir/file.txt
          trailing-slash          /dir
          root-only               /
          no-path                 /
          dot-segment             /dir/file.txt
          parent-segment          /dir/file.txt
          parent-above-root       error
          encoded-slash           /dir/file.txt    # %2F is decoded before it is normalised away
          space                   /dir/my file.txt
          encoded-space           /dir/my file.txt
          hash                    /dir/file#1.txt
          question-mark           /dir/file        # "?1.txt" is taken for a URL query
          plus                    /dir/file+1.txt
          backslash               /dir/file.txt
          leading-dots-in-name    /dir/..hidden
          unicode                 /dir/naïve.txt
          """);
    }

    /** The second slash rides in the root, which is what lets it survive being derived from. */
    @Test
    void theSecondSlashIsInTheRoot() throws Exception {
      assertEquals("sftp://user@host/", parser.parse(BASE + "/dir/file.txt").getRootURI());
      assertEquals("sftp://user@host//", parser.parse(BASE + "//dir/file.txt").getRootURI());
      assertEquals(
          "sftp://user@host//dir/file.txt", parser.parse(BASE + "//dir/file.txt").getURI());
    }

    @Test
    void uriRoundTrips() {
      VfsPathShapes.assertUriRoundTrips(BASE, parser);
    }
  }

  /**
   * The parser behind a named SFTP connection, which is Hop's own code. The two slashes behind the
   * scheme are the separator, so {@code prod://tmp/x} and {@code prod:///tmp/x} both mean {@code
   * /tmp/x}; a third pair makes {@code prod:////A.B.C} the data set {@code //A.B.C}.
   */
  @Nested
  @DisplayName("named connection URIs")
  class NamedConnection {

    private static final String BASE = "prod:/";

    private final VfsPathShapes.NameParser parser = namedConnectionParser();

    private VfsPathShapes.NameParser namedConnectionParser() {
      SftpConnection connection = new SftpConnection();
      connection.setName("prod");
      connection.setServerName("mainframe.example.com");
      connection.setServerPort("22");
      connection.setUsername("hop");

      FileSystemManager fsm = mock(FileSystemManager.class);
      when(fsm.getSchemes()).thenReturn(new String[] {"prod"});
      VfsComponentContext context = mock(VfsComponentContext.class);
      when(context.getFileSystemManager()).thenReturn(fsm);

      SftpConnectionFileNameParser nameParser =
          new SftpConnectionFileNameParser(new Variables(), connection);
      return uri -> nameParser.parseUri(context, null, uri);
    }

    @Test
    void paths() {
      VfsPathShapes.assertPaths(
          BASE,
          parser,
          """
          plain                   /dir/file.txt
          leading-double-slash    /dir/file.txt    # the data set: the extra slash is in the root
          leading-triple-slash    /dir/file.txt
          interior-double-slash   /dir/file.txt
          trailing-slash          /dir
          root-only               /
          no-path                 /
          dot-segment             /dir/file.txt
          parent-segment          /dir/file.txt
          parent-above-root       error
          encoded-slash           /dir/file.txt
          space                   /dir/my file.txt
          encoded-space           /dir/my file.txt
          hash                    /dir/file#1.txt
          question-mark           /dir/file?1.txt  # kept, unlike the sftp:// parser above
          plus                    /dir/file+1.txt
          backslash               /dir/file.txt
          leading-dots-in-name    /dir/..hidden
          unicode                 /dir/naïve.txt
          """);
    }

    /** The server stays out of the URI, so the connection name is the whole root. */
    @Test
    void theConnectionNameIsTheRoot() throws Exception {
      assertEquals("prod:///", parser.parse(BASE + "/dir/file.txt").getRootURI());
      assertEquals("prod:///dir/file.txt", parser.parse(BASE + "/dir/file.txt").getURI());
      assertEquals("prod:////", parser.parse(BASE + "///dir/file.txt").getRootURI());
      assertEquals("prod:////dir/file.txt", parser.parse(BASE + "///dir/file.txt").getURI());
    }

    @Test
    void uriRoundTrips() {
      VfsPathShapes.assertUriRoundTrips(BASE, parser);
    }
  }
}
