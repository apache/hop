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
package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.commons.vfs2.provider.local.GenericFileNameParser;
import org.apache.commons.vfs2.provider.local.LocalFileNameParser;
import org.apache.commons.vfs2.provider.local.WindowsFileNameParser;
import org.apache.hop.junit.vfs.VfsPathShapes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * What the local file name parsers make of the awkward path shapes in {@link VfsPathShapes}.
 *
 * <p>Both parsers are exercised by name rather than through {@link HopVfs}, so this pins Windows
 * behaviour on every platform. That matters more here than anywhere else: the CI workflows all run
 * on {@code ubuntu-latest}, so nothing else in the build would notice a Windows path regression,
 * and UNC is the one place in Commons VFS where a leading double slash is meant to survive.
 */
class LocalFileNameParserShapeTest {

  private static VfsPathShapes.NameParser parserFor(LocalFileNameParser parser) {
    FileSystemManager fsm = mock(FileSystemManager.class);
    when(fsm.getSchemes()).thenReturn(new String[] {"file"});
    VfsComponentContext context = mock(VfsComponentContext.class);
    when(context.getFileSystemManager()).thenReturn(fsm);
    return uri -> parser.parseUri(context, null, uri);
  }

  @Nested
  @DisplayName("Windows, drive letter")
  class WindowsDrive {

    private static final String BASE = "file:///C:";
    private final VfsPathShapes.NameParser parser = parserFor(new WindowsFileNameParser());

    @Test
    void paths() {
      VfsPathShapes.assertPaths(
          BASE,
          parser,
          """
          plain                   /dir/file.txt
          leading-double-slash    /dir/file.txt
          leading-triple-slash    /dir/file.txt
          interior-double-slash   /dir/file.txt
          trailing-slash          /dir
          root-only               /
          no-path                 error          # "file:///C:" has no separator after the drive
          dot-segment             /dir/file.txt
          parent-segment          /dir/file.txt
          parent-above-root       error
          encoded-slash           /dir/file.txt  # %2F decodes before it is normalised away
          space                   /dir/my file.txt
          encoded-space           /dir/my file.txt
          hash                    /dir/file#1.txt
          question-mark           /dir/file?1.txt
          plus                    /dir/file+1.txt
          backslash               /dir/file.txt  # a backslash becomes a separator
          leading-dots-in-name    /dir/..hidden
          unicode                 /dir/naïve.txt
          """);
    }

    @Test
    void theDriveStaysInTheRoot() throws Exception {
      assertEquals("file:///C:/", parser.parse(BASE + "/dir/file.txt").getRootURI());
    }

    @Test
    void uriRoundTrips() {
      VfsPathShapes.assertUriRoundTrips(BASE, parser);
    }
  }

  /**
   * The one shape which survives anywhere in Commons VFS: {@code //server/share} is hoisted into
   * the root by {@link WindowsFileNameParser}, so it is never handed to the normaliser as a path.
   * Every URI below keeps all four of its leading slashes, and that is what a UNC fix must not
   * break.
   */
  @Nested
  @DisplayName("Windows, UNC share")
  class WindowsUnc {

    private static final String BASE = "file:////server/share";
    private final VfsPathShapes.NameParser parser = parserFor(new WindowsFileNameParser());

    private FileName parse(String uri) throws Exception {
      return parser.parse(uri);
    }

    @Test
    void paths() {
      VfsPathShapes.assertPaths(
          BASE,
          parser,
          """
          plain                   /dir/file.txt
          leading-double-slash    /dir/file.txt
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
          question-mark           /dir/file?1.txt
          plus                    /dir/file+1.txt
          backslash               /dir/file.txt
          leading-dots-in-name    /dir/..hidden
          unicode                 /dir/naïve.txt
          """);
    }

    /**
     * The root swallows exactly two segments, whatever they were meant to be: in {@code
     * file:////servername/path/on/server} the share is {@code path}, not {@code servername}, and
     * the file is {@code /on/server} beneath it.
     */
    @Test
    void theRootIsHostAndShare() throws Exception {
      final FileName name = parse("file:////servername/path/on/server");

      assertEquals("file:////servername/path/", name.getRootURI());
      assertEquals("/on/server", name.getPath());
      assertEquals("file:////servername/path/on/server", name.getURI());
    }

    /**
     * Two, three and four slashes all mean the same UNC share, because the parser skips up to four
     * and only then looks for a drive letter. So {@code file:///servername/path} - which on any
     * other platform is the plain local path {@code /servername/path} - is the share {@code
     * \\servername\path} here. Nothing in CI would notice: every workflow runs on Linux.
     */
    @Test
    void twoThreeAndFourSlashesAllMeanTheSameShare() throws Exception {
      for (String uri :
          new String[] {
            "file://servername/path/on/server",
            "file:///servername/path/on/server",
            "file:////servername/path/on/server"
          }) {
        assertEquals("file:////servername/path/", parse(uri).getRootURI(), uri);
        assertEquals("/on/server", parse(uri).getPath(), uri);
      }
    }

    /** A drive letter still wins over the UNC reading. */
    @Test
    void aDriveLetterIsNotAShare() throws Exception {
      assertEquals("file:///C:/", parse("file:///C:/temp/x.txt").getRootURI());
    }

    /** A host on its own is not a share, and is refused rather than guessed at. */
    @Test
    void aHostWithoutAShareIsRejected() {
      final FileSystemException e =
          assertThrows(FileSystemException.class, () -> parse("file:////servername"));
      assertTrue(
          e.getMessage().contains("Share name missing"), "unexpected message: " + e.getMessage());
    }

    /** The share is part of the root, which is why it survives normalisation. */
    @Test
    void theShareStaysInTheRoot() throws Exception {
      assertEquals("file:////server/share/", parser.parse(BASE + "/dir/file.txt").getRootURI());
      assertEquals(
          "file:////server/share/dir/file.txt", parser.parse(BASE + "/dir/file.txt").getURI());
    }

    @Test
    void uriRoundTrips() {
      VfsPathShapes.assertUriRoundTrips(BASE, parser);
    }
  }

  /**
   * The parser every non-Windows platform gets. It has no root prefix to hoist anything into, so a
   * leading double slash is flattened into the path and axed - the same collapse which makes a
   * mainframe SFTP data set unaddressable.
   */
  @Nested
  @DisplayName("Generic (non-Windows)")
  class Generic {

    private static final String BASE = "file://";
    private final VfsPathShapes.NameParser parser = parserFor(new GenericFileNameParser());

    private FileName parse(String uri) throws Exception {
      return parser.parse(uri);
    }

    @Test
    void paths() {
      VfsPathShapes.assertPaths(
          BASE,
          parser,
          """
          plain                   /dir/file.txt
          leading-double-slash    /dir/file.txt  # no hoist, so the share is flattened away
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
          question-mark           /dir/file?1.txt
          plus                    /dir/file+1.txt
          backslash               /dir/file.txt
          leading-dots-in-name    /dir/..hidden
          unicode                 /dir/naïve.txt
          """);
    }

    /**
     * The same URI the Windows parser reads as a share is an ordinary local path here - so {@code
     * file:////servername/path/on/server} names two different files depending on the platform.
     */
    @Test
    void aShareIsJustAPathHere() throws Exception {
      final FileName name = parse("file:////servername/path/on/server");

      assertEquals("file:///", name.getRootURI());
      assertEquals("/servername/path/on/server", name.getPath());
      assertEquals("file:///servername/path/on/server", name.getURI());
    }

    /** Everything lands under a bare root, UNC-looking names included. */
    @Test
    void thereIsNoRootPrefixToHoistInto() throws Exception {
      assertEquals("file:///", parser.parse(BASE + "//server/share/x").getRootURI());
      assertEquals("/server/share/x", parser.parse(BASE + "//server/share/x").getPath());
    }

    @Test
    void uriRoundTrips() {
      VfsPathShapes.assertUriRoundTrips(BASE, parser);
    }
  }
}
