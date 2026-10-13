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
package org.apache.hop.vfs.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.vfs.VfsPathShapes;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.Test;

/**
 * What the named FTP connection parser makes of the awkward path shapes in {@link VfsPathShapes}.
 *
 * <p>This is the FTP twin of {@code SftpPathShapeTest}, and the two are deliberately out of step
 * for now: SFTP keeps a path rooted at a second slash, so that a z/OS MVS data set can be
 * addressed, and FTP still collapses every leading slash into one. z/OS FTP names data sets the
 * same {@code //A.B.C} way, so the same change belongs here - but the SFTP fix reaches the wire
 * through Hop's own fork of the Commons VFS SFTP provider, and there is no such fork for FTP: the
 * path sent to the server is built in {@code FtpFileObject}'s constructor, which Hop does not own.
 * The rows below say what FTP does today, so that whatever is done about that is measurable.
 */
class FtpPathShapeTest {

  private static final String BASE = "prod:/";

  private final VfsPathShapes.NameParser parser = connectionParser();

  private VfsPathShapes.NameParser connectionParser() {
    FtpConnection connection = new FtpConnection();
    connection.setName("prod");
    connection.setServerName("mainframe.example.com");
    connection.setServerPort("21");
    connection.setUserName("hop");

    FileSystemManager fsm = mock(FileSystemManager.class);
    when(fsm.getSchemes()).thenReturn(new String[] {"prod"});
    VfsComponentContext context = mock(VfsComponentContext.class);
    when(context.getFileSystemManager()).thenReturn(fsm);

    FtpConnectionFileNameParser nameParser =
        new FtpConnectionFileNameParser(new Variables(), connection);
    return uri -> nameParser.parseUri(context, null, uri);
  }

  @Test
  void paths() {
    VfsPathShapes.assertPaths(
        BASE,
        parser,
        """
        plain                   /dir/file.txt
        leading-double-slash    /dir/file.txt    # an MVS data set, collapsed - unlike SFTP
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

  /** Every leading slash count lands on the same root, which is what SFTP no longer does. */
  @Test
  void everySlashCountMeansTheSameRoot() throws Exception {
    assertEquals("prod:///", parser.parse("prod://dir/file.txt").getRootURI());
    assertEquals("prod:///", parser.parse("prod:///dir/file.txt").getRootURI());
    assertEquals("prod:///", parser.parse("prod:////dir/file.txt").getRootURI());
    assertEquals("prod:///dir/file.txt", parser.parse("prod:////dir/file.txt").getURI());
  }

  @Test
  void uriRoundTrips() {
    VfsPathShapes.assertUriRoundTrips(BASE, parser);
  }
}
