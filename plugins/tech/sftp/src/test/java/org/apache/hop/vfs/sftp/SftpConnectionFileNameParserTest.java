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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SftpConnectionFileNameParserTest {

  private SftpConnectionFileNameParser parser;
  private VfsComponentContext context;

  @BeforeEach
  void setUp() {
    SftpConnection connection = new SftpConnection();
    connection.setName("prod");
    connection.setServerName("sftp.example.com");
    connection.setServerPort("2222");
    connection.setUsername("hop");

    IVariables variables = new Variables();
    parser = new SftpConnectionFileNameParser(variables, connection);

    FileSystemManager manager = mock(FileSystemManager.class);
    when(manager.getSchemes()).thenReturn(new String[] {"prod"});
    context = mock(VfsComponentContext.class);
    when(context.getFileSystemManager()).thenReturn(manager);
  }

  /** Any number of slashes behind the scheme means the same absolute path on the server. */
  @Test
  void testPathsAreAbsoluteOnTheServer() throws Exception {
    assertEquals("/tmp/hop/file.txt", parse("prod://tmp/hop/file.txt").getPath());
    assertEquals("/tmp/hop/file.txt", parse("prod:///tmp/hop/file.txt").getPath());
    assertEquals("/tmp/hop/file.txt", parse("prod:////tmp/hop/file.txt").getPath());
    assertEquals("/", parse("prod://").getPath());
  }

  /** The name carries the server so the SFTP provider can connect, but never shows it. */
  @Test
  void testUriHidesTheServerAndTheCredentials() throws Exception {
    GenericFileName name = parse("prod://tmp/file.txt");

    assertEquals("prod", name.getScheme());
    assertEquals("sftp.example.com", name.getHostName());
    assertEquals(2222, name.getPort());
    assertEquals("hop", name.getUserName());

    assertEquals("prod:///tmp/file.txt", name.getURI());
    assertEquals("prod:///tmp/file.txt", name.getFriendlyURI());
  }

  /** What comes out of a file object has to go back in unchanged. */
  @Test
  void testUriRoundTrips() throws Exception {
    GenericFileName name = parse("prod://tmp/hop/file.txt");
    GenericFileName reparsed = parse(name.getURI());

    assertEquals(name.getPath(), reparsed.getPath());
    assertEquals(name.getURI(), reparsed.getURI());
  }

  @Test
  void testChildNamesKeepTheServerAndTheCleanUri() throws Exception {
    GenericFileName folder = parse("prod://tmp");
    GenericFileName child = (GenericFileName) folder.createName("/tmp/file.txt", FileType.FILE);

    assertEquals("sftp.example.com", child.getHostName());
    assertEquals(2222, child.getPort());
    assertEquals("prod:///tmp/file.txt", child.getURI());
  }

  @Test
  void testTrailingSlashMeansFolder() throws Exception {
    assertEquals(FileType.FOLDER, parse("prod://tmp/hop/").getType());
    assertEquals(FileType.FILE, parse("prod://tmp/hop/file.txt").getType());
  }

  @Test
  void testAnotherSchemeIsRejected() {
    assertThrows(FileSystemException.class, () -> parse("other://tmp/file.txt"));
  }

  @Test
  void testConnectionWithoutServerIsRejected() {
    SftpConnection connection = new SftpConnection();
    connection.setName("prod");
    SftpConnectionFileNameParser noServer =
        new SftpConnectionFileNameParser(new Variables(), connection);
    assertThrows(
        FileSystemException.class, () -> noServer.parseUri(context, null, "prod://tmp/file.txt"));
  }

  @Test
  void testRelativeNamesResolveAgainstTheBase() throws Exception {
    GenericFileName base = parse("prod://tmp/hop/");
    GenericFileName resolved = (GenericFileName) parser.parseUri(context, base, "file.txt");
    assertEquals("/tmp/hop/file.txt", resolved.getPath());
  }

  private GenericFileName parse(String uri) throws FileSystemException {
    return (GenericFileName) parser.parseUri(context, null, uri);
  }
}
