/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.vfs.sftp.provider;

import static org.apache.commons.vfs2.VfsTestUtils.getTestDirectoryFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.TestIdentityRepositoryFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.apache.commons.vfs2.provider.local.GenericFileNameParser;
import org.apache.commons.vfs2.provider.local.LocalFileNameParser;
import org.apache.commons.vfs2.provider.local.WindowsFileNameParser;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.sftp.SftpConnectionFileNameParser;
import org.apache.hop.vfs.sftp.SftpConnectionFileProvider;
import org.apache.hop.vfs.sftp.SftpConnections;
import org.apache.hop.vfs.sftp.client.SftpClient;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystem;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A mainframe SFTP server (z/OS) distinguishes a USS path {@code /A.B.C} from an MVS data set
 * {@code //A.B.C}: the leading double slash is part of the name, not redundant punctuation. Commons
 * VFS collapses it, so such a data set can not be addressed from Hop at all.
 *
 * <p>The collapse happens in {@link org.apache.commons.vfs2.provider.UriParser#normalisePath} which
 * every {@link org.apache.commons.vfs2.provider.AbstractFileNameParser} calls: it walks the path
 * element by element and "axes" every empty element, which is exactly what a {@code //} is. The
 * local provider escapes this for Windows UNC names ({@code \\host\share}) because {@link
 * org.apache.commons.vfs2.provider.local.WindowsFileNameParser} pulls {@code //host/share} out of
 * the path and into the <em>root prefix</em> before normalisation runs. The SFTP name parser has no
 * such escape: {@link SftpFileNameParser} extends {@link
 * org.apache.commons.vfs2.provider.URLFileNameParser}, which puts only the host and the port in the
 * root, so the whole {@code //A.B.C} is handed to the normaliser as a path.
 *
 * <p>Hop keeps the second slash by hoisting it into the <em>root</em> of the file name, the same
 * escape the Windows parser uses for a UNC share, so the path itself stays an ordinary {@code
 * /A.B.C} and survives every child and every parent derived from it.
 *
 * @see <a href="https://www.ibm.com/docs/en/zos/latest?topic=ftp-mvs-data-sets">z/OS: naming MVS
 *     data sets</a>
 */
class SftpDoubleSlashPathTest {

  private static final String USER = "testuser";
  private static final String PASSWORD = "testpass";

  /** The data set the user is after, in the four spellings that were tried. */
  private static final String DATA_SET = "//EDIJP.NAB.CRERESP/AAMIPI";

  /** The scheme of the named connection pointing at the test server. */
  private static final String NAMED_CONNECTION = "mainframe";

  /** Raw remote paths as the SFTP server received them, oldest first. */
  private static final List<String> REQUESTED_PATHS =
      Collections.synchronizedList(new ArrayList<>());

  private static SshServer sshServer;
  private static int serverPort;
  private static DefaultFileSystemManager manager;

  @BeforeAll
  static void setUp() throws Exception {
    sshServer = SshServer.setUpDefaultServer();
    sshServer.setPort(0);
    final Path hostKey = Files.createTempFile("sshd-double-slash-key", ".ser");
    hostKey.toFile().deleteOnExit();
    final SimpleGeneratorHostKeyProvider keyProvider = new SimpleGeneratorHostKeyProvider(hostKey);
    keyProvider.setAlgorithm("RSA");
    sshServer.setKeyPairProvider(keyProvider);
    sshServer.setPasswordAuthenticator(
        (user, pass, session) -> USER.equals(user) && PASSWORD.equals(pass));
    sshServer.setSubsystemFactories(Collections.singletonList(new RecordingSftpSubsystemFactory()));
    final File homeDir = getTestDirectoryFile();
    sshServer.setFileSystemFactory(new VirtualFileSystemFactory(homeDir.toPath().toAbsolutePath()));
    sshServer.start();
    serverPort = sshServer.getPort();

    manager = new DefaultFileSystemManager();
    manager.addProvider("sftp", new SftpFileProvider());
    manager.addProvider("file", new DefaultLocalFileProvider());
    manager.addProvider(
        NAMED_CONNECTION, new SftpConnectionFileProvider(new Variables(), namedConnection()));
    manager.init();
  }

  /** A named connection to the test server; the password comes from {@link #authOptions()}. */
  private static SftpConnection namedConnection() {
    final SftpConnection connection = new SftpConnection();
    connection.setName(NAMED_CONNECTION);
    connection.setServerName("localhost");
    connection.setServerPort(Integer.toString(serverPort));
    connection.setUsername(USER);
    return connection;
  }

  @AfterAll
  static void tearDown() {
    if (manager != null) {
      manager.close();
    }
    if (sshServer != null) {
      final Thread stopper = new Thread(() -> IOUtils.closeQuietly(sshServer), "sshd-stop");
      stopper.setDaemon(true);
      stopper.start();
      try {
        stopper.join(5000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static String baseUri() {
    return String.format("sftp://%s@localhost:%d", USER, serverPort);
  }

  private static FileSystemOptions authOptions() throws FileSystemException {
    final FileSystemOptions opts = new FileSystemOptions();
    DefaultFileSystemConfigBuilder.getInstance()
        .setUserAuthenticator(opts, new StaticUserAuthenticator(null, USER, PASSWORD));
    final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
    builder.setStrictHostKeyChecking(opts, "no");
    builder.setUserInfo(opts, new TrustEveryoneUserInfo());
    builder.setIdentityRepositoryFactory(opts, new TestIdentityRepositoryFactory());
    builder.setConnectTimeout(opts, Duration.ofSeconds(60));
    builder.setSessionTimeout(opts, Duration.ofSeconds(60));
    return opts;
  }

  /** Resolve a name through the SFTP provider without connecting to anything. */
  private static FileName parse(String uri) throws FileSystemException {
    return manager.resolveURI(uri);
  }

  /** Ask the server for a file and return every raw path it was asked about. */
  private static List<String> pathsSentToServer(String uri) throws FileSystemException {
    REQUESTED_PATHS.clear();
    try (FileObject file = manager.resolveFile(uri, authOptions())) {
      file.exists();
    }
    return new ArrayList<>(REQUESTED_PATHS);
  }

  @Nested
  @DisplayName("An ordinary path is left alone")
  class OrdinaryPaths {

    /**
     * The path of a name is always an ordinary absolute path, the data set spelling included - the
     * second slash lives in the root, not here. Three slashes, a {@code %2F} and a {@code .}
     * segment all mean the plain USS path.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(
        strings = {
          "//EDIJP.NAB.CRERESP/AAMIPI",
          "///EDIJP.NAB.CRERESP/AAMIPI",
          "/\\/\\/EDIJP.NAB.CRERESP/AAMIPI",
          "/%2F%2FEDIJP.NAB.CRERESP/AAMIPI",
          "/./EDIJP.NAB.CRERESP/AAMIPI"
        })
    void thePathIsAlwaysAnOrdinaryAbsolutePath(String path) throws Exception {
      assertEquals("/EDIJP.NAB.CRERESP/AAMIPI", parse(baseUri() + path).getPath());
    }

    /** The relative spelling does not even parse. */
    @Test
    void parentOfTheRootIsRejected() {
      final FileSystemException e =
          assertThrows(
              FileSystemException.class, () -> parse(baseUri() + "/../EDIJP.NAB.CRERESP/AAMIPI"));
      assertTrue(
          e.getMessage().contains("Invalid relative file name"),
          "unexpected message: " + e.getMessage());
    }

    /** A named SFTP connection reads the same way. */
    @Test
    void namedConnectionReadsTheSameWay() throws Exception {
      assertEquals("/EDIJP.NAB.CRERESP/AAMIPI", parseNamedConnection("prod:" + DATA_SET));
    }

    /** One slash on the way in is one slash on the wire: nothing gained, nothing lost. */
    @Test
    void theServerIsAskedForTheSingleSlashPath() throws Exception {
      final List<String> paths = pathsSentToServer(baseUri() + "/read-tests/file1.txt");

      assertTrue(paths.contains("/read-tests/file1.txt"), "paths asked for: " + paths);
      assertTrue(
          paths.stream().noneMatch(p -> p.startsWith("//")),
          "no request should carry a double slash, but got: " + paths);
    }
  }

  /**
   * A path rooted at a second slash keeps it, all the way to the wire. The slash rides in the root
   * of the file name rather than in its path - see {@link IRootPrefixFileName}.
   */
  @Nested
  @DisplayName("Addressing a z/OS data set")
  class MainframeDataSets {

    /** The whole point: the server is asked for the data set, not for the USS path. */
    @Test
    void theServerIsAskedForTheDataSet() throws Exception {
      final List<String> paths = pathsSentToServer(baseUri() + "//read-tests/file1.txt");

      assertTrue(
          paths.contains("//read-tests/file1.txt"),
          "the server should have been asked for a //-path, but got: " + paths);
    }

    /**
     * The SFTP Put transform builds its target out of a named connection and a remote folder, so
     * the data set has to make it through {@link SftpConnections#buildUri} as well.
     */
    @Test
    void aRemoteFolderOfANamedConnectionReachesTheDataSet() throws Exception {
      final String uri = SftpConnections.buildUri(namedConnection(), "//read-tests", "file1.txt");
      final List<String> paths = pathsSentToServer(uri);

      assertTrue(
          paths.contains("//read-tests/file1.txt"),
          "the server should have been asked for a //-path, but got: " + paths);
    }

    /**
     * A data set name of a single level, the remote folder of the SFTP Put transform for instance.
     * jsch would turn {@code //read-tests} into {@code /read-tests}; the trailing slash keeps both.
     */
    @Test
    void aSingleLevelDataSetKeepsTheSecondSlash() throws Exception {
      final String uri = SftpConnections.buildUri(namedConnection(), "//read-tests/", null);
      REQUESTED_PATHS.clear();
      try (FileObject folder = manager.resolveFile(uri, authOptions())) {
        assertEquals(FileType.FOLDER, folder.getType());
      }
      final List<String> paths = new ArrayList<>(REQUESTED_PATHS);

      assertTrue(
          paths.contains("//read-tests/"),
          "the server should have been asked for the data set, but got: " + paths);
      assertFalse(
          paths.contains("/read-tests"),
          "the server was asked for the single-slash path: " + paths);
    }

    /** A random-access read opens the same file a plain read does: the data set. */
    @Test
    void aRandomAccessReadAsksForTheDataSet() throws Exception {
      final String expected =
          Files.readString(
              new File(getTestDirectoryFile(), "read-tests/file1.txt").toPath(),
              StandardCharsets.UTF_8);

      REQUESTED_PATHS.clear();
      final String content;
      try (FileObject file =
              manager.resolveFile(baseUri() + "//read-tests/file1.txt", authOptions());
          RandomAccessContent randomAccess =
              file.getContent().getRandomAccessContent(RandomAccessMode.READ);
          InputStream inputStream = randomAccess.getInputStream()) {
        content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
      }
      final List<String> paths = new ArrayList<>(REQUESTED_PATHS);

      assertEquals(expected, content);
      assertFalse(
          paths.contains("/read-tests/file1.txt"),
          "the random-access read went to the single-slash path: " + paths);
    }

    /** One slash is still one slash: an ordinary path is untouched by any of this. */
    @Test
    void aSingleSlashIsStillAUssPath() throws Exception {
      final List<String> paths = pathsSentToServer(baseUri() + "/read-tests/file1.txt");

      assertTrue(
          paths.stream().noneMatch(p -> p.startsWith("//")),
          "an ordinary path should not have gained a slash, but got: " + paths);
    }

    /** Three or more mean the same as one, as POSIX says they do. */
    @Test
    void threeSlashesCollapseToOne() throws Exception {
      final List<String> paths = pathsSentToServer(baseUri() + "///read-tests/file1.txt");

      assertTrue(
          paths.stream().noneMatch(p -> p.startsWith("//")),
          "three slashes should have collapsed, but got: " + paths);
    }

    /** The URI reads back exactly as it was typed, so it survives a save and a reload. */
    @Test
    void theUriKeepsTheSecondSlash() throws Exception {
      final FileName name = parse(baseUri() + "//EDIJP.NAB.CRERESP/AAMIPI");

      assertEquals(baseUri() + "//EDIJP.NAB.CRERESP/AAMIPI", name.getURI());
      assertEquals("/EDIJP.NAB.CRERESP/AAMIPI", name.getPath());
      assertEquals(baseUri() + "//", name.getRootURI());
    }

    /** And it is a different file from the USS path of the same name. */
    @Test
    void aDataSetIsNotTheUssPath() throws Exception {
      assertNotEquals(
          parse(baseUri() + "/EDIJP.NAB.CRERESP").getURI(),
          parse(baseUri() + "//EDIJP.NAB.CRERESP").getURI());
    }

    /**
     * The prefix has to survive being derived from, which is the whole reason it lives in the root.
     */
    @Test
    void childrenAndParentsKeepTheSecondSlash() throws Exception {
      final FileName name = parse(baseUri() + "//EDIJP.NAB.CRERESP/AAMIPI");

      assertEquals(baseUri() + "//EDIJP.NAB.CRERESP", name.getParent().getURI());
      assertEquals(
          baseUri() + "//EDIJP.NAB.CRERESP/AAMIPI/SUB",
          manager.resolveName(name, "SUB", NameScope.CHILD).getURI());
    }
  }

  /**
   * The workflow actions talk to jsch through {@link SftpClient} rather than through VFS, with the
   * folder as the user typed it, so the second slash of a single-level data set name needs the same
   * protection there.
   */
  @Nested
  @DisplayName("A data set through the SftpClient of the workflow actions")
  class SftpClientDataSets {

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
      "//A.B.C, //A.B.C/",
      "//A.B.C/, //A.B.C/",
      "//A.B.C/MEMBER, //A.B.C/MEMBER",
      "/A.B.C, /A.B.C",
      "///A.B.C, ///A.B.C",
      "//, //",
      "A.B.C, A.B.C"
    })
    void onlyASingleLevelDataSetGetsATrailingSlash(String path, String expected) {
      assertEquals(expected, SftpFileNameParser.keepDataSetPrefix(path));
    }

    @Test
    void folderExistsAsksForTheDataSet() throws Exception {
      final List<String> paths =
          withClient(client -> assertTrue(client.folderExists("//read-tests")));

      assertAskedForTheDataSet(paths, "read-tests");
    }

    @Test
    void getFileTypeAsksForTheDataSet() throws Exception {
      final List<String> paths =
          withClient(client -> assertEquals(FileType.FOLDER, client.getFileType("//read-tests")));

      assertAskedForTheDataSet(paths, "read-tests");
    }

    /**
     * A cd asks for the real path first and then looks at whatever the server answered, here the
     * single-slash path of the test server: only the first request is ours.
     */
    @Test
    void chdirAsksForTheDataSet() throws Exception {
      final List<String> paths = withClient(client -> client.chdir("//read-tests"));

      assertFalse(paths.isEmpty(), "the server wasn't asked anything");
      assertEquals("//read-tests/", paths.get(0), "paths asked for: " + paths);
    }

    /**
     * The existence check needs the trailing slash, the mkdir - which jsch sends as is - doesn't.
     */
    @Test
    void createFolderAsksForTheDataSet() throws Exception {
      final File created = new File(getTestDirectoryFile(), "new-data-set");
      try {
        final List<String> paths = withClient(client -> client.createFolder("//new-data-set"));

        assertTrue(created.isDirectory(), "the folder should have been created");
        assertAskedForTheDataSet(paths, "new-data-set");
        assertTrue(paths.contains("//new-data-set"), "no mkdir of the data set in: " + paths);
      } finally {
        Files.deleteIfExists(created.toPath());
      }
    }

    /** The same goes for creating a folder through VFS. */
    @Test
    void aVfsFolderIsCreatedWithoutTheTrailingSlash() throws Exception {
      final File created = new File(getTestDirectoryFile(), "new-vfs-data-set");
      try (FileObject folder =
          manager.resolveFile(
              SftpConnections.buildUri(namedConnection(), "//new-vfs-data-set", null),
              authOptions())) {
        REQUESTED_PATHS.clear();
        folder.createFolder();
        final List<String> paths = new ArrayList<>(REQUESTED_PATHS);

        assertTrue(created.isDirectory(), "the folder should have been created");
        assertTrue(paths.contains("//new-vfs-data-set"), "no mkdir of the data set in: " + paths);
      } finally {
        Files.deleteIfExists(created.toPath());
      }
    }

    private void assertAskedForTheDataSet(List<String> paths, String name) {
      assertTrue(
          paths.contains("//" + name + "/"),
          "the server should have been asked for the data set, but got: " + paths);
      assertFalse(
          paths.contains("/" + name), "the server was asked for the single-slash path: " + paths);
    }

    /** Log in to the test server, run the action, and return the raw paths the server saw. */
    private List<String> withClient(ClientAction action) throws Exception {
      final SftpClient client =
          new SftpClient(InetAddress.getByName("localhost"), serverPort, USER);
      try {
        client.login(PASSWORD);
        REQUESTED_PATHS.clear();
        action.run(client);
        return new ArrayList<>(REQUESTED_PATHS);
      } finally {
        client.disconnect();
      }
    }
  }

  @FunctionalInterface
  private interface ClientAction {
    void run(SftpClient client) throws Exception;
  }

  /**
   * Windows UNC names ({@code \\server\share\...}) are the one place where Commons VFS keeps a
   * leading double slash, and the way it does that is the way out for the mainframe as well: the
   * Windows name parser hoists {@code //server/share} into the <em>root prefix</em> before {@link
   * org.apache.commons.vfs2.provider.UriParser#normalisePath} ever sees it. Take that hoist away
   * and UNC breaks exactly like the SFTP data set does — which is what the generic (non-Windows)
   * parser below shows.
   */
  @Nested
  @DisplayName("Why UNC survives and a data set does not")
  class UncKeepsItsDoubleSlash {

    private static final String UNC = "file:////server/share/dir/file.txt";

    /** Hoisted into the root, so it survives, and the URI round-trips with all four slashes. */
    @Test
    void windowsParserHoistsTheShareIntoTheRoot() throws Exception {
      final FileName name = parseLocal(new WindowsFileNameParser(), UNC);

      assertEquals("file:////server/share/", name.getRootURI());
      assertEquals("/dir/file.txt", name.getPath());
      assertEquals(UNC, name.getURI());
    }

    /** No hoist, so the slashes end up in the path and the normaliser axes them. */
    @Test
    void genericParserCollapsesTheShareLikeTheDataSet() throws Exception {
      final FileName name = parseLocal(new GenericFileNameParser(), UNC);

      assertEquals("file:///", name.getRootURI());
      assertEquals("/server/share/dir/file.txt", name.getPath());
      assertEquals("file:///server/share/dir/file.txt", name.getURI());
    }

    private FileName parseLocal(LocalFileNameParser parser, String uri) throws FileSystemException {
      final FileSystemManager fsm = mock(FileSystemManager.class);
      when(fsm.getSchemes()).thenReturn(new String[] {"file"});
      final VfsComponentContext context = mock(VfsComponentContext.class);
      when(context.getFileSystemManager()).thenReturn(fsm);

      return parser.parseUri(context, null, uri);
    }
  }

  /** Parse a {@code <connection>:<path>} URI and return the path it resolves to. */
  private static String parseNamedConnection(String uri) throws FileSystemException {
    final SftpConnection connection = new SftpConnection();
    connection.setName("prod");
    connection.setServerName("mainframe.example.com");
    connection.setServerPort("22");
    connection.setUsername(USER);

    final IVariables variables = new Variables();
    final FileSystemManager fsm = mock(FileSystemManager.class);
    when(fsm.getSchemes()).thenReturn(new String[] {"prod"});
    final VfsComponentContext context = mock(VfsComponentContext.class);
    when(context.getFileSystemManager()).thenReturn(fsm);

    return new SftpConnectionFileNameParser(variables, connection)
        .parseUri(context, null, uri)
        .getPath();
  }

  /** An SFTP subsystem that records the raw remote paths the client asks about. */
  private static class RecordingSftpSubsystemFactory extends SftpSubsystemFactory {

    @Override
    public Command createSubsystem(final ChannelSession channel) throws IOException {
      return new SftpSubsystem(channel, this) {
        @Override
        protected Path resolveFile(final String remotePath)
            throws IOException, InvalidPathException {
          REQUESTED_PATHS.add(remotePath);
          return super.resolveFile(remotePath);
        }
      };
    }
  }
}
