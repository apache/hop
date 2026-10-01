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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jcraft.jsch.TestIdentityRepositoryFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests SFTP password authentication using {@link StaticUserAuthenticator}.
 *
 * <p>Verifies that credentials supplied via {@link
 * DefaultFileSystemConfigBuilder#setUserAuthenticator} are correctly propagated to the SFTP server.
 */
public class SftpPasswordAuthTest {

  private static final String TEST_USERNAME = "testuser";

  private static final String TEST_PASSWORD = "testpass";

  private static SshServer sshServer;

  private static int serverPort;

  private static DefaultFileSystemManager manager;

  private static String baseUri() {
    return String.format("sftp://%s@localhost:%d", TEST_USERNAME, serverPort);
  }

  private static void configureSftpOptions(final FileSystemOptions opts)
      throws FileSystemException {
    final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
    builder.setStrictHostKeyChecking(opts, "no");
    builder.setUserInfo(opts, new TrustEveryoneUserInfo());
    builder.setIdentityRepositoryFactory(opts, new TestIdentityRepositoryFactory());
    builder.setConnectTimeout(opts, Duration.ofSeconds(60));
    builder.setSessionTimeout(opts, Duration.ofSeconds(60));
  }

  @BeforeAll
  static void setUp() throws Exception {
    sshServer = SshServer.setUpDefaultServer();
    sshServer.setPort(0);
    final Path tmpKeyFile = Files.createTempFile("sshd-test-key", ".ser");
    tmpKeyFile.toFile().deleteOnExit();
    final SimpleGeneratorHostKeyProvider keyProvider =
        new SimpleGeneratorHostKeyProvider(tmpKeyFile);
    keyProvider.setAlgorithm("RSA");
    sshServer.setKeyPairProvider(keyProvider);
    sshServer.setPasswordAuthenticator(
        (user, pass, session) -> TEST_USERNAME.equals(user) && TEST_PASSWORD.equals(pass));
    sshServer.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
    final File homeDir = getTestDirectoryFile();
    sshServer.setFileSystemFactory(new VirtualFileSystemFactory(homeDir.toPath().toAbsolutePath()));
    sshServer.start();
    serverPort = sshServer.getPort();
    manager = new DefaultFileSystemManager();
    manager.addProvider("sftp", new SftpFileProvider());
    manager.addProvider("file", new DefaultLocalFileProvider());
    manager.init();
  }

  private static void stopServerWithTimeout(final long timeoutMs) {
    final Thread stopThread = new Thread(() -> IOUtils.closeQuietly(sshServer), "sshd-stop");
    stopThread.setDaemon(true);
    stopThread.start();
    try {
      stopThread.join(timeoutMs);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (manager != null) {
      try {
        manager.close();
      } catch (final Exception e) {
        // ignore
      }
    }
    if (sshServer != null) {
      stopServerWithTimeout(5000);
    }
  }

  private FileSystemOptions authOptions() throws FileSystemException {
    final FileSystemOptions opts = new FileSystemOptions();
    final StaticUserAuthenticator auth =
        new StaticUserAuthenticator(null, TEST_USERNAME, TEST_PASSWORD);
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
    configureSftpOptions(opts);
    return opts;
  }

  @Test
  void testResolveFile() throws FileSystemException {
    final FileSystemOptions opts = authOptions();
    try (FileObject file = manager.resolveFile(baseUri() + "/read-tests/file1.txt", opts)) {
      assertNotNull(file);
      assertTrue(file.exists(), "file1.txt should exist");
      assertEquals(FileType.FILE, file.getType());
      assertNotNull(file.getContent(), "Content should be readable when credentials are correct");
    }
  }

  @Test
  void testResolveFolder() throws FileSystemException {
    final FileSystemOptions opts = authOptions();
    try (FileObject folder = manager.resolveFile(baseUri() + "/read-tests", opts)) {
      assertNotNull(folder);
      assertTrue(folder.exists(), "read-tests folder should exist");
    }
  }

  @Test
  void testResolveFolderWithTrailingSlash() throws FileSystemException {
    final FileSystemOptions opts = authOptions();
    try (FileObject folder = manager.resolveFile(baseUri() + "/read-tests/", opts)) {
      assertNotNull(folder);
      assertTrue(folder.exists(), "read-tests/ folder should exist");
    }
  }

  @Test
  void testWrongCredentialsThrowsException() throws FileSystemException {
    final FileSystemOptions opts = new FileSystemOptions();
    final StaticUserAuthenticator auth =
        new StaticUserAuthenticator(null, "wronguser", "wrongpassword");
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
    configureSftpOptions(opts);
    final String wrongUserUri =
        String.format("sftp://wronguser@localhost:%d/read-tests/file1.txt", serverPort);
    assertThrows(
        FileSystemException.class,
        () -> {
          try (FileObject file = manager.resolveFile(wrongUserUri, opts)) {
            file.exists();
          }
        },
        "Expected FileSystemException when accessing a resource with wrong credentials");
  }
}
