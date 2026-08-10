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

import com.jcraft.jsch.TestIdentityRepositoryFactory;
import java.time.Duration;
import org.apache.commons.vfs2.AbstractProviderTestConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.PermissionsTests;
import org.apache.commons.vfs2.ProviderTestSuiteJunit5;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.junit.jupiter.api.AfterAll;

/**
 * JUnit 5 tests for the SFTP file system provider.
 *
 * <p>This class replaces {@code SftpProviderTestCase} with a pure JUnit 5 implementation. Uses an
 * embedded Apache SSHd server for testing.
 */
public class SftpProviderTest extends ProviderTestSuiteJunit5 {

  /** Configuration for SFTP provider tests. */
  private static class SftpProviderTestConfig extends AbstractProviderTestConfig {

    @Override
    public FileObject getBaseTestFolder(final FileSystemManager manager) throws Exception {
      // Check for external server override
      String uri = System.getProperty("test.sftp.uri");
      if (uri == null) {
        // Start embedded server if not already running
        if (!SftpTestServerHelper.isServerRunning()) {
          SftpTestServerHelper.startServer();
        }
        uri = SftpTestServerHelper.getConnectionUri();
      }

      if (uri == null) {
        return null;
      }

      final FileSystemOptions fileSystemOptions = new FileSystemOptions();
      final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
      builder.setStrictHostKeyChecking(fileSystemOptions, "no");
      builder.setUserInfo(fileSystemOptions, new TrustEveryoneUserInfo());
      builder.setIdentityRepositoryFactory(fileSystemOptions, new TestIdentityRepositoryFactory());
      builder.setConnectTimeout(fileSystemOptions, Duration.ofSeconds(60));
      builder.setSessionTimeout(fileSystemOptions, Duration.ofSeconds(60));

      return manager.resolveFile(uri, fileSystemOptions);
    }

    @Override
    public void prepare(final DefaultFileSystemManager manager) throws Exception {
      manager.addProvider("sftp", new SftpFileProvider());
    }
  }

  /** Stops the embedded SFTP server after all tests. */
  @AfterAll
  public static void stopSftpServer() throws InterruptedException {
    // Only stop if we started it (not using external server)
    if (System.getProperty("test.sftp.uri") == null) {
      SftpTestServerHelper.stopServer();
    }
  }

  public SftpProviderTest() throws Exception {
    super(new SftpProviderTestConfig(), "", false);
  }

  @Override
  protected void addBaseTests() throws Exception {
    super.addBaseTests();
    // VFS-405: set/get permissions
    addTests(PermissionsTests.class);

    // SftpMultiThreadWriteTests is left out on purpose. It is one of Commons VFS master's tests
    // running against a provider forked from the 2.10.0 tag, and its 100-way parallel copy leans on
    // post-2.10.0 work we cannot take: VFS-862 propagates listing metadata to cached children in
    // place, and needs the resolveFileInternal(FileName) that only exists on the 2.11.0 base
    // classes. This is not the locking change in SftpFileObject - master with that change applied
    // passes this test 101/101. Add it back here when Hop moves to Commons VFS 2.11.0 and the fork
    // is re-based on that tag.
  }
}
