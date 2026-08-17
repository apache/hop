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

import org.apache.commons.vfs2.AbstractProviderTestConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.NamingTests;
import org.apache.commons.vfs2.PermissionsTests;
import org.apache.commons.vfs2.ProviderDeleteTests;
import org.apache.commons.vfs2.ProviderReadTests;
import org.apache.commons.vfs2.ProviderRenameTests;
import org.apache.commons.vfs2.ProviderTestSuiteJunit5;
import org.apache.commons.vfs2.ProviderWriteTests;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;

/**
 * JUnit 5 tests for SFTP provider with closed exec channel.
 *
 * <p>This class replaces {@code SftpProviderClosedExecChannelTestCase} with a pure JUnit 5
 * implementation.
 */
public class SftpProviderClosedExecChannelTest extends ProviderTestSuiteJunit5 {

  /** Configuration for SFTP closed exec channel tests. */
  private static class SftpProviderClosedExecChannelTestConfig extends AbstractProviderTestConfig {

    @Override
    public FileObject getBaseTestFolder(final FileSystemManager manager) throws Exception {
      final String uri = System.getProperty("test.sftp.uri");
      if (uri == null) {
        return null;
      }
      final FileSystemOptions fileSystemOptions = new FileSystemOptions();
      final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
      builder.setStrictHostKeyChecking(fileSystemOptions, "no");
      builder.setUserInfo(fileSystemOptions, new TrustEveryoneUserInfo());
      builder.setSessionTimeoutMillis(fileSystemOptions, 10000);
      return manager.resolveFile(uri, fileSystemOptions);
    }

    public void prepare(final DefaultFileSystemManager manager) throws Exception {
      manager.addProvider("sftp", new SftpFileProvider());
    }
  }

  public SftpProviderClosedExecChannelTest() throws Exception {
    super(new SftpProviderClosedExecChannelTestConfig(), "", false);
  }

  @Override
  protected void addBaseTests() throws Exception {
    // Only add base tests if we have a real SFTP server configured
    if (System.getProperty("test.sftp.uri") != null) {
      addTests(ProviderReadTests.class);
      addTests(ProviderWriteTests.class);
      addTests(ProviderDeleteTests.class);
      addTests(ProviderRenameTests.class);
      addTests(NamingTests.class);
      // VFS-405: set/get permissions
      addTests(PermissionsTests.class);
    }
  }
}
