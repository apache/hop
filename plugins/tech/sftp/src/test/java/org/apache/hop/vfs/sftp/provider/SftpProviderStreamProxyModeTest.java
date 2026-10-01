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
import java.net.URI;
import org.apache.commons.vfs2.AbstractProviderTestConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.PermissionsTests;
import org.apache.commons.vfs2.ProviderReadTests;
import org.apache.commons.vfs2.ProviderTestSuiteJunit5;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;

/**
 * JUnit 5 tests for SFTP provider with stream proxy mode.
 *
 * <p>This class replaces {@code SftpProviderStreamProxyModeTestCase} with a pure JUnit 5
 * implementation.
 *
 * <p>VFS-440: stream proxy test suite. We override the addBaseTests method so that only one test is
 * run (we just test that the input/output are correctly forwarded, and hence if the reading test
 * succeeds/fails the other will also succeed/fail).
 */
public class SftpProviderStreamProxyModeTest extends ProviderTestSuiteJunit5 {

  /** Configuration for SFTP stream proxy mode tests. */
  private static class SftpProviderStreamProxyModeTestConfig extends AbstractProviderTestConfig {

    @Override
    public FileObject getBaseTestFolder(final FileSystemManager manager) throws Exception {
      String uri = System.getProperty("test.sftp.uri");
      if (uri == null) {
        return null;
      }

      final FileSystemOptions fileSystemOptions = new FileSystemOptions();
      final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
      builder.setStrictHostKeyChecking(fileSystemOptions, "no");
      builder.setUserInfo(fileSystemOptions, new TrustEveryoneUserInfo());
      builder.setIdentityRepositoryFactory(fileSystemOptions, new TestIdentityRepositoryFactory());

      final FileSystemOptions proxyOptions = (FileSystemOptions) fileSystemOptions.clone();

      final URI parsedURI = new URI(uri);
      final String userInfo = parsedURI.getUserInfo();
      final String[] userFields = userInfo == null ? null : userInfo.split(":", 2);

      builder.setProxyType(fileSystemOptions, SftpFileSystemConfigBuilder.PROXY_STREAM);
      if (userFields != null) {
        if (userFields.length > 0) {
          builder.setProxyUser(fileSystemOptions, userFields[0]);
        }
        if (userFields.length > 1) {
          builder.setProxyPassword(fileSystemOptions, userFields[1]);
        }
      }
      builder.setProxyHost(fileSystemOptions, parsedURI.getHost());
      builder.setProxyPort(fileSystemOptions, parsedURI.getPort());
      builder.setProxyCommand(fileSystemOptions, SftpStreamProxy.NETCAT_COMMAND);
      builder.setProxyOptions(fileSystemOptions, proxyOptions);
      builder.setProxyPassword(fileSystemOptions, parsedURI.getAuthority());

      // Set up the new URI
      if (userInfo == null) {
        uri = String.format("sftp://localhost:%d", parsedURI.getPort());
      } else {
        uri = String.format("sftp://%s@localhost:%d", userInfo, parsedURI.getPort());
      }

      return manager.resolveFile(uri, fileSystemOptions);
    }

    public void prepare(final DefaultFileSystemManager manager) throws Exception {
      manager.addProvider("sftp", new SftpFileProvider());
    }
  }

  public SftpProviderStreamProxyModeTest() throws Exception {
    super(new SftpProviderStreamProxyModeTestConfig(), "", false);
  }

  @Override
  protected void addBaseTests() throws Exception {
    // Only add base tests if we have a real SFTP server configured
    if (System.getProperty("test.sftp.uri") != null) {
      // Just tries to read
      addTests(ProviderReadTests.class);
      // VFS-405: set/get permissions
      addTests(PermissionsTests.class);
    }
  }
}
