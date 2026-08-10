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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import java.io.IOException;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.vfs2.AbstractProviderTestConfig;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.IPv6LocalConnectionTests;
import org.apache.commons.vfs2.ProviderTestSuiteJunit5;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.junit.jupiter.api.Test;

/**
 * JUnit 5 tests for SFTP provider with IPv6 support.
 *
 * <p>This class replaces {@code SftpProviderIPv6TestCase} with a pure JUnit 5 implementation.
 */
public class SftpProviderIPv6Test extends ProviderTestSuiteJunit5 {

  /** Mocked SFTP file provider for testing IPv6 without actual network connection. */
  private static class MockedClientSftpFileProvider extends SftpFileProvider {
    @Override
    protected FileSystem doCreateFileSystem(
        final FileName name, final FileSystemOptions fileSystemOptions) {
      final GenericFileName rootName = (GenericFileName) name;

      final com.jcraft.jsch.Session sessionMock = mock(com.jcraft.jsch.Session.class);
      final ChannelExec channelExecMock = mock(ChannelExec.class);

      when(sessionMock.isConnected()).thenReturn(true);

      try {
        when(sessionMock.openChannel(anyString())).thenReturn(channelExecMock);
      } catch (final JSchException e) {
        throw new AssertionError("Should never happen", e);
      }

      when(channelExecMock.isClosed()).thenReturn(true);

      try {
        when(channelExecMock.getInputStream()).thenReturn(new NullInputStream());
      } catch (final IOException e) {
        throw new AssertionError("Should never happen", e);
      }

      return new SftpFileSystem(rootName, sessionMock, fileSystemOptions);
    }
  }

  /** Configuration for SFTP IPv6 tests. */
  private static class SftpProviderIPv6TestConfig extends AbstractProviderTestConfig {

    @Override
    public FileObject getBaseTestFolder(final FileSystemManager manager) throws Exception {
      final String uri = System.getProperty("test.sftp.uri");
      return uri != null ? manager.resolveFile(uri) : null;
    }

    public void prepare(final DefaultFileSystemManager manager) throws Exception {
      manager.addProvider("sftp", new SftpFileProvider());
    }
  }

  public SftpProviderIPv6Test() throws Exception {
    super(new SftpProviderIPv6TestConfig(), "", false);
  }

  @Override
  protected void addBaseTests() throws Exception {
    // Only add base tests if we have a real SFTP server configured
    if (System.getProperty("test.sftp.uri") != null) {
      addTests(IPv6LocalConnectionTests.class);
    }
    // Otherwise, only the @Test methods in this class will run (testResolveIPv6Url)
  }

  /** Tests resolving an IPv6 URL. */
  @Test
  public void testResolveIPv6Url() throws FileSystemException {
    try {
      // We only want to use mocked client for this test, not for the test class initialization
      getManager().removeProvider("sftp");
      getManager().addProvider("sftp", new MockedClientSftpFileProvider());

      final String ipv6Url = "sftp://user:pass@[fe80::1c42:dae:8370:aea6%en1]/file.txt";
      final FileObject fileObject = getManager().resolveFile(ipv6Url, new FileSystemOptions());
      assertEquals(
          "sftp://user:pass@[fe80::1c42:dae:8370:aea6%en1]/",
          fileObject.getFileSystem().getRootURI());
      assertEquals(
          "sftp://user:pass@[fe80::1c42:dae:8370:aea6%en1]/file.txt",
          fileObject.getName().getURI());
    } finally {
      getManager().removeProvider("sftp");
      getManager().addProvider("sftp", new SftpFileProvider());
    }
  }
}
