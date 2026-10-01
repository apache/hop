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
package org.apache.commons.vfs2;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

public class IPv6LocalConnectionTests extends AbstractProviderTestCase {

  private static final String LOOPBACK = "0:0:0:0:0:0:0:1";
  private static final Log log = LogFactory.getLog(IPv6LocalConnectionTests.class);

  private static List<String> getLocalIPv6Addresses() throws SocketException {
    final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
    final List<String> result = new ArrayList<>();
    for (final NetworkInterface networkInterface : Collections.list(networkInterfaces)) {
      if (!networkInterface.isUp()
          || networkInterface.isLoopback()
          // utun refers to VPN network interface, we don't expect this connection to work
          || networkInterface.getName().startsWith("utun")) {
        continue;
      }
      for (final InetAddress inetAddress : Collections.list(networkInterface.getInetAddresses())) {
        if (inetAddress instanceof Inet6Address
            && !inetAddress.isLoopbackAddress()
            && !inetAddress.isMulticastAddress()) {
          result.add(StringUtils.substringBefore(inetAddress.getHostAddress(), "%"));
        }
      }
    }
    // Add loopback as backstop, this is needed for some IPv6 setups on Windows.
    if (!result.contains(LOOPBACK)) {
      result.add(LOOPBACK);
    }
    return result;
  }

  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.URI, Capability.READ_CONTENT};
  }

  private FileSystemOptions setupConnectionTimeoutHints(final FileSystem fileSystem) {
    // Unfortunately there is no common way to set up timeouts for every protocol
    // So, we use this hacky approach to make this class generic and formally independent of
    // protocols implementations

    final FileSystemOptions result = (FileSystemOptions) fileSystem.getFileSystemOptions().clone();

    final Duration timeout = Duration.ofSeconds(5);

    result.setOption(
        fileSystem.getClass(),
        "org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder.CONNECT_TIMEOUT",
        timeout);
    result.setOption(
        fileSystem.getClass(),
        "org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder.TIMEOUT",
        timeout);

    result.setOption(fileSystem.getClass(), "http.connection.timeout", timeout);
    result.setOption(fileSystem.getClass(), "http.socket.timeout", timeout);

    // This actually doesn't affect FtpFileProvider now, but it looks like an issue
    // This would work, if FtpClientFactory call client.setConnectTimeout() with CONNECT_TIMEOUT
    // value
    result.setOption(
        fileSystem.getClass(),
        "org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder.CONNECT_TIMEOUT",
        timeout);
    result.setOption(
        fileSystem.getClass(),
        "org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder.SO_TIMEOUT",
        timeout);

    return result;
  }

  @Test
  public void testConnectIPv6UrlLocal() throws Exception {
    final List<String> localIPv6Addresses = getLocalIPv6Addresses();
    boolean connected = false;
    for (final String ipv6Address : localIPv6Addresses) {
      final String ipv6Url =
          Strings.CS.replace(
              getReadFolder().getURL().toString(), "localhost", "[" + ipv6Address + "]");
      try {
        final FileSystem fileSystem = getFileSystem();

        final FileObject readFolderObject =
            getManager().resolveFile(ipv6Url, setupConnectionTimeoutHints(fileSystem));
        connected =
            connected
                || readFolderObject.resolveFile("file1.txt").getContent().getByteArray() != null;
      } catch (final FileSystemException e) {
        // We don't care, if some of the discovered IPv6 addresses don't work.
        // We just need a single one to work for testing the functionality end-to-end.
        log.warn("Failed to connect to some of the local IPv6 network addresses", e);
      }
    }
    assertTrue(
        connected,
        "None of the discovered local IPv6 network addresses has responded for connection: "
            + localIPv6Addresses);
  }

  @Test
  public void testIPv6Connection() throws Throwable {
    final List<String> localIPv6Addresses = getLocalIPv6Addresses();
    if (localIPv6Addresses.isEmpty()) {
      log.info("Local machine must have IPv6 address to run this test");
      return;
    }
    // Test the IPv6 connection - actual test is in testConnectIPv6UrlLocal
  }
}
