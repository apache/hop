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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Everything the connection says has to end up in the options the SFTP provider connects with. */
class SftpConnectionOptionsTest {

  private static final SftpFileSystemConfigBuilder CONFIG =
      SftpFileSystemConfigBuilder.getInstance();

  private IVariables variables;
  private SftpConnection connection;

  @TempDir private Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    // Resolving passwords and reading the private key through VFS both need the client environment.
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    variables = new Variables();
    connection = new SftpConnection();
    connection.setName("prod");
    connection.setServerName("sftp.example.com");
  }

  @Test
  void testSessionSettings() throws Exception {
    connection.setUserDirIsRoot(true);
    connection.setStrictHostKeyChecking("yes");
    connection.setCompression("zlib");
    connection.setPreferredAuthentications("publickey,password");
    connection.setKeyExchangeAlgorithm("diffie-hellman-group14-sha256");
    connection.setFileNameEncoding("ISO-8859-1");
    connection.setLoadOpenSshConfig(true);
    connection.setDisableDetectExecChannel(true);
    connection.setConnectionTimeout("5000");
    connection.setSessionTimeout("60000");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertTrue(CONFIG.getUserDirIsRoot(options));
    assertEquals("yes", CONFIG.getStrictHostKeyChecking(options));
    assertEquals("zlib", CONFIG.getCompression(options));
    assertEquals("publickey,password", CONFIG.getPreferredAuthentications(options));
    assertEquals("diffie-hellman-group14-sha256", CONFIG.getKeyExchangeAlgorithm(options));
    assertEquals("ISO-8859-1", CONFIG.getFileNameEncoding(options));
    assertTrue(CONFIG.isLoadOpenSSHConfig(options));
    assertTrue(CONFIG.isDisableDetectExecChannel(options));
    assertEquals(5000, CONFIG.getConnectTimeoutMillis(options));
    assertEquals(60000, CONFIG.getSessionTimeoutMillis(options));
  }

  /** Empty settings stay empty: jsch's own defaults are better than ours. */
  @Test
  void testEmptySettingsAreNotApplied() throws Exception {
    connection.setCompression("");
    connection.setStrictHostKeyChecking("");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertNull(CONFIG.getCompression(options));
    assertEquals("no", CONFIG.getStrictHostKeyChecking(options));
    assertNull(CONFIG.getPreferredAuthentications(options));
    assertNull(CONFIG.getKeyExchangeAlgorithm(options));
    assertNull(CONFIG.getFileNameEncoding(options));
    // commons-vfs reports a timeout which was never set as zero, jsch's "wait forever".
    assertEquals(0, CONFIG.getConnectTimeoutMillis(options));
    assertEquals(0, CONFIG.getSessionTimeoutMillis(options));
    assertNull(CONFIG.getProxyType(options));
  }

  @Test
  void testVariablesAreResolved() throws Exception {
    variables.setVariable("SFTP_COMPRESSION", "zlib");
    variables.setVariable("SFTP_TIMEOUT", "1234");
    connection.setCompression("${SFTP_COMPRESSION}");
    connection.setSessionTimeout("${SFTP_TIMEOUT}");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertEquals("zlib", CONFIG.getCompression(options));
    assertEquals(1234, CONFIG.getSessionTimeoutMillis(options));
  }

  @Test
  void testKnownHostsFile() throws Exception {
    File knownHosts = Files.createFile(tempDir.resolve("known_hosts")).toFile();
    connection.setKnownHostsFile(knownHosts.getAbsolutePath());

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertEquals(knownHosts.getAbsolutePath(), CONFIG.getKnownHosts(options).getAbsolutePath());
  }

  /** The key is read rather than handed to jsch as a file: it may live anywhere VFS reaches. */
  @Test
  void testPrivateKeyIsReadThroughVfs() throws Exception {
    Path keyFile = Files.writeString(tempDir.resolve("id_rsa"), "-----BEGIN PRIVATE KEY-----");
    connection.setUseKeyFile(true);
    connection.setKeyFilename(keyFile.toAbsolutePath().toString());
    connection.setKeyPassphrase("phrase");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertEquals(1, CONFIG.getIdentityProvider(options).length);
  }

  @Test
  void testMissingPrivateKeyIsReported() {
    connection.setUseKeyFile(true);
    connection.setKeyFilename(tempDir.resolve("absent").toAbsolutePath().toString());

    assertThrows(
        FileSystemException.class, () -> SftpConnectionOptions.build(variables, connection, null));
  }

  @Test
  void testHttpProxy() throws Exception {
    connection.setProxyType("HTTP");
    connection.setProxyHost("proxy.example.com");
    connection.setProxyPort("8080");
    connection.setProxyUsername("proxy-user");
    connection.setProxyPassword("proxy-secret");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertEquals(SftpFileSystemConfigBuilder.PROXY_HTTP, CONFIG.getProxyType(options));
    assertEquals("proxy.example.com", CONFIG.getProxyHost(options));
    assertEquals(8080, CONFIG.getProxyPort(options));
    assertEquals("proxy-user", CONFIG.getProxyUser(options));
    assertEquals("proxy-secret", CONFIG.getProxyPassword(options));
  }

  @Test
  void testSocks5Proxy() throws Exception {
    connection.setProxyType("SOCKS5");
    connection.setProxyHost("proxy.example.com");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertEquals(SftpFileSystemConfigBuilder.PROXY_SOCKS5, CONFIG.getProxyType(options));
  }

  @Test
  void testStreamProxyCarriesItsCommand() throws Exception {
    connection.setProxyType("STREAM");
    connection.setProxyHost("jump.example.com");
    connection.setProxyCommand("nc %h %p");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertEquals(SftpFileSystemConfigBuilder.PROXY_STREAM, CONFIG.getProxyType(options));
    assertEquals("nc %h %p", CONFIG.getProxyCommand(options));
  }

  /** A proxy host without a type would silently connect straight to the server. */
  @Test
  void testUnknownProxyTypeIsReported() {
    connection.setProxyHost("proxy.example.com");
    connection.setProxyType("SOCKS4");

    assertThrows(
        FileSystemException.class, () -> SftpConnectionOptions.build(variables, connection, null));
  }

  /** Without a proxy host there's no proxy, whatever else is filled in. */
  @Test
  void testNoProxyHostMeansNoProxy() throws Exception {
    connection.setProxyType("HTTP");

    FileSystemOptions options = SftpConnectionOptions.build(variables, connection, null);

    assertNull(CONFIG.getProxyType(options));
  }
}
