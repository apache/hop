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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * FTPS, in both the explicit and the implicit flavour, against an embedded FTP server with a self
 * signed certificate.
 */
class FtpsClientFactoryTest {

  private static final String PAYLOAD = "over-tls";

  private final IVariables variables = new Variables();

  @BeforeAll
  static void initEncryption() throws Exception {
    // Resolving the password of a connection goes through the password encoder.
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  @DisplayName("Explicit FTPS upgrades the connection and transfers over TLS")
  void explicitFtps(@TempDir Path root) throws Exception {
    assertTransfers(root, FtpSecurityMode.FTPS_EXPLICIT);
  }

  @Test
  @DisplayName("Implicit FTPS is TLS from the first byte and transfers over TLS")
  void implicitFtps(@TempDir Path root) throws Exception {
    assertTransfers(root, FtpSecurityMode.FTPS_IMPLICIT);
  }

  @Test
  @DisplayName("A self signed certificate is refused while verification is on")
  void selfSignedCertificateIsRefusedByDefault(@TempDir Path root) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, FtpSecurityMode.FTPS_EXPLICIT)) {
      FtpConnection connection = connection(server, FtpSecurityMode.FTPS_EXPLICIT);
      connection.setVerifyServerCertificate(true);

      assertThrows(
          HopException.class,
          () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
    }
  }

  @Test
  @DisplayName("A server which demands a client certificate is satisfied by the configured one")
  void mutualTls(@TempDir Path root) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, FtpSecurityMode.FTPS_EXPLICIT, true)) {
      server.writeFile("mutual.txt", PAYLOAD);

      FtpConnection connection = connection(server, FtpSecurityMode.FTPS_EXPLICIT);
      connection.setClientCertificateFile(server.getKeyStore().toString());
      connection.setClientCertificatePassword(FtpTestServer.getKeyStorePassword());

      FTPClient client =
          FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
      try {
        ByteArrayOutputStream downloaded = new ByteArrayOutputStream();
        assertTrue(client.retrieveFile("mutual.txt", downloaded));
        assertEquals(PAYLOAD, downloaded.toString(StandardCharsets.UTF_8));
      } finally {
        FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
      }
    }
  }

  @Test
  @DisplayName("A keystore which can't be read names itself in the error")
  void anUnreadableKeystoreIsReported(@TempDir Path root) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, FtpSecurityMode.FTPS_EXPLICIT)) {
      FtpConnection connection = connection(server, FtpSecurityMode.FTPS_EXPLICIT);
      connection.setClientCertificateFile(root.resolve("not-a-keystore.p12").toString());

      HopException e =
          assertThrows(
              HopException.class,
              () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
      assertTrue(e.getMessage().contains("not-a-keystore.p12"));
    }
  }

  @Test
  @DisplayName("Each security mode has its traditional default port")
  void defaultPorts() {
    assertEquals(21, FtpSecurityMode.FTP.getDefaultPort());
    assertEquals(21, FtpSecurityMode.FTPS_EXPLICIT.getDefaultPort());
    assertEquals(990, FtpSecurityMode.FTPS_IMPLICIT.getDefaultPort());
  }

  private void assertTransfers(Path root, FtpSecurityMode mode) throws Exception {
    try (FtpTestServer server = FtpTestServer.start(root, mode)) {
      server.writeFile("secret.txt", PAYLOAD);

      FTPClient client =
          FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection(server, mode));
      try {
        assertInstanceOf(FTPSClient.class, client, mode + " must use an FTPS client");
        assertTrue(client.isConnected());

        ByteArrayOutputStream downloaded = new ByteArrayOutputStream();
        assertTrue(client.retrieveFile("secret.txt", downloaded));
        assertEquals(PAYLOAD, downloaded.toString(StandardCharsets.UTF_8));
      } finally {
        FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
      }
    }
  }

  private FtpConnection connection(FtpTestServer server, FtpSecurityMode mode) {
    FtpConnection connection = new FtpConnection();
    connection.setName("test-ftps");
    connection.setSecurityMode(mode);
    connection.setServerName(FtpTestServer.HOST);
    connection.setServerPort(Integer.toString(server.getPort()));
    connection.setUserName(FtpTestServer.USER);
    connection.setPassword(FtpTestServer.PASSWORD);
    // The embedded server signs its own certificate, which is the whole point of the option.
    connection.setVerifyServerCertificate(false);
    return connection;
  }
}
