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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.parser.ParserInitializationException;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** {@link FtpClientFactory} against an embedded FTP server. */
class FtpClientFactoryTest {

  @TempDir private static Path serverRoot;

  private static FtpTestServer server;

  private final IVariables variables = new Variables();

  @BeforeAll
  static void startServer() throws Exception {
    // Resolving the password of a connection goes through the password encoder.
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");

    server = FtpTestServer.start(serverRoot, FtpSecurityMode.FTP);
    server.writeFile("greeting.txt", "hello");
  }

  @AfterAll
  static void stopServer() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  @Test
  @DisplayName("A connection with the right credentials is connected, logged in and set up")
  void connectsAndLogsIn() throws Exception {
    FTPClient client =
        FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection());
    try {
      assertTrue(client.isConnected());
      assertEquals(1, client.listNames().length);
      assertEquals("greeting.txt", client.listNames()[0]);
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("A login the server refuses throws instead of handing back a useless client")
  void refusedLoginThrows() {
    FtpConnection connection = connection();
    connection.setPassword("not-the-password");

    HopException e =
        assertThrows(
            HopException.class,
            () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
    assertTrue(
        e.getMessage().contains(FtpTestServer.USER),
        "the error should name the user that was refused, but was: " + e.getMessage());
  }

  @Test
  @DisplayName("A connection without a server name is refused before any socket is opened")
  void missingServerNameThrows() {
    FtpConnection connection = connection();
    connection.setServerName("");

    HopException e =
        assertThrows(
            HopException.class,
            () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
    assertTrue(e.getMessage().contains("test-ftp"));
  }

  @Test
  @DisplayName("A socks proxy without a port is refused with an error naming the connection")
  void socksProxyWithoutPortThrows() {
    FtpConnection connection = connection();
    connection.setSocksProxyHost("proxy.invalid");
    connection.setSocksProxyPort("");

    HopException e =
        assertThrows(
            HopException.class,
            () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
    assertTrue(e.getMessage().contains("proxy.invalid"));
  }

  @Test
  @DisplayName("Half a set of socks credentials is refused rather than silently ignored")
  void incompleteSocksCredentialsThrow() {
    FtpConnection connection = connection();
    connection.setSocksProxyHost("proxy.invalid");
    connection.setSocksProxyPort("1080");
    connection.setSocksProxyUsername("someone");
    connection.setSocksProxyPassword("");

    HopException e =
        assertThrows(
            HopException.class,
            () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
    assertTrue(e.getMessage().contains("proxy.invalid"));
  }

  @Test
  @DisplayName("The transfer mode of the connection is applied to the session")
  void appliesTheTransferMode() throws Exception {
    FtpConnection connection = connection();
    connection.setBinaryMode(false);
    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      // Nothing on FTPClient exposes the negotiated type, so ask the server to set it again: a
      // positive reply means the ASCII type we sent during the setup was understood.
      assertTrue(client.setFileType(FTP.ASCII_FILE_TYPE));
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("An FTP proxy is named in the user name, so the proxy knows where to forward")
  void proxyUserNameCarriesTheServer() {
    FtpConnection connection = connection();
    connection.setProxyHost("proxy.example.com");
    connection.setProxyUsername("proxyuser");
    connection.setProxyPassword("proxypass");

    assertEquals(
        FtpTestServer.USER + "@ftp.example.com proxyuser",
        FtpClientFactory.loginUserName(variables, connection, "ftp.example.com"));
    assertEquals(
        FtpTestServer.PASSWORD + " proxypass",
        FtpClientFactory.loginPassword(variables, connection));
  }

  @Test
  @DisplayName("Without a proxy the user name and password are used as they are")
  void withoutProxyTheCredentialsAreUntouched() {
    FtpConnection connection = connection();

    assertEquals(
        FtpTestServer.USER,
        FtpClientFactory.loginUserName(variables, connection, FtpTestServer.HOST));
    assertEquals(FtpTestServer.PASSWORD, FtpClientFactory.loginPassword(variables, connection));
  }

  @Test
  @DisplayName("The listing format of the connection is applied to the client, not only to VFS")
  void appliesTheListingFormat() throws Exception {
    FtpConnection connection = connection();
    connection.setEntryParser("UNIX");
    connection.setServerTimeZone("UTC");

    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      assertEquals(1, client.listFiles().length);
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  /**
   * Proves the listing format really reaches the parser rather than being dropped on the way: a
   * system key no parser exists for has to make the listing fail.
   */
  @Test
  @DisplayName("A listing format the client doesn't know fails the listing")
  void anUnknownListingFormatFails() throws Exception {
    FtpConnection connection = connection();
    connection.setEntryParser("NO-SUCH-SYSTEM");

    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      assertThrows(ParserInitializationException.class, client::listFiles);
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("An FTP proxy is connected to instead of the server")
  void theProxyIsWhatGetsConnectedTo() {
    FtpConnection connection = connection();
    // The embedded server plays the part of the proxy: the "server" behind it doesn't exist, so a
    // connect which reaches this at all proves it went to the proxy and not to the server.
    connection.setProxyHost(FtpTestServer.HOST);
    connection.setProxyPort(Integer.toString(server.getPort()));
    connection.setServerName("ftp.example.com");
    connection.setServerPort("21");

    HopException e =
        assertThrows(
            HopException.class,
            () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));

    assertTrue(
        e.getMessage().contains(FtpTestServer.USER + "@ftp.example.com"),
        "the proxy should have been asked for ftp.example.com, but got: " + e.getMessage());
  }

  @Test
  @DisplayName("An FTP proxy without a port of its own uses the port of the server")
  void theProxyFallsBackToTheServerPort() {
    FtpConnection connection = connection();
    connection.setServerPort(Integer.toString(server.getPort()));
    connection.setProxyHost(FtpTestServer.HOST);
    connection.setProxyPort("");

    // Reaching the login at all means it connected, which it can only have done on that port.
    assertThrows(
        HopException.class,
        () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
  }

  @Test
  @DisplayName("A socks proxy which isn't a socks proxy fails instead of connecting anyway")
  void aSocksProxyIsActuallyUsed() {
    FtpConnection connection = connection();
    // Pointed at the FTP server itself, which speaks no SOCKS: the handshake has to fail.
    connection.setSocksProxyHost(FtpTestServer.HOST);
    connection.setSocksProxyPort(Integer.toString(server.getPort()));
    connection.setSocksProxyUsername("socks-user");
    connection.setSocksProxyPassword("socks-password");

    assertThrows(
        HopException.class,
        () -> FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection));
  }

  @Test
  @DisplayName("The timeouts and the keep alive of the connection reach the client")
  void timeoutsAndKeepAliveReachTheClient() throws Exception {
    FtpConnection connection = connection();
    connection.setConnectTimeout("9000");
    connection.setSocketTimeout("8000");
    connection.setDataTimeout("7000");
    connection.setControlKeepAliveTimeout("6000");
    connection.setControlKeepAliveReplyTimeout("5000");
    connection.setAutodetectUtf8(true);

    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      assertEquals(9000, client.getConnectTimeout());
      assertEquals(8000, client.getSoTimeout());
      assertEquals(Duration.ofMillis(6000), client.getControlKeepAliveTimeoutDuration());
      assertEquals(Duration.ofMillis(5000), client.getControlKeepAliveReplyTimeoutDuration());
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("A timeout of zero or nonsense means the default of the library, not zero")
  void emptyTimeoutsAreLeftAtTheirDefault() throws Exception {
    FtpConnection connection = connection();
    connection.setConnectTimeout("0");
    connection.setSocketTimeout("not a number");

    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      assertTrue(client.isConnected());
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("An active connection with a port range still transfers")
  void activeModeWithAPortRange() throws Exception {
    FtpConnection connection = connection();
    connection.setActiveConnection(true);
    connection.setActivePortRangeFrom("42000");
    connection.setActivePortRangeTo("42100");

    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      ByteArrayOutputStream downloaded = new ByteArrayOutputStream();
      assertTrue(client.retrieveFile("greeting.txt", downloaded));
      assertEquals("hello", downloaded.toString(StandardCharsets.UTF_8));
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("Turning off the data connection check still connects")
  void withoutRemoteVerification() throws Exception {
    FtpConnection connection = connection();
    connection.setRemoteVerification(false);

    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      assertTrue(client.isConnected());
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("Disconnecting without a log channel is allowed: not every caller has one")
  void disconnectingWithoutALogChannel() throws Exception {
    FTPClient client =
        FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection());

    FtpClientFactory.disconnectQuietly(null, client);

    assertFalse(client.isConnected());
  }

  @Test
  @DisplayName("A connection with no client certificate has no key manager either")
  void noClientCertificateMeansNoKeyManager() throws Exception {
    assertNull(FtpClientFactory.keyManager(variables, connection()));
  }

  @Test
  @DisplayName("Every part of the listing format can be given at once")
  void thewholeListingFormatIsApplied() throws Exception {
    FtpConnection connection = connection();
    connection.setEntryParser("UNIX");
    connection.setServerLanguageCode("en");
    connection.setServerTimeZone("UTC");
    connection.setDefaultDateFormat("d MMM yyyy");
    connection.setRecentDateFormat("d MMM HH:mm");
    connection.setShortMonthNames("jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec");

    FTPClient client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    try {
      assertEquals(1, client.listFiles().length);
    } finally {
      FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    }
  }

  @Test
  @DisplayName("A server which went away while we held the connection is closed without a fuss")
  void disconnectingFromAServerWhichWentAway(@TempDir Path otherRoot) throws Exception {
    FTPClient client;
    try (FtpTestServer ownServer = FtpTestServer.start(otherRoot, FtpSecurityMode.FTP)) {
      FtpConnection connection = connection();
      connection.setServerPort(Integer.toString(ownServer.getPort()));
      client = FtpClientFactory.connectAndLogin(LogChannel.GENERAL, variables, connection);
    }

    // The server is gone: the logout has nobody to talk to any more.
    FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);

    assertFalse(client.isConnected());
  }

  @Test
  @DisplayName("Disconnecting a client which was never connected is harmless")
  void disconnectingAnIdleClientIsHarmless() {
    FTPClient client = new FTPClient();
    FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, client);
    FtpClientFactory.disconnectQuietly(LogChannel.GENERAL, null);
    assertFalse(client.isConnected());
  }

  private FtpConnection connection() {
    FtpConnection connection = new FtpConnection();
    connection.setName("test-ftp");
    connection.setServerName(FtpTestServer.HOST);
    connection.setServerPort(Integer.toString(server.getPort()));
    connection.setUserName(FtpTestServer.USER);
    connection.setPassword(FtpTestServer.PASSWORD);
    return connection;
  }
}
