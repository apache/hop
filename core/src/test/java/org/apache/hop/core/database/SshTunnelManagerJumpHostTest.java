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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.session.Session;
import org.apache.sshd.common.util.net.SshdSocketAddress;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.forward.StaticDecisionForwardingFilter;
import org.apache.sshd.server.forward.TcpForwardingFilter;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Opens real tunnels through embedded SSH servers: a jump host that only accepts a private key and
 * an SSH server behind it that only accepts a password, in front of a TCP server that stands in for
 * the database. Each SSH server records the addresses it is asked to connect to, which shows the
 * tunnel passes the servers in the configured order.
 */
class SshTunnelManagerJumpHostTest {

  private static final String LOOPBACK = "127.0.0.1";
  private static final String DATABASE_GREETING = "hello from the database";

  private static final String JUMP_USER = "jumper";
  private static final String SERVER_USER = "hop";
  private static final String SERVER_PASSWORD = "server-secret";

  @TempDir Path tempDir;

  private final List<SshServer> sshServers = new ArrayList<>();
  private final List<String> jumpHostConnects = Collections.synchronizedList(new ArrayList<>());
  private final List<String> serverConnects = Collections.synchronizedList(new ArrayList<>());

  private ServerSocket database;
  private Thread databaseThread;
  private KeyPair jumpKeyPair;
  private Path jumpKeyFile;
  private SshServer jumpHost;
  private SshServer sshServer;
  private SshTunnelManager tunnelManager;
  private ILogChannel log;
  private IVariables variables;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    database = new ServerSocket(0, 50, InetAddress.getByName(LOOPBACK));
    databaseThread = new Thread(this::serveDatabase, "fake-database");
    databaseThread.setDaemon(true);
    databaseThread.start();

    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    jumpKeyPair = generator.generateKeyPair();
    jumpKeyFile = tempDir.resolve("jump_rsa");
    Files.writeString(
        jumpKeyFile,
        "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                .encodeToString(jumpKeyPair.getPrivate().getEncoded())
            + "\n-----END PRIVATE KEY-----\n",
        StandardCharsets.US_ASCII);

    // The jump host only takes the private key: no password, no keyboard-interactive.
    jumpHost = startSshServer(jumpHostConnects);
    jumpHost.setPublickeyAuthenticator(
        (username, key, session) ->
            JUMP_USER.equals(username) && KeyUtils.compareKeys(jumpKeyPair.getPublic(), key));
    jumpHost.setPasswordAuthenticator(null);
    jumpHost.setKeyboardInteractiveAuthenticator(null);
    jumpHost.start();

    // The SSH server behind it only takes a password.
    sshServer = startSshServer(serverConnects);
    sshServer.setPasswordAuthenticator(
        (username, password, session) ->
            SERVER_USER.equals(username) && SERVER_PASSWORD.equals(password));
    sshServer.setPublickeyAuthenticator(null);
    sshServer.start();

    tunnelManager = new SshTunnelManager();
    log = mock(ILogChannel.class);
    variables = new Variables();
  }

  @AfterEach
  void tearDown() throws Exception {
    tunnelManager.closeTunnel(log);
    for (SshServer server : sshServers) {
      server.stop(true);
    }
    database.close();
  }

  @Test
  void tunnelPassesTheJumpHostBeforeTheSshServer() throws Exception {
    DatabaseMeta databaseMeta =
        databaseMeta(
            SERVER_PASSWORD,
            new SshJumpHost(
                LOOPBACK,
                String.valueOf(jumpHost.getPort()),
                JUMP_USER,
                SshAuthenticationMethod.PRIVATE_KEY,
                null,
                jumpKeyFile.toString(),
                null));

    int localPort = tunnelManager.openTunnel(variables, databaseMeta, log);

    assertTrue(tunnelManager.isOpen());
    assertEquals(localPort, tunnelManager.getLocalPort());
    assertEquals(DATABASE_GREETING, readGreeting(localPort));

    // The jump host was only asked for the SSH server, the SSH server only for the database.
    assertEquals(List.of(LOOPBACK + ":" + sshServer.getPort()), jumpHostConnects);
    assertEquals(List.of(LOOPBACK + ":" + database.getLocalPort()), serverConnects);

    tunnelManager.closeTunnel(log);
    assertFalse(tunnelManager.isOpen());
  }

  @Test
  void tunnelWithoutJumpHostsGoesStraightToTheSshServer() throws Exception {
    int localPort = tunnelManager.openTunnel(variables, databaseMeta(SERVER_PASSWORD), log);

    assertEquals(DATABASE_GREETING, readGreeting(localPort));
    assertTrue(jumpHostConnects.isEmpty());
    assertEquals(List.of(LOOPBACK + ":" + database.getLocalPort()), serverConnects);
  }

  @Test
  void jumpHostWithAnEmptyHostNameIsSkipped() throws Exception {
    variables.setVariable("BASTION_HOST", "");
    DatabaseMeta databaseMeta =
        databaseMeta(
            SERVER_PASSWORD,
            new SshJumpHost(
                "${BASTION_HOST}",
                "22",
                JUMP_USER,
                SshAuthenticationMethod.PASSWORD,
                "unused",
                null,
                null));

    int localPort = tunnelManager.openTunnel(variables, databaseMeta, log);

    assertEquals(DATABASE_GREETING, readGreeting(localPort));
    assertTrue(jumpHostConnects.isEmpty());
  }

  @Test
  void jumpHostVariablesAreResolved() throws Exception {
    variables.setVariable("BASTION_HOST", LOOPBACK);
    variables.setVariable("BASTION_PORT", String.valueOf(jumpHost.getPort()));
    variables.setVariable("BASTION_KEY", jumpKeyFile.toString());
    DatabaseMeta databaseMeta =
        databaseMeta(
            SERVER_PASSWORD,
            new SshJumpHost(
                "${BASTION_HOST}",
                "${BASTION_PORT}",
                JUMP_USER,
                SshAuthenticationMethod.PRIVATE_KEY,
                null,
                "${BASTION_KEY}",
                null));

    int localPort = tunnelManager.openTunnel(variables, databaseMeta, log);

    assertEquals(DATABASE_GREETING, readGreeting(localPort));
    assertEquals(List.of(LOOPBACK + ":" + sshServer.getPort()), jumpHostConnects);
  }

  @Test
  void failingJumpHostIsNamedAndNothingStaysOpen() {
    // The jump host does not accept passwords.
    DatabaseMeta databaseMeta =
        databaseMeta(
            SERVER_PASSWORD,
            new SshJumpHost(
                LOOPBACK,
                String.valueOf(jumpHost.getPort()),
                JUMP_USER,
                SshAuthenticationMethod.PASSWORD,
                "wrong",
                null,
                null));

    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class,
            () -> tunnelManager.openTunnel(variables, databaseMeta, log));

    assertTrue(e.getMessage().contains("SSH jump host 1 of 1"), e.getMessage());
    assertFalse(tunnelManager.isOpen());
    assertTrue(serverConnects.isEmpty());
  }

  @Test
  void failingSshServerBehindTheJumpHostIsNamedAndTheJumpHostIsClosed() throws Exception {
    DatabaseMeta databaseMeta =
        databaseMeta(
            "wrong-password",
            new SshJumpHost(
                LOOPBACK,
                String.valueOf(jumpHost.getPort()),
                JUMP_USER,
                SshAuthenticationMethod.PRIVATE_KEY,
                null,
                jumpKeyFile.toString(),
                null));

    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class,
            () -> tunnelManager.openTunnel(variables, databaseMeta, log));

    assertTrue(e.getMessage().contains("SSH server " + LOOPBACK), e.getMessage());
    assertFalse(tunnelManager.isOpen());
    assertTrue(waitForNoSessions(jumpHost), "the jump host session stays open");
  }

  private DatabaseMeta databaseMeta(String serverPassword, SshJumpHost... jumpHosts) {
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getSshTunnelHost()).thenReturn(LOOPBACK);
    when(databaseMeta.getSshTunnelPort()).thenReturn(String.valueOf(sshServer.getPort()));
    when(databaseMeta.getSshTunnelUsername()).thenReturn(SERVER_USER);
    when(databaseMeta.getSshTunnelPassword()).thenReturn(serverPassword);
    when(databaseMeta.isSshTunnelUsePrivateKey()).thenReturn(false);
    when(databaseMeta.getSshTunnelJumpHosts()).thenReturn(List.of(jumpHosts));
    when(databaseMeta.getHostname()).thenReturn(LOOPBACK);
    when(databaseMeta.getPort()).thenReturn(String.valueOf(database.getLocalPort()));
    when(databaseMeta.getDefaultDatabasePort()).thenReturn(5432);
    return databaseMeta;
  }

  private SshServer startSshServer(List<String> connects) {
    SshServer server = SshServer.setUpDefaultServer();
    server.setHost(LOOPBACK);
    server.setPort(0);
    server.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
    server.setForwardingFilter(
        new StaticDecisionForwardingFilter(true) {
          @Override
          public boolean canConnect(
              TcpForwardingFilter.Type type, SshdSocketAddress address, Session session) {
            connects.add(address.getHostName() + ":" + address.getPort());
            return true;
          }
        });
    sshServers.add(server);
    return server;
  }

  private void serveDatabase() {
    while (!database.isClosed()) {
      try (Socket client = database.accept()) {
        OutputStream out = client.getOutputStream();
        out.write((DATABASE_GREETING + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
      } catch (IOException e) {
        // closed at the end of the test
      }
    }
  }

  private static String readGreeting(int localPort) throws IOException {
    try (Socket socket = new Socket(LOOPBACK, localPort)) {
      socket.setSoTimeout(10000);
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      return reader.readLine();
    }
  }

  private static boolean waitForNoSessions(SshServer server) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10000;
    while (System.currentTimeMillis() < deadline) {
      if (server.getActiveSessions().isEmpty()) {
        return true;
      }
      Thread.sleep(50);
    }
    return false;
  }
}
