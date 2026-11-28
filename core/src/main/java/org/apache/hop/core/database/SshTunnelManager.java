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

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/**
 * Manages SSH tunnels for database connections using JSch port forwarding.
 *
 * <p>Without jump hosts Hop signs in to the connection's SSH server and forwards a free local port
 * to the database. With jump hosts ({@link DatabaseMeta#getSshTunnelJumpHosts()}) Hop signs in to
 * the first jump host directly and reaches every next SSH server through a local port forwarded by
 * the session before it, the way {@code ssh -J} does. The last session, the one to the connection's
 * own SSH server, forwards the database port. Each session has its own credentials and its own
 * keepalive.
 */
public class SshTunnelManager {

  /**
   * Name of the variable that exposes the dynamically allocated local port of the SSH tunnel. It
   * can be referenced from a manually specified JDBC URL (for example a complex Oracle TCPS
   * descriptor) as <code>${sshTunnel.localPort}</code> to point the driver at the local end of the
   * tunnel.
   */
  public static final String VARIABLE_SSH_TUNNEL_LOCAL_PORT = "sshTunnel.localPort";

  /** The address every forwarded port listens on: never reachable from another machine. */
  private static final String LOOPBACK_ADDRESS = "127.0.0.1";

  private static final int DEFAULT_SSH_PORT = 22;

  private static final int CONNECT_TIMEOUT_MS = 30000;

  /** The sessions in the order they were opened: the jump hosts first, the SSH server last. */
  private final List<Session> sessions = new ArrayList<>();

  private int localPort;

  /**
   * Opens an SSH tunnel with local port forwarding, through the connection's jump hosts if it has
   * any.
   *
   * @param variables variables for resolving expressions
   * @param databaseMeta the database metadata containing SSH tunnel configuration
   * @param log the log channel
   * @return the local port that forwards to the remote database
   * @throws HopDatabaseException if the tunnel cannot be established
   */
  public int openTunnel(IVariables variables, DatabaseMeta databaseMeta, ILogChannel log)
      throws HopDatabaseException {
    SshServer server = null;
    try {
      List<SshServer> servers = getSshServers(variables, databaseMeta, log);
      String remoteHost = variables.resolve(databaseMeta.getHostname());
      int remotePort =
          Const.toInt(
              variables.resolve(databaseMeta.getPort()), databaseMeta.getDefaultDatabasePort());

      Session previous = null;
      for (SshServer next : servers) {
        server = next;
        String connectHost = server.host();
        int connectPort = server.port();
        if (previous != null) {
          // Reach this server through the session before it, like ssh -J.
          connectHost = LOOPBACK_ADDRESS;
          connectPort = previous.setPortForwardingL(0, server.host(), server.port());
        }
        log.logBasic(
            "Opening SSH tunnel to "
                + server.describe()
                + (previous == null ? "" : " through localhost:" + connectPort));
        Session session = createSession(server, connectHost, connectPort, previous != null);
        sessions.add(session);
        session.connect(CONNECT_TIMEOUT_MS);
        previous = session;
      }
      server = null;

      // Local port 0 = auto-assign a free port
      localPort = previous.setPortForwardingL(0, remoteHost, remotePort);
      log.logBasic(
          "SSH tunnel established: localhost:"
              + localPort
              + " -> "
              + remoteHost
              + ":"
              + remotePort);

      return localPort;
    } catch (Exception e) {
      closeTunnel(log);
      String message = "Failed to open SSH tunnel";
      if (server != null) {
        message += ": unable to connect to " + server.describe();
      }
      throw new HopDatabaseException(message, e);
    }
  }

  /**
   * The SSH servers of the tunnel in the order Hop connects to them: the jump hosts with a host
   * name, then the connection's own SSH server. A jump host whose host name resolves to an empty
   * string is left out, so a variable can drop it in an environment that does not need it.
   */
  private static List<SshServer> getSshServers(
      IVariables variables, DatabaseMeta databaseMeta, ILogChannel log) {
    List<SshServer> servers = new ArrayList<>();
    List<SshJumpHost> jumpHosts = databaseMeta.getSshTunnelJumpHosts();
    if (jumpHosts != null) {
      for (int i = 0; i < jumpHosts.size(); i++) {
        SshJumpHost jumpHost = jumpHosts.get(i);
        String label = "SSH jump host " + (i + 1) + " of " + jumpHosts.size();
        String host = variables.resolve(jumpHost.getHost());
        if (Utils.isEmpty(host)) {
          log.logDetailed(label + " has no host name and is skipped");
          continue;
        }
        servers.add(
            new SshServer(
                label,
                host,
                Const.toInt(variables.resolve(jumpHost.getPort()), DEFAULT_SSH_PORT),
                variables.resolve(jumpHost.getUsername()),
                jumpHost.getAuthenticationMethod() == SshAuthenticationMethod.PRIVATE_KEY,
                decrypt(variables, jumpHost.getPassword()),
                variables.resolve(jumpHost.getPrivateKeyFile()),
                decrypt(variables, jumpHost.getPassphrase())));
      }
    }
    servers.add(
        new SshServer(
            "SSH server",
            variables.resolve(databaseMeta.getSshTunnelHost()),
            Const.toInt(variables.resolve(databaseMeta.getSshTunnelPort()), DEFAULT_SSH_PORT),
            variables.resolve(databaseMeta.getSshTunnelUsername()),
            databaseMeta.isSshTunnelUsePrivateKey(),
            decrypt(variables, databaseMeta.getSshTunnelPassword()),
            variables.resolve(databaseMeta.getSshTunnelPrivateKeyFile()),
            decrypt(variables, databaseMeta.getSshTunnelPassphrase())));
    return servers;
  }

  private static String decrypt(IVariables variables, String value) {
    return Encr.decryptPasswordOptionallyEncrypted(variables.resolve(value));
  }

  /**
   * Creates the session for one SSH server. Every server gets its own {@link JSch} instance: the
   * identities belong to that instance, so a server is only offered its own private key and never
   * the key of another hop, which would count against its {@code MaxAuthTries}.
   */
  private static Session createSession(
      SshServer server, String connectHost, int connectPort, boolean throughJumpHost)
      throws JSchException {
    JSch jsch = new JSch();

    // Private key authentication
    if (server.usePrivateKey()) {
      if (!Utils.isEmpty(server.passphrase())) {
        jsch.addIdentity(server.privateKeyFile(), server.passphrase());
      } else {
        jsch.addIdentity(server.privateKeyFile());
      }
    }

    Session session = jsch.getSession(server.username(), connectHost, connectPort);
    if (throughJumpHost) {
      // Behind a jump host the session points at a local port: keep the real name for its host key
      session.setHostKeyAlias(server.host());
    }

    // Password authentication (supports both password and keyboard-interactive)
    if (!server.usePrivateKey() && !Utils.isEmpty(server.password())) {
      session.setPassword(server.password());
      session.setUserInfo((UserInfo) new SshPasswordUserInfo(server.password()));
    }

    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");
    config.put("PreferredAuthentications", "publickey,keyboard-interactive,password");
    session.setConfig(config);

    // Send keepalive every 30s to prevent VPN/firewall from dropping idle connections
    session.setServerAliveInterval(30000);
    session.setServerAliveCountMax(3);
    return session;
  }

  /**
   * Closes the SSH tunnel: disconnects the sessions from the last one opened to the first, so no
   * session loses the forwarded port it runs over while it is still connected.
   */
  public void closeTunnel(ILogChannel log) {
    if (sessions.isEmpty()) {
      return;
    }
    Session last = sessions.get(sessions.size() - 1);
    if (last.isConnected()) {
      try {
        last.delPortForwardingL(localPort);
      } catch (Exception e) {
        log.logDebug("Error removing port forwarding: " + e.getMessage());
      }
    }
    boolean wasOpen = false;
    for (int i = sessions.size() - 1; i >= 0; i--) {
      Session session = sessions.get(i);
      if (session.isConnected()) {
        wasOpen = true;
        session.disconnect();
      }
    }
    sessions.clear();
    if (wasOpen) {
      log.logBasic("SSH tunnel closed");
    }
  }

  /**
   * @return true if every session of the tunnel, jump hosts included, is connected
   */
  public boolean isOpen() {
    if (sessions.isEmpty()) {
      return false;
    }
    for (Session session : sessions) {
      if (!session.isConnected()) {
        return false;
      }
    }
    return true;
  }

  public int getLocalPort() {
    return localPort;
  }

  /** One resolved SSH server of the tunnel, a jump host or the connection's own SSH server. */
  record SshServer(
      String label,
      String host,
      int port,
      String username,
      boolean usePrivateKey,
      String password,
      String privateKeyFile,
      String passphrase) {

    String describe() {
      return label + " " + host + ":" + port + " as user " + username;
    }

    /** Never print the password or passphrase, also not by accident in a debug log. */
    @Override
    public String toString() {
      return describe();
    }
  }

  /** Handles keyboard-interactive authentication by responding with the password. */
  private static class SshPasswordUserInfo implements UserInfo, UIKeyboardInteractive {
    private final String password;

    SshPasswordUserInfo(String password) {
      this.password = password;
    }

    @Override
    public String[] promptKeyboardInteractive(
        String destination, String name, String instruction, String[] prompt, boolean[] echo) {
      return new String[] {password};
    }

    @Override
    public String getPassphrase() {
      return null;
    }

    @Override
    public String getPassword() {
      return password;
    }

    @Override
    public boolean promptPassword(String message) {
      return true;
    }

    @Override
    public boolean promptPassphrase(String message) {
      return false;
    }

    @Override
    public boolean promptYesNo(String message) {
      return true;
    }

    @Override
    public void showMessage(String message) {
      // JSch's informational messages are not shown
    }
  }
}
