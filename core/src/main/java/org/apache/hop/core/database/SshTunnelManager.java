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
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;
import java.util.Properties;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/** Manages SSH tunnels for database connections using JSch port forwarding. */
public class SshTunnelManager {

  private Session session;
  private int localPort;

  /**
   * Opens an SSH tunnel with local port forwarding.
   *
   * @param variables variables for resolving expressions
   * @param databaseMeta the database metadata containing SSH tunnel configuration
   * @param log the log channel
   * @return the local port that forwards to the remote database
   * @throws HopDatabaseException if the tunnel cannot be established
   */
  public int openTunnel(IVariables variables, DatabaseMeta databaseMeta, ILogChannel log)
      throws HopDatabaseException {
    try {
      String sshHost = variables.resolve(databaseMeta.getSshTunnelHost());
      int sshPort = Const.toInt(variables.resolve(databaseMeta.getSshTunnelPort()), 22);
      String sshUser = variables.resolve(databaseMeta.getSshTunnelUsername());
      String sshPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              variables.resolve(databaseMeta.getSshTunnelPassword()));

      String remoteHost = variables.resolve(databaseMeta.getHostname());
      int remotePort =
          Const.toInt(
              variables.resolve(databaseMeta.getPort()), databaseMeta.getDefaultDatabasePort());

      JSch jsch = new JSch();

      // Private key authentication
      if (databaseMeta.isSshTunnelUsePrivateKey()) {
        String keyFile = variables.resolve(databaseMeta.getSshTunnelPrivateKeyFile());
        String passphrase =
            Encr.decryptPasswordOptionallyEncrypted(
                variables.resolve(databaseMeta.getSshTunnelPassphrase()));
        if (!Utils.isEmpty(passphrase)) {
          jsch.addIdentity(keyFile, passphrase);
        } else {
          jsch.addIdentity(keyFile);
        }
      }

      session = jsch.getSession(sshUser, sshHost, sshPort);

      // Password authentication (supports both password and keyboard-interactive)
      if (!databaseMeta.isSshTunnelUsePrivateKey() && !Utils.isEmpty(sshPassword)) {
        session.setPassword(sshPassword);
        session.setUserInfo((UserInfo) new SshPasswordUserInfo(sshPassword));
      }

      Properties config = new Properties();
      config.put("StrictHostKeyChecking", "no");
      config.put("PreferredAuthentications", "publickey,keyboard-interactive,password");
      session.setConfig(config);

      // Send keepalive every 30s to prevent VPN/firewall from dropping idle connections
      session.setServerAliveInterval(30000);
      session.setServerAliveCountMax(3);

      log.logBasic("Opening SSH tunnel to " + sshHost + ":" + sshPort + " as user " + sshUser);
      session.connect(30000);

      // Local port 0 = auto-assign a free port
      localPort = session.setPortForwardingL(0, remoteHost, remotePort);
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
      throw new HopDatabaseException("Failed to open SSH tunnel", e);
    }
  }

  /** Closes the SSH tunnel and disconnects the session. */
  public void closeTunnel(ILogChannel log) {
    if (session != null && session.isConnected()) {
      try {
        session.delPortForwardingL(localPort);
      } catch (Exception e) {
        log.logDebug("Error removing port forwarding: " + e.getMessage());
      }
      session.disconnect();
      log.logBasic("SSH tunnel closed");
    }
    session = null;
  }

  public boolean isOpen() {
    return session != null && session.isConnected();
  }

  public int getLocalPort() {
    return localPort;
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
    public void showMessage(String message) {}
  }
}
