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

import java.net.InetAddress;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.vfs.sftp.client.SftpClient;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;

/** Looking up named SFTP connections and connecting with them. */
public final class SftpConnections {

  private SftpConnections() {}

  /**
   * @param metadataProvider where the connections live
   * @param name the name of the connection, variables already resolved
   * @return the connection, never null
   * @throws HopException if there's no connection with that name
   */
  public static SftpConnection load(IHopMetadataProvider metadataProvider, String name)
      throws HopException {
    if (StringUtils.isEmpty(name)) {
      throw new HopException("Please specify an SFTP connection");
    }
    SftpConnection connection =
        metadataProvider.getSerializer(SftpConnection.class).load(name.trim());
    if (connection == null) {
      throw new HopException("Unable to find SFTP connection \"" + name + "\" in the metadata");
    }
    return connection;
  }

  /**
   * The base URI of a connection: {@code <name>://}. Append an absolute path on the server to get
   * to a file.
   */
  public static String getBaseUri(SftpConnection connection) {
    return connection.getName() + "://";
  }

  /**
   * Build the URI of a file behind a named connection. Both the folder and the file name are
   * optional.
   */
  public static String buildUri(SftpConnection connection, String folder, String filename) {
    StringBuilder uri = new StringBuilder(getBaseUri(connection));
    String path = StringUtils.isEmpty(folder) ? "" : folder.replace('\\', '/').trim();
    if (StringUtils.isNotEmpty(filename)) {
      if (StringUtils.isNotEmpty(path) && !path.endsWith("/")) {
        path += "/";
      }
      path += filename.replace('\\', '/').trim();
    }
    while (path.startsWith("/")) {
      path = path.substring(1);
    }
    return uri.append(path).toString();
  }

  /**
   * Open a jsch based client for the given connection. It's connected and logged in, the caller
   * disconnects.
   *
   * @param variables to resolve the variables in the connection with
   * @param connection the connection to open
   * @return a connected client
   */
  public static SftpClient createClient(IVariables variables, SftpConnection connection)
      throws HopException {
    String serverName = variables.resolve(connection.getServerName());
    if (StringUtils.isEmpty(serverName)) {
      throw new HopException("SFTP connection \"" + connection.getName() + "\" has no server name");
    }
    int port =
        Const.toInt(variables.resolve(connection.getServerPort()), SftpConnection.DEFAULT_PORT);
    String username = variables.resolve(connection.getUsername());

    String keyFilename = null;
    String passPhrase = null;
    if (connection.isUseKeyFile()) {
      keyFilename = variables.resolve(connection.getKeyFilename());
      if (StringUtils.isEmpty(keyFilename)) {
        throw new HopException(
            "SFTP connection \""
                + connection.getName()
                + "\" is set to use a private key but no key file is configured");
      }
      if (!HopVfs.fileExists(keyFilename)) {
        throw new HopException(
            "The private key file \""
                + keyFilename
                + "\" of SFTP connection \""
                + connection.getName()
                + "\" doesn't exist");
      }
      passPhrase = Utils.resolvePassword(variables, connection.getKeyPassphrase());
    }

    try {
      SftpClient client =
          new SftpClient(
              InetAddress.getByName(serverName), port, username, keyFilename, passPhrase);
      client.setCompression(variables.resolve(connection.getCompression()));

      String proxyHost = variables.resolve(connection.getProxyHost());
      if (StringUtils.isNotEmpty(proxyHost)) {
        client.setProxy(
            proxyHost,
            variables.resolve(connection.getProxyPort()),
            variables.resolve(connection.getProxyUsername()),
            Utils.resolvePassword(variables, connection.getProxyPassword()),
            variables.resolve(connection.getProxyType()));
      }

      client.login(Utils.resolvePassword(variables, connection.getPassword()));
      return client;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to connect to SFTP connection \"" + connection.getName() + "\"", e);
    }
  }
}
