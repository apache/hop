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

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileUtil;
import org.apache.commons.vfs2.provider.sftp.BytesIdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;

/** Translates a {@link SftpConnection} into the VFS options the SFTP provider connects with. */
public final class SftpConnectionOptions {

  private SftpConnectionOptions() {}

  /**
   * @param variables used to resolve the variables in the connection
   * @param connection the named connection
   * @param base the options VFS handed us, may be null
   * @return a copy of {@code base} with the settings of the connection applied
   */
  public static FileSystemOptions build(
      IVariables variables, SftpConnection connection, FileSystemOptions base)
      throws FileSystemException {
    FileSystemOptions options =
        base == null ? new FileSystemOptions() : (FileSystemOptions) base.clone();
    SftpFileSystemConfigBuilder config = SftpFileSystemConfigBuilder.getInstance();

    config.setUserDirIsRoot(options, connection.isUserDirIsRoot());
    config.setStrictHostKeyChecking(
        options, Const.NVL(variables.resolve(connection.getStrictHostKeyChecking()), "no").trim());

    String knownHosts = variables.resolve(connection.getKnownHostsFile());
    if (StringUtils.isNotEmpty(knownHosts)) {
      config.setKnownHosts(options, new File(HopVfs.getFilename(getFileObject(knownHosts))));
    }

    String compression = variables.resolve(connection.getCompression());
    if (StringUtils.isNotEmpty(compression)) {
      config.setCompression(options, compression);
    }

    String preferredAuthentications = variables.resolve(connection.getPreferredAuthentications());
    if (StringUtils.isNotEmpty(preferredAuthentications)) {
      config.setPreferredAuthentications(options, preferredAuthentications);
    }

    String keyExchangeAlgorithm = variables.resolve(connection.getKeyExchangeAlgorithm());
    if (StringUtils.isNotEmpty(keyExchangeAlgorithm)) {
      config.setKeyExchangeAlgorithm(options, keyExchangeAlgorithm);
    }

    String fileNameEncoding = variables.resolve(connection.getFileNameEncoding());
    if (StringUtils.isNotEmpty(fileNameEncoding)) {
      config.setFileNameEncoding(options, fileNameEncoding);
    }

    config.setLoadOpenSSHConfig(options, connection.isLoadOpenSshConfig());
    config.setDisableDetectExecChannel(options, connection.isDisableDetectExecChannel());

    if (connection.isUseKeyFile()) {
      config.setIdentityProvider(options, identity(variables, connection));
    }

    Integer connectionTimeout = timeout(variables, connection.getConnectionTimeout());
    if (connectionTimeout != null) {
      config.setConnectTimeoutMillis(options, connectionTimeout);
    }
    Integer sessionTimeout = timeout(variables, connection.getSessionTimeout());
    if (sessionTimeout != null) {
      config.setSessionTimeoutMillis(options, sessionTimeout);
    }

    applyProxy(variables, connection, options, config);

    return options;
  }

  private static void applyProxy(
      IVariables variables,
      SftpConnection connection,
      FileSystemOptions options,
      SftpFileSystemConfigBuilder config)
      throws FileSystemException {
    String proxyHost = variables.resolve(connection.getProxyHost());
    if (StringUtils.isEmpty(proxyHost)) {
      return;
    }
    String proxyType = Const.NVL(variables.resolve(connection.getProxyType()), "").trim();
    if ("SOCKS5".equalsIgnoreCase(proxyType)) {
      config.setProxyType(options, SftpFileSystemConfigBuilder.PROXY_SOCKS5);
    } else if ("HTTP".equalsIgnoreCase(proxyType)) {
      config.setProxyType(options, SftpFileSystemConfigBuilder.PROXY_HTTP);
    } else if ("STREAM".equalsIgnoreCase(proxyType)) {
      config.setProxyType(options, SftpFileSystemConfigBuilder.PROXY_STREAM);
      String proxyCommand = variables.resolve(connection.getProxyCommand());
      if (StringUtils.isNotEmpty(proxyCommand)) {
        config.setProxyCommand(options, proxyCommand);
      }
    } else {
      throw new FileSystemException(
          "Unsupported proxy type \""
              + proxyType
              + "\" for SFTP connection \""
              + connection.getName()
              + "\", use HTTP, SOCKS5 or STREAM");
    }
    config.setProxyHost(options, proxyHost);
    int proxyPort = Const.toInt(variables.resolve(connection.getProxyPort()), -1);
    if (proxyPort > 0) {
      config.setProxyPort(options, proxyPort);
    }
    String proxyUser = variables.resolve(connection.getProxyUsername());
    if (StringUtils.isNotEmpty(proxyUser)) {
      config.setProxyUser(options, proxyUser);
    }
    String proxyPassword = Utils.resolvePassword(variables, connection.getProxyPassword());
    if (StringUtils.isNotEmpty(proxyPassword)) {
      config.setProxyPassword(options, proxyPassword);
    }
  }

  private static BytesIdentityInfo identity(IVariables variables, SftpConnection connection)
      throws FileSystemException {
    String keyFilename = variables.resolve(connection.getKeyFilename());
    if (StringUtils.isEmpty(keyFilename)) {
      throw new FileSystemException(
          "SFTP connection \""
              + connection.getName()
              + "\" is set to use a private key but no key file is configured");
    }
    FileObject keyFile = getFileObject(keyFilename);
    try {
      if (!keyFile.exists()) {
        throw new FileSystemException(
            "The private key file \""
                + keyFilename
                + "\" of SFTP connection \""
                + connection.getName()
                + "\" doesn't exist");
      }
      // Read the key rather than pointing jsch at a File: the key is allowed to live anywhere VFS
      // can reach, not just on the local file system.
      //
      byte[] privateKey = FileUtil.getContent(keyFile);
      String passphrase = Utils.resolvePassword(variables, connection.getKeyPassphrase());
      byte[] passphraseBytes =
          StringUtils.isEmpty(passphrase) ? null : passphrase.getBytes(StandardCharsets.ISO_8859_1);
      return new BytesIdentityInfo(privateKey, passphraseBytes);
    } catch (FileSystemException e) {
      throw e;
    } catch (Exception e) {
      throw new FileSystemException(
          "Unable to read the private key file \"" + keyFilename + "\"", e);
    }
  }

  private static Integer timeout(IVariables variables, String value) {
    String resolved = variables.resolve(value);
    if (StringUtils.isEmpty(resolved)) {
      return null;
    }
    int timeout = Const.toInt(resolved, -1);
    return timeout < 0 ? null : timeout;
  }

  private static FileObject getFileObject(String filename) throws FileSystemException {
    try {
      return HopVfs.getFileObject(filename);
    } catch (Exception e) {
      throw new FileSystemException("Unable to find file \"" + filename + "\"", e);
    }
  }
}
