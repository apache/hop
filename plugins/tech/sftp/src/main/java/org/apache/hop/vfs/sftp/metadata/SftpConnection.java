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
package org.apache.hop.vfs.sftp.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * A named SFTP connection. The name of the connection doubles as a VFS scheme: a connection called
 * {@code prod} makes every file on that server available as {@code prod:///path/to/file} in any
 * transform, action or dialog which accepts a file name.
 *
 * <p>Credentials live here and are applied through VFS file system options, they're never part of
 * the URI.
 */
@Getter
@Setter
@HopMetadata(
    key = "sftp-connection",
    name = "i18n::SftpConnection.Name",
    description = "i18n::SftpConnection.Description",
    image = "SFTP.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/sftp-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_SFTP_CONNECTION,
    supportsGlobalReplace = true)
public class SftpConnection extends HopMetadataBase implements Serializable, IHopMetadata {

  public static final int DEFAULT_PORT = 22;

  @HopMetadataProperty private String description;

  /** The host name or IP address of the SSH server. */
  @HopMetadataProperty private String serverName;

  /** The port of the SSH server, {@value #DEFAULT_PORT} when left empty. */
  @HopMetadataProperty private String serverPort;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  /** Authenticate with a private key instead of (or on top of) a password. */
  @HopMetadataProperty private boolean useKeyFile;

  /** The private key file, resolved through VFS so it can live outside the local file system. */
  @HopMetadataProperty private String keyFilename;

  @HopMetadataProperty(password = true)
  private String keyPassphrase;

  /** Compression to negotiate with the server: empty or {@code none}, or {@code zlib}. */
  @HopMetadataProperty private String compression;

  /**
   * When enabled, paths are relative to the home directory of the user. When disabled (the
   * default), paths are absolute paths on the server.
   */
  @HopMetadataProperty private boolean userDirIsRoot;

  /** {@code no} (default), {@code yes} or {@code ask}. */
  @HopMetadataProperty private String strictHostKeyChecking;

  /** Optional known_hosts file, only used with strict host key checking. */
  @HopMetadataProperty private String knownHostsFile;

  /** Optional, for example {@code publickey,keyboard-interactive,password}. */
  @HopMetadataProperty private String preferredAuthentications;

  /** Optional key exchange algorithms, for example {@code diffie-hellman-group14-sha256}. */
  @HopMetadataProperty private String keyExchangeAlgorithm;

  /** Read the OpenSSH configuration of the user running Hop ({@code ~/.ssh/config}). */
  @HopMetadataProperty private boolean loadOpenSshConfig;

  /** Connection timeout in milliseconds, empty for the jsch default. */
  @HopMetadataProperty private String connectionTimeout;

  /** Session timeout in milliseconds, empty for the jsch default. */
  @HopMetadataProperty private String sessionTimeout;

  /** The encoding of the file names on the server, empty for the jsch default (UTF-8). */
  @HopMetadataProperty private String fileNameEncoding;

  /**
   * Skip the exec channel the provider opens to read POSIX permissions and the like. Servers which
   * only allow the sftp subsystem need this.
   */
  @HopMetadataProperty private boolean disableDetectExecChannel;

  /** {@code HTTP}, {@code SOCKS5} or {@code STREAM}, empty for a direct connection. */
  @HopMetadataProperty private String proxyType;

  /** The command a {@code STREAM} proxy runs, for example {@code nc %h %p}. */
  @HopMetadataProperty private String proxyCommand;

  @HopMetadataProperty private String proxyHost;

  @HopMetadataProperty private String proxyPort;

  @HopMetadataProperty private String proxyUsername;

  @HopMetadataProperty(password = true)
  private String proxyPassword;

  public SftpConnection() {
    serverPort = Integer.toString(DEFAULT_PORT);
    strictHostKeyChecking = "no";
    compression = "none";
  }
}
