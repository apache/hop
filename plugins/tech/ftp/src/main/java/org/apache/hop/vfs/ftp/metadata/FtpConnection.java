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
package org.apache.hop.vfs.ftp.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.vfs.ftp.FtpDataChannelProtection;
import org.apache.hop.vfs.ftp.FtpSecurityMode;
import org.apache.hop.vfs.ftp.IFtpConnection;

/**
 * A named FTP or FTPS connection. The name of the connection doubles as a VFS scheme: a connection
 * called {@code prod} makes every file on that server available as {@code prod:///path/to/file} in
 * any transform, action or dialog which accepts a file name.
 *
 * <p>Credentials live here and are applied when the connection is opened, they're never part of the
 * URI.
 */
@Getter
@Setter
@HopMetadata(
    key = "ftp-connection",
    name = "i18n::FtpConnection.Name",
    description = "i18n::FtpConnection.Description",
    image = "FTP.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/ftp-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_FTP_CONNECTION,
    supportsGlobalReplace = true,
    classLoaderGroup = "sftp")
public class FtpConnection extends HopMetadataBase
    implements Serializable, IHopMetadata, IFtpConnection {

  @HopMetadataProperty private String description;

  /**
   * Plain FTP, or FTPS in explicit or implicit mode.
   *
   * <p>Stored under the name of the constant, not under its code: the JSON serializer of the
   * metadata writes {@code Enum.name()} and reads it back with {@code Enum.valueOf}, and honours no
   * {@code storeWithCode} the way the XML one does.
   */
  @HopMetadataProperty private FtpSecurityMode securityMode;

  /** The host name or IP address of the FTP server. */
  @HopMetadataProperty private String serverName;

  /** The port of the server, the default of the security mode when left empty. */
  @HopMetadataProperty private String serverPort;

  @HopMetadataProperty private String userName;

  @HopMetadataProperty(password = true)
  private String password;

  /** Transfer files as binary rather than ASCII. Binary is what you want for anything but text. */
  @HopMetadataProperty private boolean binaryMode;

  /**
   * Open an active data connection, in which the server connects back to the client. Off, the
   * default, means passive, which is what works through a firewall.
   */
  @HopMetadataProperty private boolean activeConnection;

  /** The encoding of the command channel, and with it of the file names. */
  @HopMetadataProperty private String controlEncoding;

  /** Connect timeout in milliseconds, empty for the commons-net default. */
  @HopMetadataProperty private String connectTimeout;

  /** How long a read on the control connection may block, in milliseconds. */
  @HopMetadataProperty private String socketTimeout;

  /** How long a read on the data connection may block, in milliseconds. */
  @HopMetadataProperty private String dataTimeout;

  /**
   * Check that the data connection goes to the same host as the control connection. Servers behind
   * NAT hand out an address which fails that check.
   */
  @HopMetadataProperty private boolean remoteVerification;

  /** Paths are relative to the directory the server drops us in, rather than to its root. */
  @HopMetadataProperty private boolean userDirIsRoot;

  /** Ask the server whether it speaks UTF-8 instead of assuming the control encoding. */
  @HopMetadataProperty private boolean autodetectUtf8;

  /**
   * How often to send a keep alive on the control connection during a transfer, in milliseconds.
   * Empty or 0 for none.
   */
  @HopMetadataProperty private String controlKeepAliveTimeout;

  /** How long to wait for the reply to a keep alive, in milliseconds. */
  @HopMetadataProperty private String controlKeepAliveReplyTimeout;

  /** The lowest port a data connection may be opened on in active mode. */
  @HopMetadataProperty private String activePortRangeFrom;

  /** The highest port a data connection may be opened on in active mode. */
  @HopMetadataProperty private String activePortRangeTo;

  /**
   * Read the last modified time with {@code MDTM}, which is to the second rather than the minute.
   */
  @HopMetadataProperty private boolean mdtmLastModifiedTime;

  /**
   * The listing format of the server, for the rare one commons-net can't work out by itself. For
   * example {@code UNIX}, {@code WINDOWS} or {@code VMS}. Empty means auto detect.
   */
  @HopMetadataProperty private String entryParser;

  /** The language the server writes its listings in, as a two letter code such as {@code fr}. */
  @HopMetadataProperty private String serverLanguageCode;

  /** The time zone the server reports its file dates in, for example {@code UTC}. */
  @HopMetadataProperty private String serverTimeZone;

  /** The date format in the listings of the server, for example {@code d MMM yyyy}. */
  @HopMetadataProperty private String defaultDateFormat;

  /** The format the server uses for dates within the last year, for example {@code d MMM HH:mm}. */
  @HopMetadataProperty private String recentDateFormat;

  /** The month names the server writes, pipe separated. Only for a language nobody else knows. */
  @HopMetadataProperty private String shortMonthNames;

  /** Validate the certificate of an FTPS server. Only used in the FTPS security modes. */
  @HopMetadataProperty private boolean verifyServerCertificate;

  /** Whether the FTPS data connections are encrypted as well as the commands. */
  @HopMetadataProperty private FtpDataChannelProtection dataChannelProtection;

  /** A keystore with the certificate to identify this client with, for FTPS servers asking. */
  @HopMetadataProperty private String clientCertificateFile;

  @HopMetadataProperty(password = true)
  private String clientCertificatePassword;

  /** Which key in the keystore to use, empty for the first one. */
  @HopMetadataProperty private String clientCertificateAlias;

  /** The type of the keystore, empty for the default of the JVM. */
  @HopMetadataProperty private String clientCertificateType;

  /** An FTP proxy to log in through. */
  @HopMetadataProperty private String proxyHost;

  @HopMetadataProperty private String proxyPort;

  @HopMetadataProperty private String proxyUsername;

  @HopMetadataProperty(password = true)
  private String proxyPassword;

  /** A SOCKS proxy to tunnel the connection through. */
  @HopMetadataProperty private String socksProxyHost;

  @HopMetadataProperty private String socksProxyPort;

  @HopMetadataProperty private String socksProxyUsername;

  @HopMetadataProperty(password = true)
  private String socksProxyPassword;

  public FtpConnection() {
    securityMode = FtpSecurityMode.FTP;
    serverPort = Integer.toString(FtpSecurityMode.FTP.getDefaultPort());
    binaryMode = true;
    remoteVerification = true;
    userDirIsRoot = true;
    verifyServerCertificate = true;
    dataChannelProtection = FtpDataChannelProtection.PRIVATE;
  }

  @Override
  public String getFtpConnectionName() {
    return getName();
  }

  /** Never null: an unset security mode is plain FTP. */
  @Override
  public FtpSecurityMode getSecurityMode() {
    return securityMode == null ? FtpSecurityMode.FTP : securityMode;
  }

  /** Never null: an unset protection level is the private one. */
  @Override
  public FtpDataChannelProtection getDataChannelProtection() {
    return dataChannelProtection == null ? FtpDataChannelProtection.PRIVATE : dataChannelProtection;
  }
}
