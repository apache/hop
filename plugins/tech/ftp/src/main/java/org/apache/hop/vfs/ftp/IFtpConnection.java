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

/**
 * Everything {@link FtpClientFactory} needs to open a connection to an FTP server.
 *
 * <p>Two things implement it: {@link org.apache.hop.vfs.ftp.metadata.FtpConnection}, the named
 * connection in the metadata, and the FTP actions, which keep the same settings inline for
 * backwards compatibility. That way both routes end up in the same connect code instead of each
 * growing their own.
 *
 * <p>All values are unresolved: the factory resolves the variables in them.
 *
 * <p>The methods with a default are the ones the older inline settings of the actions never had.
 * Their defaults are what those actions did before the setting existed, so an action which doesn't
 * override them behaves exactly as it used to.
 */
public interface IFtpConnection {

  /** How this connection is named in errors and log lines. */
  String getFtpConnectionName();

  String getServerName();

  /** The port, empty for the default of the security mode. */
  String getServerPort();

  String getUserName();

  String getPassword();

  /** Transfer files as binary rather than ASCII. */
  boolean isBinaryMode();

  /**
   * Use an active data connection: the server connects back to the client. Off means passive, which
   * is what works through a firewall.
   */
  boolean isActiveConnection();

  /** The encoding of the command channel, and with it of the file names. */
  String getControlEncoding();

  /** How long to wait for the control connection to be established, in milliseconds. */
  String getConnectTimeout();

  /**
   * An FTP proxy to log in through. The client connects to this host instead of to the server, and
   * the server it really wants is passed along in the user name - see {@link
   * FtpClientFactory#loginUserName}.
   */
  String getProxyHost();

  String getProxyPort();

  String getProxyUsername();

  String getProxyPassword();

  /** A SOCKS proxy to tunnel the connection through. */
  String getSocksProxyHost();

  String getSocksProxyPort();

  String getSocksProxyUsername();

  String getSocksProxyPassword();

  /** Plain FTP unless the connection says otherwise. */
  default FtpSecurityMode getSecurityMode() {
    return FtpSecurityMode.FTP;
  }

  /**
   * Whether the FTPS data connections are encrypted as well as the commands. Only used when {@link
   * #getSecurityMode()} is an FTPS mode.
   */
  default FtpDataChannelProtection getDataChannelProtection() {
    return FtpDataChannelProtection.PRIVATE;
  }

  /** How long a read on the control connection may block, in milliseconds. Empty for no limit. */
  default String getSocketTimeout() {
    return null;
  }

  /** How long a read on the data connection may block, in milliseconds. Empty for no limit. */
  default String getDataTimeout() {
    return null;
  }

  /**
   * Check that the data connection is opened to the same host as the control connection. Servers
   * behind NAT hand out an address which fails that check, so it can be turned off.
   */
  default boolean isRemoteVerification() {
    return true;
  }

  /**
   * Validate the certificate of an FTPS server against the trust store and check that it was issued
   * for the host we're talking to. Turning this off accepts any certificate, which is what a self
   * signed one needs.
   */
  default boolean isVerifyServerCertificate() {
    return true;
  }

  /**
   * A keystore holding the certificate this client identifies itself with, for FTPS servers which
   * ask for one. Read through VFS, so it doesn't have to sit on the local file system.
   */
  default String getClientCertificateFile() {
    return null;
  }

  /** The password of the keystore, which is also used to unlock the key in it. */
  default String getClientCertificatePassword() {
    return null;
  }

  /** Which key in the keystore to use, empty for the first one in it. */
  default String getClientCertificateAlias() {
    return null;
  }

  /** The type of the keystore, empty for the default of the JVM ({@code PKCS12}). */
  default String getClientCertificateType() {
    return null;
  }

  /**
   * Ask the server whether it speaks UTF-8 rather than assuming the control encoding. Servers which
   * answer that question badly are the reason this is off by default.
   */
  default boolean isAutodetectUtf8() {
    return false;
  }

  /**
   * How often to send a keep alive on the control connection while a transfer is running, in
   * milliseconds. Long transfers die on firewalls which drop an idle control connection; this keeps
   * it busy. Empty or 0 means no keep alive.
   */
  default String getControlKeepAliveTimeout() {
    return null;
  }

  /** How long to wait for the reply to a keep alive, in milliseconds. */
  default String getControlKeepAliveReplyTimeout() {
    return null;
  }

  /** The lowest port to open a data connection on in active mode. Empty for any free port. */
  default String getActivePortRangeFrom() {
    return null;
  }

  /** The highest port to open a data connection on in active mode. */
  default String getActivePortRangeTo() {
    return null;
  }

  /**
   * The listing format of the server, for the rare one which can't be worked out from its own
   * answer to {@code SYST}. For example {@code UNIX}, {@code WINDOWS} or {@code VMS}.
   */
  default String getEntryParser() {
    return null;
  }

  /** The language the server writes its listings in, as a two letter code such as {@code fr}. */
  default String getServerLanguageCode() {
    return null;
  }

  /** The time zone the server reports its file dates in, for example {@code UTC}. */
  default String getServerTimeZone() {
    return null;
  }

  /** The date format in the listings of the server, for example {@code d MMM yyyy}. */
  default String getDefaultDateFormat() {
    return null;
  }

  /** The format the server uses for dates within the last year, for example {@code d MMM HH:mm}. */
  default String getRecentDateFormat() {
    return null;
  }

  /** The month names the server writes, pipe separated, for a language nobody else knows. */
  default String getShortMonthNames() {
    return null;
  }
}
