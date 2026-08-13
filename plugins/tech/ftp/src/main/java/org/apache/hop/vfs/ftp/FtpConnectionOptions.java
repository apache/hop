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

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;
import javax.net.ssl.KeyManager;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftps.FtpsMode;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;

/** Translates a {@link FtpConnection} into the VFS options the FTP provider connects with. */
public final class FtpConnectionOptions {

  private static final int SOCKS_DEFAULT_PORT = 1080;

  private FtpConnectionOptions() {}

  /**
   * @param variables used to resolve the variables in the connection
   * @param connection the named connection
   * @param base the options VFS handed us, may be null
   * @return a copy of {@code base} with the settings of the connection applied
   */
  public static FileSystemOptions build(
      IVariables variables, FtpConnection connection, FileSystemOptions base)
      throws FileSystemException {
    FileSystemOptions options =
        base == null ? new FileSystemOptions() : (FileSystemOptions) base.clone();

    FtpSecurityMode securityMode = connection.getSecurityMode();
    FtpFileSystemConfigBuilder config =
        securityMode.isSecure()
            ? FtpsFileSystemConfigBuilder.getInstance()
            : FtpFileSystemConfigBuilder.getInstance();

    config.setPassiveMode(options, !connection.isActiveConnection());
    config.setFileType(options, connection.isBinaryMode() ? FtpFileType.BINARY : FtpFileType.ASCII);
    config.setUserDirIsRoot(options, connection.isUserDirIsRoot());
    config.setRemoteVerification(options, connection.isRemoteVerification());
    config.setAutodetectUtf8(options, connection.isAutodetectUtf8());
    config.setMdtmLastModifiedTime(options, connection.isMdtmLastModifiedTime());

    String controlEncoding = variables.resolve(connection.getControlEncoding());
    if (StringUtils.isNotEmpty(controlEncoding)) {
      config.setControlEncoding(options, controlEncoding);
    }

    String entryParser = variables.resolve(connection.getEntryParser());
    if (StringUtils.isNotEmpty(entryParser)) {
      config.setEntryParser(options, entryParser);
    }

    String serverLanguageCode = variables.resolve(connection.getServerLanguageCode());
    if (StringUtils.isNotEmpty(serverLanguageCode)) {
      config.setServerLanguageCode(options, serverLanguageCode);
    }

    String serverTimeZone = variables.resolve(connection.getServerTimeZone());
    if (StringUtils.isNotEmpty(serverTimeZone)) {
      config.setServerTimeZoneId(options, serverTimeZone);
    }

    String defaultDateFormat = variables.resolve(connection.getDefaultDateFormat());
    if (StringUtils.isNotEmpty(defaultDateFormat)) {
      config.setDefaultDateFormat(options, defaultDateFormat);
    }

    String recentDateFormat = variables.resolve(connection.getRecentDateFormat());
    if (StringUtils.isNotEmpty(recentDateFormat)) {
      config.setRecentDateFormat(options, recentDateFormat);
    }

    String shortMonthNames = variables.resolve(connection.getShortMonthNames());
    if (StringUtils.isNotEmpty(shortMonthNames)) {
      config.setShortMonthNames(options, shortMonthNames.split("\\|"));
    }

    Duration connectTimeout = timeout(variables, connection.getConnectTimeout());
    if (connectTimeout != null) {
      config.setConnectTimeout(options, connectTimeout);
    }
    Duration socketTimeout = timeout(variables, connection.getSocketTimeout());
    if (socketTimeout != null) {
      config.setSoTimeout(options, socketTimeout);
    }
    Duration dataTimeout = timeout(variables, connection.getDataTimeout());
    if (dataTimeout != null) {
      config.setDataTimeout(options, dataTimeout);
    }

    Duration keepAlive = timeout(variables, connection.getControlKeepAliveTimeout());
    if (keepAlive != null) {
      config.setControlKeepAliveTimeout(options, keepAlive);
    }
    Duration keepAliveReply = timeout(variables, connection.getControlKeepAliveReplyTimeout());
    if (keepAliveReply != null) {
      config.setControlKeepAliveReplyTimeout(options, keepAliveReply);
    }

    int activeFrom = Const.toInt(variables.resolve(connection.getActivePortRangeFrom()), -1);
    int activeTo = Const.toInt(variables.resolve(connection.getActivePortRangeTo()), -1);
    if (activeFrom > 0 && activeTo >= activeFrom) {
      config.setActivePortRange(options, Range.of(activeFrom, activeTo));
    }

    String socksProxyHost = variables.resolve(connection.getSocksProxyHost());
    if (StringUtils.isNotEmpty(socksProxyHost)) {
      int socksProxyPort =
          Const.toInt(variables.resolve(connection.getSocksProxyPort()), SOCKS_DEFAULT_PORT);
      config.setProxy(
          options,
          new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(socksProxyHost, socksProxyPort)));
    }

    if (securityMode.isSecure()) {
      applyFtps(variables, connection, options, securityMode);
    }

    return options;
  }

  private static void applyFtps(
      IVariables variables,
      FtpConnection connection,
      FileSystemOptions options,
      FtpSecurityMode securityMode)
      throws FileSystemException {
    FtpsFileSystemConfigBuilder config = FtpsFileSystemConfigBuilder.getInstance();
    config.setFtpsMode(
        options,
        securityMode == FtpSecurityMode.FTPS_IMPLICIT ? FtpsMode.IMPLICIT : FtpsMode.EXPLICIT);

    config.setDataChannelProtectionLevel(
        options,
        FtpsDataChannelProtectionLevel.valueOf(connection.getDataChannelProtection().getCode()));

    try {
      config.setTrustManager(
          options, FtpClientFactory.trustManager(connection.isVerifyServerCertificate()));
      KeyManager keyManager = FtpClientFactory.keyManager(variables, connection);
      if (keyManager != null) {
        config.setKeyManager(options, keyManager);
      }
    } catch (HopException e) {
      throw new FileSystemException(e.getMessage(), e);
    }
  }

  private static Duration timeout(IVariables variables, String value) {
    String resolved = variables.resolve(value);
    if (StringUtils.isEmpty(resolved)) {
      return null;
    }
    int timeout = Const.toInt(resolved, -1);
    return timeout <= 0 ? null : Duration.ofMillis(timeout);
  }
}
