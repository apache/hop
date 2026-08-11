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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import javax.net.ssl.KeyManager;
import javax.net.ssl.X509TrustManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.KeyManagerUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

/**
 * Opens FTP and FTPS connections for anything which can describe itself as an {@link
 * IFtpConnection}: the named connections in the metadata as well as the inline settings of the FTP
 * actions.
 */
public final class FtpClientFactory {
  private static final Class<?> PKG = FtpClientFactory.class;

  /**
   * The JDK reads the SOCKS credentials from these two system properties during the SOCKS
   * handshake, and offers no per-socket alternative. They're therefore set for the length of a
   * connect and cleared right after, under this lock, so that two connects with different
   * credentials can't hand each other their own.
   */
  private static final Object SOCKS_CREDENTIALS_LOCK = new Object();

  private static final String SOCKS_USERNAME_PROPERTY = "java.net.socks.username";
  private static final String SOCKS_PASSWORD_PROPERTY = "java.net.socks.password";
  private static final int SOCKS_DEFAULT_PORT = 1080;

  private FtpClientFactory() {}

  /**
   * Connect to the server of the given connection and log in.
   *
   * @param log where the progress of the connect is written to
   * @param variables to resolve the variables in the connection with
   * @param connection the server to connect to
   * @return a connected and logged in client; the caller disconnects it
   * @throws HopException when the server can't be reached or refuses the login
   */
  public static FTPClient connectAndLogin(
      ILogChannel log, IVariables variables, IFtpConnection connection) throws HopException {

    String serverName = variables.resolve(connection.getServerName());
    if (StringUtils.isEmpty(serverName)) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "FtpConnection.Error.NoServerName", connection.getFtpConnectionName()));
    }

    FtpSecurityMode securityMode = connection.getSecurityMode();
    int port =
        Const.toInt(variables.resolve(connection.getServerPort()), securityMode.getDefaultPort());

    // With an FTP proxy the client connects to the proxy and names the server it's really after in
    // the user name; without one it connects straight to the server.
    //
    String proxyHost = variables.resolve(connection.getProxyHost());
    boolean throughProxy = StringUtils.isNotEmpty(proxyHost);
    String connectHost = throughProxy ? proxyHost : serverName;
    int connectPort =
        throughProxy ? Const.toInt(variables.resolve(connection.getProxyPort()), port) : port;

    FTPClient client = createClient(securityMode, variables, connection);
    FTPClient connected = null;
    try {
      configure(log, client, variables, connection);
      connect(log, client, variables, connection, connectHost, connectPort);
      connected = client;

      if (log.isDetailed()) {
        log.logDetailed(BaseMessages.getString(PKG, "FtpConnection.Log.Opened", connectHost));
        if (throughProxy) {
          log.logDetailed(BaseMessages.getString(PKG, "FtpConnection.Log.ThroughProxy", proxyHost));
        }
      }

      login(log, client, variables, connection, serverName);
      protectDataChannel(client, connection, securityMode);
      applySession(log, client, variables, connection);
      return client;
    } catch (HopException e) {
      abandon(connected);
      throw e;
    } catch (Exception e) {
      abandon(connected);
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "FtpConnection.Error.UnableToConnect",
              connection.getFtpConnectionName(),
              connectHost + ":" + connectPort),
          e);
    }
  }

  /**
   * The user name to log in with. An FTP proxy is told which server to forward to by appending it
   * to the user name, and takes its own credentials as a second word - the convention these proxy
   * settings have always followed.
   */
  public static String loginUserName(
      IVariables variables, IFtpConnection connection, String serverName) {
    String userName = Const.NVL(variables.resolve(connection.getUserName()), "");
    if (StringUtils.isEmpty(variables.resolve(connection.getProxyHost()))) {
      return userName;
    }
    String proxyUser = variables.resolve(connection.getProxyUsername());
    return userName + "@" + serverName + (StringUtils.isEmpty(proxyUser) ? "" : " " + proxyUser);
  }

  /** The password to log in with, see {@link #loginUserName}. */
  public static String loginPassword(IVariables variables, IFtpConnection connection) {
    String password = Const.NVL(Utils.resolvePassword(variables, connection.getPassword()), "");
    if (StringUtils.isEmpty(variables.resolve(connection.getProxyHost()))) {
      return password;
    }
    String proxyPassword = Utils.resolvePassword(variables, connection.getProxyPassword());
    return password + (StringUtils.isEmpty(proxyPassword) ? "" : " " + proxyPassword);
  }

  /** Does a file with this name exist on the server? */
  public static boolean fileExists(FTPClient client, String filename) throws IOException {
    String[] filenames = client.listNames(filename);
    return filenames != null && filenames.length > 0;
  }

  /**
   * Log out and disconnect. A failure on the way out is logged but never thrown: it would hide
   * whatever the caller was really doing.
   */
  public static void disconnectQuietly(ILogChannel log, FTPClient client) {
    if (client == null || !client.isConnected()) {
      return;
    }
    try {
      client.logout();
    } catch (Exception e) {
      // A server hanging up after the last transfer is a normal way for a session to end, so this
      // is not worth more than a debug line. The disconnect below releases the socket either way.
      if (log != null && log.isDebug()) {
        log.logDebug(BaseMessages.getString(PKG, "FtpConnection.Log.LogoutFailed", e.getMessage()));
      }
    }
    try {
      client.disconnect();
    } catch (Exception e) {
      if (log != null) {
        log.logError(
            BaseMessages.getString(PKG, "FtpConnection.Error.Disconnect", e.getMessage()), e);
      }
    }
  }

  /**
   * The trust manager an FTPS connection checks the server with.
   *
   * <p>Verifying means the trust manager of the JVM, which validates the whole certificate chain
   * against the trust store. Note that {@code TrustManagerUtils.getValidateServerCertificateTrust‐
   * Manager()} of commons-net is deliberately not used for that: despite its name it only checks
   * the dates on the certificate and accepts any issuer.
   *
   * @param verify whether the certificate should be validated at all
   */
  public static X509TrustManager trustManager(boolean verify) throws HopException {
    if (!verify) {
      // Explicitly asked for: self signed certificates are common on the FTPS servers people run.
      return TrustManagerUtils.getAcceptAllTrustManager();
    }
    try {
      return TrustManagerUtils.getDefaultTrustManager(null);
    } catch (GeneralSecurityException e) {
      throw new HopException(BaseMessages.getString(PKG, "FtpConnection.Error.NoTrustManager"), e);
    }
  }

  /**
   * The key manager an FTPS connection identifies itself with, or null when the connection has no
   * client certificate. The keystore is read through VFS, so it can live wherever the rest of the
   * files of a project live.
   */
  public static KeyManager keyManager(IVariables variables, IFtpConnection connection)
      throws HopException {
    String filename = variables.resolve(connection.getClientCertificateFile());
    if (StringUtils.isEmpty(filename)) {
      return null;
    }
    String password = Utils.resolvePassword(variables, connection.getClientCertificatePassword());
    String alias = variables.resolve(connection.getClientCertificateAlias());
    String type = Const.NVL(variables.resolve(connection.getClientCertificateType()), "").trim();

    try (FileObject file = HopVfs.getFileObject(filename);
        InputStream in = file.getContent().getInputStream()) {
      KeyStore keyStore =
          KeyStore.getInstance(StringUtils.isEmpty(type) ? KeyStore.getDefaultType() : type);
      char[] secret = password == null ? new char[0] : password.toCharArray();
      keyStore.load(in, secret);
      return KeyManagerUtils.createClientKeyManager(
          keyStore, StringUtils.isEmpty(alias) ? null : alias, password);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "FtpConnection.Error.ClientCertificate",
              filename,
              connection.getFtpConnectionName()),
          e);
    }
  }

  private static FTPClient createClient(
      FtpSecurityMode securityMode, IVariables variables, IFtpConnection connection)
      throws HopException {
    if (!securityMode.isSecure()) {
      return new FTPClient();
    }
    FTPSClient client = new FTPSClient(securityMode == FtpSecurityMode.FTPS_IMPLICIT);
    boolean verify = connection.isVerifyServerCertificate();
    client.setTrustManager(trustManager(verify));
    // Whether the certificate has to be issued for the host we're talking to.
    client.setEndpointCheckingEnabled(verify);

    KeyManager keyManager = keyManager(variables, connection);
    if (keyManager != null) {
      client.setKeyManager(keyManager);
    }
    return client;
  }

  private static void configure(
      ILogChannel log, FTPClient client, IVariables variables, IFtpConnection connection)
      throws HopException {

    Integer connectTimeout = timeout(variables, connection.getConnectTimeout());
    if (connectTimeout != null) {
      client.setConnectTimeout(connectTimeout);
      client.setDefaultTimeout(connectTimeout);
      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(
                PKG, "FtpConnection.Log.SetTimeout", String.valueOf(connectTimeout)));
      }
    }

    String controlEncoding = variables.resolve(connection.getControlEncoding());
    if (StringUtils.isNotEmpty(controlEncoding)) {
      client.setControlEncoding(controlEncoding);
      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(PKG, "FtpConnection.Log.SetEncoding", controlEncoding));
      }
    }

    if (!connection.isRemoteVerification()) {
      client.setRemoteVerificationEnabled(false);
    }

    client.setAutodetectUTF8(connection.isAutodetectUtf8());

    Integer keepAlive = timeout(variables, connection.getControlKeepAliveTimeout());
    if (keepAlive != null) {
      client.setControlKeepAliveTimeout(Duration.ofMillis(keepAlive));
    }
    Integer keepAliveReply = timeout(variables, connection.getControlKeepAliveReplyTimeout());
    if (keepAliveReply != null) {
      client.setControlKeepAliveReplyTimeout(Duration.ofMillis(keepAliveReply));
    }

    int activeFrom = Const.toInt(variables.resolve(connection.getActivePortRangeFrom()), -1);
    int activeTo = Const.toInt(variables.resolve(connection.getActivePortRangeTo()), -1);
    if (activeFrom > 0 && activeTo >= activeFrom) {
      client.setActivePortRange(activeFrom, activeTo);
    }

    applyListingFormat(client, variables, connection);

    String socksProxyHost = variables.resolve(connection.getSocksProxyHost());
    if (StringUtils.isNotEmpty(socksProxyHost)) {
      String socksProxyPort = variables.resolve(connection.getSocksProxyPort());
      if (StringUtils.isEmpty(socksProxyPort)) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "FtpConnection.Error.SocksProxyPortMissing",
                socksProxyHost,
                connection.getFtpConnectionName()));
      }
      client.setProxy(
          new Proxy(
              Proxy.Type.SOCKS,
              new InetSocketAddress(
                  socksProxyHost, Const.toInt(socksProxyPort, SOCKS_DEFAULT_PORT))));
    }
  }

  /**
   * How the listings of this server should be read. Everything here is optional: left empty,
   * commons-net works the format out from the answer of the server to {@code SYST}.
   */
  private static void applyListingFormat(
      FTPClient client, IVariables variables, IFtpConnection connection) {
    String entryParser = Const.NVL(variables.resolve(connection.getEntryParser()), "").trim();
    String languageCode =
        Const.NVL(variables.resolve(connection.getServerLanguageCode()), "").trim();
    String timeZone = Const.NVL(variables.resolve(connection.getServerTimeZone()), "").trim();
    String defaultDateFormat =
        Const.NVL(variables.resolve(connection.getDefaultDateFormat()), "").trim();
    String recentDateFormat =
        Const.NVL(variables.resolve(connection.getRecentDateFormat()), "").trim();
    String shortMonthNames =
        Const.NVL(variables.resolve(connection.getShortMonthNames()), "").trim();

    if (entryParser.isEmpty()
        && languageCode.isEmpty()
        && timeZone.isEmpty()
        && defaultDateFormat.isEmpty()
        && recentDateFormat.isEmpty()
        && shortMonthNames.isEmpty()) {
      return;
    }

    // An empty system key means "work it out yourself", which is what the default constructor of
    // the config does.
    FTPClientConfig config =
        entryParser.isEmpty() ? new FTPClientConfig() : new FTPClientConfig(entryParser);
    if (!languageCode.isEmpty()) {
      config.setServerLanguageCode(languageCode);
    }
    if (!timeZone.isEmpty()) {
      config.setServerTimeZoneId(timeZone);
    }
    if (!defaultDateFormat.isEmpty()) {
      config.setDefaultDateFormatStr(defaultDateFormat);
    }
    if (!recentDateFormat.isEmpty()) {
      config.setRecentDateFormatStr(recentDateFormat);
    }
    if (!shortMonthNames.isEmpty()) {
      config.setShortMonthNames(shortMonthNames);
    }
    client.configure(config);
  }

  /**
   * Connect, with the SOCKS credentials in place if there are any. They live in system properties
   * for the length of the handshake because that's the only place the JDK reads them from.
   */
  private static void connect(
      ILogChannel log,
      FTPClient client,
      IVariables variables,
      IFtpConnection connection,
      String host,
      int port)
      throws IOException, HopException {

    String socksUser = variables.resolve(connection.getSocksProxyUsername());
    String socksPassword = Utils.resolvePassword(variables, connection.getSocksProxyPassword());
    boolean hasUser = StringUtils.isNotEmpty(socksUser);
    boolean hasPassword = StringUtils.isNotEmpty(socksPassword);

    if (hasUser != hasPassword) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "FtpConnection.Error.SocksProxyIncompleteCredentials",
              variables.resolve(connection.getSocksProxyHost()),
              connection.getFtpConnectionName()));
    }

    if (hasUser) {
      synchronized (SOCKS_CREDENTIALS_LOCK) {
        System.setProperty(SOCKS_USERNAME_PROPERTY, socksUser);
        System.setProperty(SOCKS_PASSWORD_PROPERTY, socksPassword);
        try {
          client.connect(host, port);
        } finally {
          System.clearProperty(SOCKS_USERNAME_PROPERTY);
          System.clearProperty(SOCKS_PASSWORD_PROPERTY);
        }
      }
    } else {
      client.connect(host, port);
    }

    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "FtpConnection.Error.ConnectionRefused",
              host + ":" + port,
              String.valueOf(reply),
              Const.NVL(client.getReplyString(), "").trim()));
    }
    if (log.isDebug()) {
      log.logDebug(
          BaseMessages.getString(
              PKG,
              "FtpConnection.Log.ServerGreeting",
              Const.NVL(client.getReplyString(), "").trim()));
    }
  }

  private static void login(
      ILogChannel log,
      FTPClient client,
      IVariables variables,
      IFtpConnection connection,
      String serverName)
      throws IOException, HopException {

    String userName = loginUserName(variables, connection, serverName);
    if (!client.login(userName, loginPassword(variables, connection))) {
      // The reply code, never the reply text: servers have been known to echo back what was sent.
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "FtpConnection.Error.LoginRefused",
              userName,
              serverName,
              String.valueOf(client.getReplyCode())));
    }
    if (log.isDetailed()) {
      log.logDetailed(BaseMessages.getString(PKG, "FtpConnection.Log.LoggedIn", userName));
    }
  }

  /**
   * Ask the server to encrypt the data connections as well. Sent after the login because that's
   * where servers expect it, and only for FTPS.
   */
  private static void protectDataChannel(
      FTPClient client, IFtpConnection connection, FtpSecurityMode securityMode)
      throws IOException {
    if (!securityMode.isSecure() || !(client instanceof FTPSClient ftpsClient)) {
      return;
    }
    ftpsClient.execPBSZ(0);
    ftpsClient.execPROT(connection.getDataChannelProtection().getCode());
  }

  private static void applySession(
      ILogChannel log, FTPClient client, IVariables variables, IFtpConnection connection)
      throws IOException {

    if (connection.isActiveConnection()) {
      client.enterLocalActiveMode();
    } else {
      client.enterLocalPassiveMode();
    }
    if (log.isDetailed()) {
      log.logDetailed(
          BaseMessages.getString(
              PKG,
              connection.isActiveConnection()
                  ? "FtpConnection.Log.SetActive"
                  : "FtpConnection.Log.SetPassive"));
    }

    client.setFileType(connection.isBinaryMode() ? FTP.BINARY_FILE_TYPE : FTP.ASCII_FILE_TYPE);
    if (log.isDetailed()) {
      log.logDetailed(
          BaseMessages.getString(
              PKG,
              connection.isBinaryMode()
                  ? "FtpConnection.Log.SetBinary"
                  : "FtpConnection.Log.SetAscii"));
    }

    Integer socketTimeout = timeout(variables, connection.getSocketTimeout());
    if (socketTimeout != null) {
      client.setSoTimeout(socketTimeout);
    }
    Integer dataTimeout = timeout(variables, connection.getDataTimeout());
    if (dataTimeout != null) {
      client.setDataTimeout(Duration.ofMillis(dataTimeout));
    }
  }

  /** Drop a connection we're about to throw over: we already have the better error. */
  private static void abandon(FTPClient client) {
    if (client != null) {
      try {
        client.disconnect();
      } catch (Exception e) {
        // Nothing useful left to do with this one.
      }
    }
  }

  private static Integer timeout(IVariables variables, String value) {
    String resolved = variables.resolve(value);
    if (StringUtils.isEmpty(resolved)) {
      return null;
    }
    int timeout = Const.toInt(resolved, -1);
    return timeout <= 0 ? null : timeout;
  }
}
