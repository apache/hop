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
 *
 */

package org.apache.hop.workflow.actions.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;

public class FtpClientUtil {
  private static final Class<?> PKG = FtpClientUtil.class; // For Translator

  public static final FTPClient connectAndLogin(
      ILogChannel log, IVariables variables, IFtpConnection connection, String actionName)
      throws HopException {
    FTPClient ftpClient = new FTPClient();

    try {
      // Create ftp client to host:port ...
      String realServername = variables.resolve(connection.getServerName());
      String realServerPort = variables.resolve(connection.getServerPort());
      int realPort = 21;
      if (StringUtils.isNotEmpty(realServername)) {
        realPort = Const.toInt(realServerPort, 21);
      }

      // Connect to the server
      ftpClient.connect(realServername, realPort);

      if (log.isDetailed()) {
        log.logDetailed(BaseMessages.getString(PKG, "ActionFTP.OpenedConnection", realServername));
      }

      Proxy proxy = Proxy.NO_PROXY;

      if (!Utils.isEmpty(connection.getProxyHost())) {
        String realProxyHost = variables.resolve(connection.getProxyHost());
        if (log.isDetailed()) {
          log.logDetailed(
              BaseMessages.getString(PKG, "ActionFTP.OpenedProxyConnectionOn", realProxyHost));
        }

        // FIXME: Proper default port for proxy
        int realProxyPort = Const.toInt(variables.resolve(connection.getProxyPort()), 21);

        proxy = new Proxy(Proxy.Type.DIRECT, new InetSocketAddress(realProxyHost, realProxyPort));
      }

      // set activeConnection connectmode ...
      if (connection.isActiveConnection()) {
        ftpClient.enterLocalActiveMode();
        if (log.isDetailed()) {
          log.logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetActive"));
        }
      } else {
        ftpClient.enterLocalPassiveMode();
        if (log.isDetailed()) {
          log.logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetPassive"));
        }
      }

      // Binary transfer type?
      //
      if (connection.isBinaryMode()) {
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        if (log.isDetailed()) {
          log.logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetBinary"));
        }
      } else {
        ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
        if (log.isDetailed()) {
          log.logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetAscii"));
        }
      }

      // Set the timeout
      ftpClient.setConnectTimeout(connection.getTimeout());
      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(
                PKG, "ActionFTP.SetTimeout", String.valueOf(connection.getTimeout())));
      }

      // The control encoding (special filename characters etc)
      //
      if (StringUtils.isNotEmpty(connection.getControlEncoding())) {
        String realControlEncoding = variables.resolve(connection.getControlEncoding());
        ftpClient.setControlEncoding(realControlEncoding);
        if (log.isDetailed()) {
          log.logDetailed(
              BaseMessages.getString(
                  PKG, "ActionFTP.SetEncoding", connection.getControlEncoding()));
        }
      }

      // If socks proxy server was provided
      if (!Utils.isEmpty(connection.getSocksProxyHost())) {
        if (!Utils.isEmpty(connection.getSocksProxyPort())) {
          String realSocksProxyHost = variables.resolve(connection.getSocksProxyPort());
          int realSocksProxyPort =
              Const.toInt(variables.resolve(connection.getSocksProxyPort()), 21);
          proxy =
              new Proxy(
                  Proxy.Type.SOCKS, new InetSocketAddress(realSocksProxyHost, realSocksProxyPort));
        } else {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "ActionFTP.SocksProxy.PortMissingException",
                  variables.resolve(connection.getSocksProxyHost()),
                  actionName));
        }

        // Proxy Authentication
        //
        // Clear the settings first to be safe.
        //
        clearSocksJvmSettings();

        if (!Utils.isEmpty(connection.getSocksProxyUsername())
            && !Utils.isEmpty(connection.getSocksProxyPassword())) {
          System.setProperty(
              "java.net.socks.username", variables.resolve(connection.getSocksProxyUsername()));
          System.setProperty(
              "java.net.socks.password", variables.resolve(connection.getSocksProxyPassword()));
        } else if (!Utils.isEmpty(connection.getSocksProxyUsername())
                && Utils.isEmpty(connection.getSocksProxyPassword())
            || Utils.isEmpty(connection.getSocksProxyUsername())
                && !Utils.isEmpty(connection.getSocksProxyPassword())) {
          // we have a username without a password or vica versa
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "ActionFTP.SocksProxy.IncompleteCredentials",
                  variables.resolve(connection.getSocksProxyHost()),
                  actionName));
        }
      }

      String realUsername =
          variables.resolve(connection.getUserName())
              + (!Utils.isEmpty(connection.getProxyHost()) ? "@" + realServername : "")
              + (!Utils.isEmpty(connection.getProxyUsername())
                  ? " " + variables.resolve(connection.getProxyUsername())
                  : "");

      String realPassword =
          Utils.resolvePassword(variables, connection.getPassword())
              + (!Utils.isEmpty(connection.getProxyPassword())
                  ? " " + Utils.resolvePassword(variables, connection.getProxyPassword())
                  : "");

      // Login to the server...
      //
      ftpClient.login(realUsername, realPassword);

      // Remove password from logging, you don't know where it ends up.
      if (log.isDetailed()) {
        log.logDetailed(BaseMessages.getString(PKG, "ActionFTP.LoggedIn", realUsername));
      }

    } catch (Exception e) {
      throw new HopException("Error creating FTP connection", e);
    }

    return ftpClient;
  }

  public static void clearSocksJvmSettings() {
    System.clearProperty("java.net.socks.username");
    System.clearProperty("java.net.socks.password");
  }

  public static boolean fileExists(FTPClient ftpClient, String filename) throws IOException {
    InputStream checkInputStream = ftpClient.retrieveFileStream(filename);
    if (checkInputStream == null) {
      return false;
    }
    int replyCode = ftpClient.getReplyCode();
    if (replyCode == 500) {
      return false;
    }
    // Don't leave this dangling...
    //
    checkInputStream.close();

    return true;
  }
}
