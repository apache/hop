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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;

/**
 * The part a named FTP connection adds to a Commons VFS file provider. Commons VFS has separate
 * providers for {@code ftp} and {@code ftps} and they can only be extended one at a time, so the
 * two providers behind a named connection share this instead of a common base class.
 */
class FtpConnectionSupport {

  private static final String SOCKS_USERNAME_PROPERTY = "java.net.socks.username";
  private static final String SOCKS_PASSWORD_PROPERTY = "java.net.socks.password";

  /** Guards the SOCKS credentials, see {@link FtpClientFactory}. */
  private static final Object SOCKS_CREDENTIALS_LOCK = new Object();

  private final IVariables variables;
  private final FtpConnection connection;

  /** The session settings of the connection, the same instance for every file we resolve. */
  private FileSystemOptions connectionOptions;

  FtpConnectionSupport(IVariables variables, FtpConnection connection) {
    this.variables = variables;
    this.connection = connection;
  }

  FtpConnection getConnection() {
    return connection;
  }

  /**
   * VFS caches a file system under the options it was created with, and looks it up under the
   * options of the caller. Handing it a freshly built set of options for every file would make
   * those two differ every single time: a new file system, and with it a new control connection,
   * for every file. Build them once and use them for both.
   */
  synchronized FileSystemOptions options(FileSystemOptions base) throws FileSystemException {
    if (connectionOptions == null) {
      connectionOptions = FtpConnectionOptions.build(variables, connection, base);
    }
    return connectionOptions;
  }

  /**
   * Run the code which opens the connection with the SOCKS credentials of the connection in place.
   * The JDK only reads them from these system properties, so they're set for the length of the
   * handshake and cleared again right after.
   *
   * <p>This covers the connect Commons VFS makes when it builds the file system. A reconnect it
   * makes later on, after the server dropped the control connection, happens outside of this and
   * would go without the credentials - one more reason to prefer an FTP proxy over a SOCKS one.
   */
  <T> T withSocksCredentials(FileSystemCall<T> call) throws FileSystemException {
    String user = variables.resolve(connection.getSocksProxyUsername());
    String password = Utils.resolvePassword(variables, connection.getSocksProxyPassword());
    if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password)) {
      return call.call();
    }
    synchronized (SOCKS_CREDENTIALS_LOCK) {
      System.setProperty(SOCKS_USERNAME_PROPERTY, user);
      System.setProperty(SOCKS_PASSWORD_PROPERTY, password);
      try {
        return call.call();
      } finally {
        System.clearProperty(SOCKS_USERNAME_PROPERTY);
        System.clearProperty(SOCKS_PASSWORD_PROPERTY);
      }
    }
  }

  @FunctionalInterface
  interface FileSystemCall<T> {
    T call() throws FileSystemException;
  }
}
