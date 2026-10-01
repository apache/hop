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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The part the two named-connection providers share. */
class FtpConnectionSupportTest {

  private static final String SOCKS_USER_PROPERTY = "java.net.socks.username";
  private static final String SOCKS_PASSWORD_PROPERTY = "java.net.socks.password";

  private final IVariables variables = new Variables();

  @BeforeAll
  static void initEncryption() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  /**
   * VFS looks a file system up under the options it was created with. Handing out a fresh set every
   * time would mean a new file system, and a new control connection, for every single file.
   */
  @Test
  @DisplayName("The options are built once and handed out again after that")
  void optionsAreBuiltOnce() throws Exception {
    FtpConnectionSupport support = new FtpConnectionSupport(variables, connection());

    FileSystemOptions first = support.options(null);
    FileSystemOptions second = support.options(new FileSystemOptions());

    assertSame(first, second);
  }

  @Test
  @DisplayName("The settings of the connection are on the options it hands out")
  void theOptionsCarryTheConnection() throws Exception {
    FtpConnection connection = connection();
    connection.setControlEncoding("UTF-8");

    FileSystemOptions options = new FtpConnectionSupport(variables, connection).options(null);

    assertEquals("UTF-8", FtpFileSystemConfigBuilder.getInstance().getControlEncoding(options));
    assertSame(connection, new FtpConnectionSupport(variables, connection).getConnection());
  }

  @Test
  @DisplayName("Without socks credentials nothing is put in the system properties")
  void withoutCredentialsNothingIsSet() throws Exception {
    FtpConnectionSupport support = new FtpConnectionSupport(variables, connection());

    String seen = support.withSocksCredentials(() -> System.getProperty(SOCKS_USER_PROPERTY));

    assertNull(seen);
  }

  @Test
  @DisplayName("The socks credentials are in place during the call and gone right after")
  void credentialsAreScopedToTheCall() throws Exception {
    FtpConnection connection = connection();
    connection.setSocksProxyHost("proxy.example.com");
    connection.setSocksProxyPort("1080");
    connection.setSocksProxyUsername("socks-user");
    connection.setSocksProxyPassword("socks-password");
    FtpConnectionSupport support = new FtpConnectionSupport(variables, connection);

    String[] seen =
        support.withSocksCredentials(
            () ->
                new String[] {
                  System.getProperty(SOCKS_USER_PROPERTY),
                  System.getProperty(SOCKS_PASSWORD_PROPERTY)
                });

    assertEquals("socks-user", seen[0]);
    assertEquals("socks-password", seen[1]);
    assertNull(System.getProperty(SOCKS_USER_PROPERTY), "the user name outlived the call");
    assertNull(System.getProperty(SOCKS_PASSWORD_PROPERTY), "the password outlived the call");
  }

  @Test
  @DisplayName("A failure inside the call still clears the credentials")
  void credentialsAreClearedOnFailure() {
    FtpConnection connection = connection();
    connection.setSocksProxyHost("proxy.example.com");
    connection.setSocksProxyUsername("socks-user");
    connection.setSocksProxyPassword("socks-password");
    FtpConnectionSupport support = new FtpConnectionSupport(variables, connection);

    assertThrows(
        FileSystemException.class,
        () ->
            support.withSocksCredentials(
                () -> {
                  throw new FileSystemException("the connect failed");
                }));

    assertNull(System.getProperty(SOCKS_USER_PROPERTY));
    assertNull(System.getProperty(SOCKS_PASSWORD_PROPERTY));
  }

  private FtpConnection connection() {
    FtpConnection connection = new FtpConnection();
    connection.setName("prod");
    connection.setServerName("ftp.example.com");
    connection.setUserName("hop");
    connection.setPassword("secret");
    return connection;
  }
}
