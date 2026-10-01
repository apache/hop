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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;

/**
 * An embedded Apache FtpServer for the tests in this plugin, so none of them need a server of their
 * own or a network. Serves one directory over plain FTP or over FTPS.
 */
public final class FtpTestServer implements AutoCloseable {

  public static final String USER = "tester";
  public static final String PASSWORD = "secret";
  public static final String HOST = "127.0.0.1";

  /** A second user with no write permission, to check what happens when the server says no. */
  public static final String READ_ONLY_USER = "reader";

  public static final String READ_ONLY_PASSWORD = "read-only";

  private static final String KEYSTORE_PASSWORD = "hop-test-pass";

  private final FtpServer server;
  private final Listener listener;
  private final Path home;
  private final Path keyStore;

  private FtpTestServer(FtpServer server, Listener listener, Path home, Path keyStore) {
    this.server = server;
    this.listener = listener;
    this.home = home;
    this.keyStore = keyStore;
  }

  /**
   * @param home the directory the server hands out
   * @param mode plain FTP, or one of the FTPS modes to serve it over TLS
   */
  public static FtpTestServer start(Path home, FtpSecurityMode mode) throws Exception {
    return start(home, mode, false);
  }

  /**
   * @param home the directory the server hands out
   * @param mode plain FTP, or one of the FTPS modes to serve it over TLS
   * @param needClientCertificate demand a certificate from the client, for mutual TLS
   */
  public static FtpTestServer start(Path home, FtpSecurityMode mode, boolean needClientCertificate)
      throws Exception {
    Files.createDirectories(home);

    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory listenerFactory = new ListenerFactory();
    listenerFactory.setPort(0);
    listenerFactory.setServerAddress(HOST);

    Path keyStore = null;
    if (mode.isSecure()) {
      keyStore = generateKeyStore();
      SslConfigurationFactory ssl = new SslConfigurationFactory();
      ssl.setKeystoreFile(keyStore.toFile());
      ssl.setKeystorePassword(KEYSTORE_PASSWORD);
      ssl.setKeyPassword(KEYSTORE_PASSWORD);
      if (needClientCertificate) {
        // The client signs itself with the same self signed certificate the server uses, so the
        // server's own keystore doubles as the store it trusts clients from.
        ssl.setTruststoreFile(keyStore.toFile());
        ssl.setTruststorePassword(KEYSTORE_PASSWORD);
        ssl.setClientAuthentication("NEED");
      }
      listenerFactory.setSslConfiguration(ssl.createSslConfiguration());
      listenerFactory.setImplicitSsl(mode == FtpSecurityMode.FTPS_IMPLICIT);
    }

    Listener listener = listenerFactory.createListener();
    serverFactory.addListener("default", listener);

    PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
    serverFactory.setUserManager(userManagerFactory.createUserManager());

    BaseUser user = new BaseUser();
    user.setName(USER);
    user.setPassword(PASSWORD);
    user.setHomeDirectory(home.toString());
    user.setAuthorities(Collections.singletonList(new WritePermission()));
    serverFactory.getUserManager().save(user);

    BaseUser readOnly = new BaseUser();
    readOnly.setName(READ_ONLY_USER);
    readOnly.setPassword(READ_ONLY_PASSWORD);
    readOnly.setHomeDirectory(home.toString());
    readOnly.setAuthorities(Collections.emptyList());
    serverFactory.getUserManager().save(readOnly);

    FtpServer server = serverFactory.createServer();
    server.start();
    return new FtpTestServer(server, listener, home, keyStore);
  }

  public int getPort() {
    return listener.getPort();
  }

  public Path getHome() {
    return home;
  }

  /** The keystore of this server, which the tests also use as the client certificate. */
  public Path getKeyStore() {
    return keyStore;
  }

  public static String getKeyStorePassword() {
    return KEYSTORE_PASSWORD;
  }

  /** Put a file on the server. */
  public Path writeFile(String name, String content) throws IOException {
    Path file = home.resolve(name);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
    return file;
  }

  /** What the server has under this name, or null when there's nothing there. */
  public String readFile(String name) throws IOException {
    Path file = home.resolve(name);
    return Files.exists(file) ? Files.readString(file) : null;
  }

  @Override
  public void close() throws IOException {
    if (server != null) {
      server.stop();
    }
    if (keyStore != null) {
      Files.deleteIfExists(keyStore);
    }
  }

  /** A fresh PKCS12 keystore with a self-signed certificate for the loopback address. */
  private static Path generateKeyStore() throws Exception {
    Path path = Files.createTempFile("hop-ftp-test-", ".p12");
    Files.deleteIfExists(path);
    String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    Process process =
        new ProcessBuilder(
                keytool,
                "-genkeypair",
                "-alias",
                "hop-ftp-test",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-validity",
                "1",
                "-storetype",
                "PKCS12",
                "-keystore",
                path.toString(),
                "-storepass",
                KEYSTORE_PASSWORD,
                "-keypass",
                KEYSTORE_PASSWORD,
                "-dname",
                "CN=localhost, OU=Hop, O=Apache, L=Test, S=Test, C=US",
                "-ext",
                "SAN=DNS:localhost,IP:" + HOST,
                "-noprompt")
            .redirectErrorStream(true)
            .start();
    int exit = process.waitFor();
    if (exit != 0) {
      try (InputStream in = process.getInputStream()) {
        throw new IOException(
            "keytool failed (exit "
                + exit
                + "): "
                + new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
    return path;
  }
}
