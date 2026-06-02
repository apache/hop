/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Collections;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.AcceptAllPasswordAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trip tests for the network VFS providers Hop registers by default: {@code http}, {@code
 * https}, {@code ftp}, {@code ftps}, {@code sftp}. Each is exercised against an embedded server so
 * we don't need external resources.
 */
class HopVfsNetworkProvidersTest {

  private static final String KEYSTORE_PASSWORD = "hop-test-pass";
  private static final String FTP_USER = "tester";
  private static final String FTP_PASS = "secret";
  private static final String SFTP_USER = "alice";
  private static final String SFTP_PASS = "secret";
  private static String LOCALHOST;

  @TempDir static Path sharedRoot;

  private static HttpServer httpServer;
  private static int httpPort;

  private static HttpsServer httpsServer;
  private static int httpsPort;
  private static Path keyStorePath;
  private static String previousTrustStoreProperty;
  private static String previousTrustStorePasswordProperty;

  private static FtpServer ftpServer;
  private static Listener ftpListener;
  private static int ftpPort;

  private static FtpServer ftpsServer;
  private static Listener ftpsListener;
  private static int ftpsPort;

  private static SshServer sshServer;
  private static int sftpPort;

  static {
    try {
      LOCALHOST = InetAddress.getLocalHost().getHostAddress();
    } catch (final UnknownHostException e) {
      fail(e);
    }
  }

  @BeforeAll
  static void startServers() throws Exception {
    keyStorePath = generateTestKeyStore();

    // HTTP
    httpServer = HttpServer.create(new InetSocketAddress(LOCALHOST, 0), 0);
    httpPort = httpServer.getAddress().getPort();
    httpServer.createContext("/payload.txt", new FixedPayloadHandler("http-payload"));
    httpServer.start();

    // HTTPS
    SSLContext sslContext = buildServerSslContext(keyStorePath);
    httpsServer = HttpsServer.create(new InetSocketAddress(LOCALHOST, 0), 0);
    httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    httpsServer.createContext("/secure.txt", new FixedPayloadHandler("https-payload"));
    httpsServer.start();
    httpsPort = httpsServer.getAddress().getPort();

    // Make Hop's HTTPS provider trust the self-signed cert via the JVM-wide trust store.
    previousTrustStoreProperty = System.getProperty("javax.net.ssl.trustStore");
    previousTrustStorePasswordProperty = System.getProperty("javax.net.ssl.trustStorePassword");
    System.setProperty("javax.net.ssl.trustStore", keyStorePath.toString());
    System.setProperty("javax.net.ssl.trustStorePassword", KEYSTORE_PASSWORD);

    // FTP
    Path ftpHome = sharedRoot.resolve("ftp");
    Files.createDirectories(ftpHome);
    Files.writeString(ftpHome.resolve("greeting.txt"), "ftp-payload");
    FtpServerStart ftp = startFtp(ftpHome, false);
    ftpServer = ftp.server;
    ftpListener = ftp.listener;
    ftpPort = ftpListener.getPort();

    // FTPS
    Path ftpsHome = sharedRoot.resolve("ftps");
    Files.createDirectories(ftpsHome);
    Files.writeString(ftpsHome.resolve("greeting.txt"), "ftps-payload");
    FtpServerStart ftps = startFtp(ftpsHome, true);
    ftpsServer = ftps.server;
    ftpsListener = ftps.listener;
    ftpsPort = ftpsListener.getPort();

    // SFTP
    Path sftpHome = sharedRoot.resolve("sftp");
    Files.createDirectories(sftpHome);
    Files.writeString(sftpHome.resolve("greeting.txt"), "sftp-payload");
    sshServer = startSftp(sftpHome);
    sftpPort = sshServer.getPort();
  }

  @AfterAll
  static void stopServers() throws Exception {
    if (httpServer != null) httpServer.stop(0);
    if (httpsServer != null) httpsServer.stop(0);
    if (ftpServer != null) ftpServer.stop();
    if (ftpsServer != null) ftpsServer.stop();
    if (sshServer != null) sshServer.stop();
    restoreSystemProperty("javax.net.ssl.trustStore", previousTrustStoreProperty);
    restoreSystemProperty("javax.net.ssl.trustStorePassword", previousTrustStorePasswordProperty);
    if (keyStorePath != null) Files.deleteIfExists(keyStorePath);
  }

  @Test
  @DisplayName("http:// fetches a payload from an embedded HttpServer")
  void httpProviderReadsFromEmbeddedServer() throws Exception {
    assertEquals(
        "http-payload", readToString("http://" + LOCALHOST + ":" + httpPort + "/payload.txt"));
  }

  @Test
  @DisplayName("https:// fetches a payload over TLS from an embedded HttpsServer")
  void httpsProviderReadsFromEmbeddedServer() throws Exception {
    assertEquals(
        "https-payload", readToString("https://" + LOCALHOST + ":" + httpsPort + "/secure.txt"));
  }

  @Test
  @DisplayName("ftp:// fetches a payload from an embedded Apache FtpServer")
  void ftpProviderReadsFromEmbeddedServer() throws Exception {
    String url =
        "ftp://" + FTP_USER + ":" + FTP_PASS + "@" + LOCALHOST + ":" + ftpPort + "/greeting.txt";
    FileSystemOptions opts = new FileSystemOptions();
    FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
    assertEquals("ftp-payload", readWithOptions(url, opts));
  }

  @Test
  @DisplayName("ftps:// fetches a payload over TLS from an embedded Apache FtpServer")
  void ftpsProviderReadsFromEmbeddedServer() throws Exception {
    String url =
        "ftps://" + FTP_USER + ":" + FTP_PASS + "@" + LOCALHOST + ":" + ftpsPort + "/greeting.txt";
    FileSystemOptions opts = new FileSystemOptions();
    FtpsFileSystemConfigBuilder ftps = FtpsFileSystemConfigBuilder.getInstance();
    ftps.setPassiveMode(opts, true);
    ftps.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.P);
    assertEquals("ftps-payload", readWithOptions(url, opts));
  }

  @Test
  @DisplayName("sftp:// fetches a payload from an embedded Apache MINA SSHD server")
  void sftpProviderReadsFromEmbeddedServer() throws Exception {
    String url =
        "sftp://"
            + SFTP_USER
            + ":"
            + SFTP_PASS
            + "@"
            + LOCALHOST
            + ":"
            + sftpPort
            + "/greeting.txt";
    assertEquals("sftp-payload", readToString(url));
  }

  // --- helpers ---------------------------------------------------------------------------

  private static String readToString(String url) throws Exception {
    try (InputStream in = HopVfs.getFileObject(url).getContent().getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static String readWithOptions(String url, FileSystemOptions opts) throws Exception {
    FileObject obj = HopVfs.getFileSystemManager().resolveFile(url, opts);
    try (InputStream in = obj.getContent().getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** Generates a fresh PKCS12 keystore containing a self-signed cert for CN=localhost. */
  private static Path generateTestKeyStore() throws Exception {
    Path path = Files.createTempFile("hopvfs-network-", ".p12");
    Files.deleteIfExists(path);
    String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    Process process =
        new ProcessBuilder(
                keytool,
                "-genkeypair",
                "-alias",
                "hopvfs-test",
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
                "SAN=DNS:localhost,IP:" + LOCALHOST,
                "-noprompt")
            .redirectErrorStream(true)
            .start();
    int exit = process.waitFor();
    if (exit != 0) {
      String stderr = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      throw new IOException("keytool failed (exit " + exit + "): " + stderr);
    }
    return path;
  }

  private static SSLContext buildServerSslContext(Path ks) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream in = Files.newInputStream(ks)) {
      keyStore.load(in, KEYSTORE_PASSWORD.toCharArray());
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, KEYSTORE_PASSWORD.toCharArray());

    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(keyStore);

    SSLContext ctx = SSLContext.getInstance("TLS");
    ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return ctx;
  }

  /** Holder so callers can recover the dynamic port from the {@link Listener}. */
  private record FtpServerStart(FtpServer server, Listener listener) {}

  private static FtpServerStart startFtp(Path home, boolean tls) throws Exception {
    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory listenerFactory = new ListenerFactory();
    listenerFactory.setPort(0);
    if (tls) {
      SslConfigurationFactory ssl = new SslConfigurationFactory();
      ssl.setKeystoreFile(keyStorePath.toFile());
      ssl.setKeystorePassword(KEYSTORE_PASSWORD);
      ssl.setKeyPassword(KEYSTORE_PASSWORD);
      listenerFactory.setSslConfiguration(ssl.createSslConfiguration());
      // Explicit FTPS (AUTH TLS) — matches commons-vfs2's default FtpsMode.EXPLICIT.
      listenerFactory.setImplicitSsl(false);
    }
    Listener listener = listenerFactory.createListener();
    serverFactory.addListener("default", listener);

    PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
    BaseUser user = new BaseUser();
    user.setName(FTP_USER);
    user.setPassword(FTP_PASS);
    user.setHomeDirectory(home.toString());
    user.setAuthorities(Collections.singletonList(new WritePermission()));
    serverFactory.setUserManager(userManagerFactory.createUserManager());
    serverFactory.getUserManager().save(user);

    FtpServer server = serverFactory.createServer();
    server.start();
    return new FtpServerStart(server, listener);
  }

  private static SshServer startSftp(Path home) throws IOException {
    SshServer sshd = SshServer.setUpDefaultServer();
    sshd.setHost(LOCALHOST);
    sshd.setPort(0);
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(sharedRoot.resolve("hostkey.ser")));
    sshd.setPasswordAuthenticator(AcceptAllPasswordAuthenticator.INSTANCE);
    sshd.setFileSystemFactory(new VirtualFileSystemFactory(home));
    sshd.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
    // commons-vfs2's SFTP client doesn't actively close the session when the FileObject
    // is closed; the server's default 10-minute IDLE_TIMEOUT would otherwise hold the
    // test open. Drop it to a few seconds so the read returns promptly.
    org.apache.sshd.core.CoreModuleProperties.IDLE_TIMEOUT.set(
        sshd, java.time.Duration.ofSeconds(5));
    sshd.start();
    return sshd;
  }

  private static void restoreSystemProperty(String key, String previousValue) {
    if (previousValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, previousValue);
    }
  }

  /** Sends a fixed string body with a 200 response. */
  private static final class FixedPayloadHandler implements com.sun.net.httpserver.HttpHandler {
    private final byte[] body;

    FixedPayloadHandler(String body) {
      this.body = body.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
      exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
      exchange.close();
    }
  }
}
