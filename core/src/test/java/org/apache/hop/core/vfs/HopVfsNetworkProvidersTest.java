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
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for the network VFS providers Hop core registers by default: {@code http} and
 * {@code https}. Each is exercised against an embedded server so we don't need external resources.
 *
 * <p>{@code ftp}, {@code ftps} and {@code sftp} are served by their technology plugins and proved
 * there, in {@code org.apache.hop.vfs.ftp.HopVfsFtpSchemeTest} and {@code
 * org.apache.hop.vfs.sftp.HopVfsSftpSchemeTest}.
 */
class HopVfsNetworkProvidersTest {

  private static final String KEYSTORE_PASSWORD = "hop-test-pass";
  private static String LOCALHOST;

  private static HttpServer httpServer;
  private static int httpPort;

  private static HttpsServer httpsServer;
  private static int httpsPort;
  private static Path keyStorePath;
  private static String previousTrustStoreProperty;
  private static String previousTrustStorePasswordProperty;

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

    // ftp://, ftps:// and sftp:// are served by their technology plugins, not by core - see
    // org.apache.hop.vfs.ftp.HopVfsFtpSchemeTest and org.apache.hop.vfs.sftp.HopVfsSftpSchemeTest.
  }

  @AfterAll
  static void stopServers() throws Exception {
    if (httpServer != null) httpServer.stop(0);
    if (httpsServer != null) httpsServer.stop(0);
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

  // --- helpers ---------------------------------------------------------------------------

  private static String readToString(String url) throws Exception {
    try (InputStream in = HopVfs.getFileObject(url).getContent().getInputStream()) {
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
