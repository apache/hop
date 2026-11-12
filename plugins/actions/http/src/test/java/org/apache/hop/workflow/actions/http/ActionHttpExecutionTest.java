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

package org.apache.hop.workflow.actions.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class ActionHttpExecutionTest {
  private static final String HTTP_HOST = "localhost";
  private static final String HTTPS_HOST = "localhost";
  private static final String HTTPS_KEYSTORE_PASSWORD = "changeit";
  private static final String HEADER_NAME = "X-Test-Header";
  private static final String HEADER_VALUE = "header-value";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static HttpServer httpServer;
  private static HttpsServer httpsServer;
  private static Path httpsKeyStorePath;
  private static final AtomicInteger countedRequests = new AtomicInteger();

  @BeforeAll
  static void setupBeforeClass() throws Exception {
    HopClientEnvironment.init();
    ActionHttpExecutionTest.startHttpServer();
    ActionHttpExecutionTest.startHttpsServer();
  }

  @AfterAll
  static void tearDown() throws IOException {
    ActionHttpExecutionTest.stopHttpServer();
    ActionHttpExecutionTest.stopHttpsServer();
  }

  @Test
  void testHttpConfiguredSingleRunTracksDownloadAndAddsResultFile() throws Exception {
    String payload = "download only payload";
    File tempFileForDownload = File.createTempFile("downloadedFile3", ".tmp");
    tempFileForDownload.deleteOnExit();

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setAddFilenameToResult(true);
    http.setUrl(httpBaseUrl() + "/downloadFile");
    http.setTargetFilename(tempFileForDownload.getCanonicalPath());

    Result result = http.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertFalse(result.getResultFiles().isEmpty());
    assertEquals(payload, FileUtils.readFileToString(tempFileForDownload, UTF_8));
    assertEquals(payload.getBytes(UTF_8).length, result.getBytesReadThisAction());
    assertEquals(payload.getBytes(UTF_8).length, result.getBytesWrittenThisAction());
  }

  @Test
  void testHttpConfiguredSingleRunSetsHeaders() throws Exception {
    String payload = "header ok";
    File tempFileForDownload = File.createTempFile("downloadedHeader", ".tmp");
    tempFileForDownload.deleteOnExit();

    ActionHttp.Header header = new ActionHttp.Header();
    header.setHeaderName(HEADER_NAME);
    header.setHeaderValue(HEADER_VALUE);

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setAddFilenameToResult(false);
    http.setHeaders(List.of(header));
    http.setUrl(httpBaseUrl() + "/headerCheck");
    http.setTargetFilename(tempFileForDownload.getCanonicalPath());

    Result result = http.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(payload, FileUtils.readFileToString(tempFileForDownload, UTF_8));
  }

  @Test
  void testHttpConfiguredSingleRunWithMalformedUrlSetsError() throws Exception {
    File tempFileForDownload = File.createTempFile("downloadedMalformed", ".tmp");
    tempFileForDownload.deleteOnExit();

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setUrl("ht!tp://bad-url");
    http.setTargetFilename(tempFileForDownload.getCanonicalPath());

    Result result = http.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals(0, result.getBytesReadThisAction());
    assertEquals(0, result.getBytesWrittenThisAction());
  }

  @Test
  void testHttpConfiguredSingleRunWithIgnoreSslAcceptsSelfSignedCertificate() throws Exception {
    String payload = "https payload";
    File tempFileForDownload = File.createTempFile("downloadedHttps", ".tmp");
    tempFileForDownload.deleteOnExit();

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setAddFilenameToResult(false);
    http.setIgnoreSsl(true);
    http.setUrl(httpsBaseUrl() + "/downloadFile");
    http.setTargetFilename(tempFileForDownload.getCanonicalPath());

    Result result = http.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(payload, FileUtils.readFileToString(tempFileForDownload, UTF_8));
    assertEquals(payload.getBytes(UTF_8).length, result.getBytesReadThisAction());
    assertEquals(payload.getBytes(UTF_8).length, result.getBytesWrittenThisAction());
  }

  @Test
  void testHttpConfiguredSingleRunStoresReplyInVariable() throws Exception {
    String payload = "download only payload";
    File tempFileForDownload = File.createTempFile("downloadedFileVar", ".tmp");
    tempFileForDownload.deleteOnExit();

    LocalWorkflowEngine workflow = new LocalWorkflowEngine();
    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(workflow);
    http.setRunForEveryRow(false);
    http.setAddFilenameToResult(false);
    http.setUrl(httpBaseUrl() + "/downloadFile");
    http.setTargetFilename(tempFileForDownload.getCanonicalPath());
    http.setReplyVariableName("HTTP_REPLY");

    Result result = http.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(payload, FileUtils.readFileToString(tempFileForDownload, UTF_8));
    assertEquals(payload, http.getVariable("HTTP_REPLY"));
    assertEquals(payload, workflow.getVariable("HTTP_REPLY"));
  }

  @Test
  void testHttpConfiguredSingleRunDoesNotStoreReplyWhenVariableUnset() throws Exception {
    File tempFileForDownload = File.createTempFile("downloadedFileNoVar", ".tmp");
    tempFileForDownload.deleteOnExit();

    LocalWorkflowEngine workflow = new LocalWorkflowEngine();
    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(workflow);
    http.setRunForEveryRow(false);
    http.setAddFilenameToResult(false);
    http.setUrl(httpBaseUrl() + "/downloadFile");
    http.setTargetFilename(tempFileForDownload.getCanonicalPath());

    Result result = http.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertNull(http.getVariable("HTTP_REPLY"));
    assertNull(workflow.getVariable("HTTP_REPLY"));
  }

  @Test
  void testHttpErrorStatusLeavesAnExistingTargetFileAlone(@TempDir Path dir) throws Exception {
    Path target = dir.resolve("notFound.txt");
    Files.writeString(target, "previous content");

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setUrl(httpBaseUrl() + "/notFound");
    http.setTargetFilename(target.toString());

    Result result = http.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals("previous content", Files.readString(target));
    assertEquals(List.of(target), filesIn(dir), "no part file is left behind");
  }

  @Test
  void testHttpReplyThatBreaksOffLeavesAnExistingTargetFileAlone(@TempDir Path dir)
      throws Exception {
    Path target = dir.resolve("truncated.txt");
    Files.writeString(target, "previous content");

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setUrl(httpBaseUrl() + "/truncated");
    http.setTargetFilename(target.toString());

    Result result = http.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals("previous content", Files.readString(target));
    assertEquals(List.of(target), filesIn(dir), "no part file is left behind");
  }

  @Test
  void testHttpAppendAddsTheReplyToTheTargetFile(@TempDir Path dir) throws Exception {
    Path target = dir.resolve("append.txt");
    Files.writeString(target, "previous content|");

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setAddFilenameToResult(false);
    http.setFileAppended(true);
    http.setUrl(httpBaseUrl() + "/downloadFile");
    http.setTargetFilename(target.toString());

    Result result = http.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals("previous content|download only payload", Files.readString(target));
    assertEquals(List.of(target), filesIn(dir), "no part file is left behind");
  }

  @Test
  void testHttpAppendReplyThatBreaksOffLeavesTheTargetFileAlone(@TempDir Path dir)
      throws Exception {
    Path target = dir.resolve("appendTruncated.txt");
    Files.writeString(target, "previous content");

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setFileAppended(true);
    http.setUrl(httpBaseUrl() + "/truncated");
    http.setTargetFilename(target.toString());

    Result result = http.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals("previous content", Files.readString(target));
    assertEquals(List.of(target), filesIn(dir), "no part file is left behind");
  }

  @Test
  void testHttpMissingTargetFolderFailsBeforeTheRequestIsSent(@TempDir Path dir) throws Exception {
    int requestsBefore = countedRequests.get();

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(false);
    http.setUrl(httpBaseUrl() + "/countRequests");
    http.setTargetFilename(dir.resolve("missing").resolve("reply.txt").toString());

    Result result = http.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals(requestsBefore, countedRequests.get(), "the request must not have been sent");
  }

  @Test
  void testHttpForEveryRowFailsWhenALaterRowFails(@TempDir Path dir) throws Exception {
    Path first = dir.resolve("first.txt");
    Path second = dir.resolve("second.txt");

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("URL"));
    rowMeta.addValueMeta(new ValueMetaString("UPLOAD"));
    rowMeta.addValueMeta(new ValueMetaString("DESTINATION"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(
        new RowMetaAndData(
            rowMeta, new Object[] {httpBaseUrl() + "/downloadFile", null, first.toString()}));
    rows.add(
        new RowMetaAndData(
            rowMeta, new Object[] {httpBaseUrl() + "/notFound", null, second.toString()}));
    Result previousResult = new Result();
    previousResult.setRows(rows);

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(true);
    http.setAddFilenameToResult(false);

    Result result = http.execute(previousResult, 0);

    assertFalse(result.getResult(), "a failed row must fail the action");
    assertEquals(1, result.getNrErrors());
    assertEquals("download only payload", Files.readString(first));
    assertEquals(List.of(first), filesIn(dir), "a failed row must not create a target file");
  }

  @Test
  void testHttpForEveryRowWithoutRowsIsFalseWithoutErrors() {
    Result previousResult = new Result();
    previousResult.setRows(new ArrayList<>());

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow(new LocalWorkflowEngine());
    http.setRunForEveryRow(true);

    Result result = http.execute(previousResult, 0);

    assertFalse(result.getResult());
    assertEquals(0, result.getNrErrors());
  }

  private static List<Path> filesIn(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files.sorted().toList();
    }
  }

  private static void startHttpServer() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress(ActionHttpExecutionTest.HTTP_HOST, 0), 10);
    httpServer.createContext(
        "/downloadFile",
        httpExchange -> {
          byte[] response = "download only payload".getBytes(UTF_8);
          Headers h = httpExchange.getResponseHeaders();
          h.add("Content-Type", "application/octet-stream");
          httpExchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = httpExchange.getResponseBody()) {
            os.write(response);
            os.flush();
          }
          httpExchange.close();
        });
    httpServer.createContext(
        "/headerCheck",
        httpExchange -> {
          String headerValue = httpExchange.getRequestHeaders().getFirst(HEADER_NAME);
          byte[] response =
              (HEADER_VALUE.equals(headerValue) ? "header ok" : "missing header").getBytes(UTF_8);
          Headers h = httpExchange.getResponseHeaders();
          h.add("Content-Type", "text/plain");
          httpExchange.sendResponseHeaders(
              HEADER_VALUE.equals(headerValue) ? 200 : 400, response.length);
          try (OutputStream os = httpExchange.getResponseBody()) {
            os.write(response);
            os.flush();
          }
          httpExchange.close();
        });
    httpServer.createContext(
        "/notFound",
        httpExchange -> {
          httpExchange.sendResponseHeaders(404, -1);
          httpExchange.close();
        });
    httpServer.createContext(
        "/truncated",
        httpExchange -> {
          // Promise 1000 bytes, send a few and hang up: the client sees the reply break off.
          httpExchange.sendResponseHeaders(200, 1000);
          try {
            OutputStream os = httpExchange.getResponseBody();
            os.write("partial".getBytes(UTF_8));
            os.flush();
          } finally {
            httpExchange.close();
          }
        });
    httpServer.createContext(
        "/countRequests",
        httpExchange -> {
          countedRequests.incrementAndGet();
          httpExchange.sendResponseHeaders(200, -1);
          httpExchange.close();
        });
    httpServer.start();
  }

  private static void startHttpsServer() throws Exception {
    httpsKeyStorePath = Files.createTempFile("action-http-execution-test", ".p12");
    Files.deleteIfExists(httpsKeyStorePath);
    generateHttpsKeyStore(httpsKeyStorePath);

    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream inputStream = Files.newInputStream(httpsKeyStorePath)) {
      keyStore.load(inputStream, HTTPS_KEYSTORE_PASSWORD.toCharArray());
    }

    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, HTTPS_KEYSTORE_PASSWORD.toCharArray());

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());

    httpsServer = HttpsServer.create(new InetSocketAddress(HTTPS_HOST, 0), 10);
    httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    httpsServer.createContext(
        "/downloadFile",
        httpExchange -> {
          byte[] response = "https payload".getBytes(UTF_8);
          Headers h = httpExchange.getResponseHeaders();
          h.add("Content-Type", "application/octet-stream");
          httpExchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = httpExchange.getResponseBody()) {
            os.write(response);
            os.flush();
          }
          httpExchange.close();
        });
    httpsServer.start();
  }

  private static void generateHttpsKeyStore(Path keyStorePath)
      throws IOException, InterruptedException {
    Process process =
        new ProcessBuilder(
                keytoolExecutable(),
                "-genkeypair",
                "-alias",
                "hop-test",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-validity",
                "1",
                "-storetype",
                "PKCS12",
                "-keystore",
                keyStorePath.toString(),
                "-storepass",
                HTTPS_KEYSTORE_PASSWORD,
                "-keypass",
                HTTPS_KEYSTORE_PASSWORD,
                "-dname",
                "CN=localhost, OU=Hop, O=Apache, L=Test, S=Test, C=US",
                "-noprompt")
            .redirectErrorStream(true)
            .start();

    int exitCode = process.waitFor();
    if (exitCode != 0) {
      String output = new String(process.getInputStream().readAllBytes(), UTF_8);
      throw new IOException("Unable to generate HTTPS test keystore: " + output);
    }
  }

  private static String keytoolExecutable() {
    return Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
  }

  private static String httpBaseUrl() {
    return "http://" + HTTP_HOST + ":" + httpServer.getAddress().getPort();
  }

  private static String httpsBaseUrl() {
    return "https://" + HTTPS_HOST + ":" + httpsServer.getAddress().getPort();
  }

  private static void stopHttpServer() {
    if (httpServer != null) {
      httpServer.stop(2);
    }
  }

  private static void stopHttpsServer() throws IOException {
    if (httpsServer != null) {
      httpsServer.stop(2);
    }
    if (httpsKeyStorePath != null) {
      Files.deleteIfExists(httpsKeyStorePath);
    }
  }
}
