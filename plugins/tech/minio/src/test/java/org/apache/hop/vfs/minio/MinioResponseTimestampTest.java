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

package org.apache.hop.vfs.minio;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.vfs.minio.metadata.MinioMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The timestamps S3-compatible stores put in their XML answers. AWS and MinIO always write three
 * fraction digits; SeaweedFS drops trailing zeros ({@code .45Z} for {@code .450}, {@code .4Z} for
 * {@code .400}, no fraction for {@code .000}). A rename (a {@code CopyObject}) and a listing read
 * such a timestamp, and both must work whatever the number of digits. See issue #9760.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class MinioResponseTimestampTest {

  private static final String SCHEME = "staging";
  private static final String BUCKET = "hop-staging";
  private static final byte[] CONTENT = "hop-spark-package".getBytes(StandardCharsets.UTF_8);

  private FakeS3 s3;
  private DefaultFileSystemManager manager;

  @BeforeEach
  void setUp() throws Exception {
    s3 = new FakeS3();

    MinioMeta meta = new MinioMeta();
    meta.setName(SCHEME);
    meta.setEndPointHostname(s3.host());
    meta.setEndPointPort(Integer.toString(s3.port()));
    meta.setEndPointSecure(false);
    meta.setAccessKey("access");
    meta.setSecretKey("secret");
    meta.setRegion("us-east-1");

    manager = new DefaultFileSystemManager();
    manager.addProvider(SCHEME, new MinioFileProvider(new Variables(), meta));
    manager.init();
  }

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
    }
    if (s3 != null) {
      s3.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "2026-11-27T17:44:37.450Z",
        "2026-11-27T17:44:37.45Z",
        "2026-11-27T17:44:37.4Z",
        "2026-11-27T17:44:37Z",
        "2026-11-27T17:44:37.450123Z"
      })
  void renameReadsTheCopyAnswerWhateverTheFractionDigits(String lastModified) throws Exception {
    s3.lastModified = lastModified;
    s3.objects.put("package.zip.uploading-1", CONTENT);

    FileObject partial = manager.resolveFile(SCHEME + ":///" + BUCKET + "/package.zip.uploading-1");
    FileObject target = manager.resolveFile(SCHEME + ":///" + BUCKET + "/package.zip");
    partial.moveTo(target);

    assertArrayEquals(CONTENT, s3.objects.get("package.zip"), "renamed object");
    assertFalse(s3.objects.containsKey("package.zip.uploading-1"), "source removed");
    assertTrue(s3.unexpected.isEmpty(), "unexpected requests: " + s3.unexpected);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "2026-11-27T17:44:37.450Z",
        "2026-11-27T17:44:37.45Z",
        "2026-11-27T17:44:37.4Z",
        "2026-11-27T17:44:37Z",
        "2026-11-27T17:44:37.450123Z"
      })
  void listingReadsTheLastModifiedWhateverTheFractionDigits(String lastModified) throws Exception {
    s3.lastModified = lastModified;
    s3.objects.put("a.zip", CONTENT);
    s3.objects.put("b.zip", CONTENT);

    FileObject folder = manager.resolveFile(SCHEME + ":///" + BUCKET);
    FileObject[] children = folder.getChildren();

    List<String> names = new ArrayList<>();
    for (FileObject child : children) {
      names.add(child.getName().getBaseName());
      assertEquals(
          Instant.parse(lastModified).toEpochMilli(),
          child.getContent().getLastModifiedTime(),
          "last modified of " + child.getName().getBaseName());
      assertEquals(CONTENT.length, child.getContent().getSize());
    }
    assertEquals(List.of("a.zip", "b.zip"), names);
    assertTrue(s3.unexpected.isEmpty(), "unexpected requests: " + s3.unexpected);
  }

  /**
   * The few S3 calls a rename and a listing make, on one bucket, answered with the configured
   * {@code LastModified} text the way SeaweedFS writes it. Authentication is not checked.
   */
  private static final class FakeS3 {
    private static final String S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/";
    private static final DateTimeFormatter HTTP_DATE =
        DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC);

    final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    final List<String> unexpected = new CopyOnWriteArrayList<>();
    volatile String lastModified;

    private final HttpServer server;

    FakeS3() throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext("/", this::handle);
      server.start();
    }

    String host() {
      return server.getAddress().getHostString();
    }

    int port() {
      return server.getAddress().getPort();
    }

    void stop() {
      server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
      try (exchange) {
        exchange.getRequestBody().readAllBytes();
        String method = exchange.getRequestMethod();
        String path =
            URLDecoder.decode(exchange.getRequestURI().getRawPath(), StandardCharsets.UTF_8);
        String query = exchange.getRequestURI().getRawQuery();

        if (!path.startsWith("/" + BUCKET)) {
          reject(exchange, method + " " + path);
          return;
        }
        String key = path.length() > BUCKET.length() + 2 ? path.substring(BUCKET.length() + 2) : "";

        if (key.isEmpty()) {
          if ("HEAD".equals(method)) {
            exchange.sendResponseHeaders(200, -1);
          } else if ("GET".equals(method) && query != null && query.contains("list-type=2")) {
            sendXml(exchange, listBucketResult(queryParameter(query, "prefix")));
          } else {
            reject(exchange, method + " " + path + "?" + query);
          }
          return;
        }

        switch (method) {
          case "HEAD" -> headObject(exchange, key);
          case "GET" -> getObject(exchange, key);
          case "PUT" -> {
            String copySource = exchange.getRequestHeaders().getFirst("x-amz-copy-source");
            if (copySource == null) {
              reject(exchange, "PUT " + path + " (only copies are served)");
            } else {
              copyObject(exchange, copySource, key);
            }
          }
          case "DELETE" -> {
            objects.remove(key);
            exchange.sendResponseHeaders(204, -1);
          }
          default -> reject(exchange, method + " " + path);
        }
      }
    }

    private void headObject(HttpExchange exchange, String key) throws IOException {
      byte[] content = objects.get(key);
      if (content == null) {
        exchange.sendResponseHeaders(404, -1);
        return;
      }
      exchange.getResponseHeaders().set("Content-Length", Integer.toString(content.length));
      exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
      exchange.getResponseHeaders().set("ETag", "\"etag\"");
      // Response headers carry an HTTP date, which minio-java reads with its own parser
      exchange
          .getResponseHeaders()
          .set("Last-Modified", HTTP_DATE.format(Instant.parse(lastModified)));
      exchange.sendResponseHeaders(200, -1);
    }

    private void getObject(HttpExchange exchange, String key) throws IOException {
      byte[] content = objects.get(key);
      if (content == null) {
        exchange.sendResponseHeaders(404, -1);
        return;
      }
      exchange.getResponseHeaders().set("ETag", "\"etag\"");
      exchange.sendResponseHeaders(200, content.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(content);
      }
    }

    private void copyObject(HttpExchange exchange, String copySource, String key)
        throws IOException {
      String source = URLDecoder.decode(copySource, StandardCharsets.UTF_8);
      source = source.startsWith("/") ? source.substring(1) : source;
      if (!source.startsWith(BUCKET + "/")
          || !objects.containsKey(source.substring(BUCKET.length() + 1))) {
        reject(exchange, "copy from unknown source " + copySource);
        return;
      }
      objects.put(key, objects.get(source.substring(BUCKET.length() + 1)));
      sendXml(
          exchange,
          "<CopyObjectResult xmlns=\""
              + S3_NAMESPACE
              + "\"><LastModified>"
              + lastModified
              + "</LastModified><ETag>\"etag\"</ETag></CopyObjectResult>");
    }

    private String listBucketResult(String prefix) {
      String effectivePrefix = prefix == null ? "" : prefix;
      Map<String, byte[]> sorted = new TreeMap<>(objects);
      StringBuilder contents = new StringBuilder();
      int count = 0;
      for (Map.Entry<String, byte[]> entry : sorted.entrySet()) {
        if (!entry.getKey().startsWith(effectivePrefix)
            || entry.getKey().indexOf('/', effectivePrefix.length()) >= 0) {
          continue;
        }
        count++;
        contents
            .append("<Contents><Key>")
            .append(entry.getKey())
            .append("</Key><LastModified>")
            .append(lastModified)
            .append("</LastModified><ETag>\"etag\"</ETag><Size>")
            .append(entry.getValue().length)
            .append("</Size><StorageClass>STANDARD</StorageClass></Contents>");
      }
      return "<ListBucketResult xmlns=\""
          + S3_NAMESPACE
          + "\"><Name>"
          + BUCKET
          + "</Name><Prefix>"
          + effectivePrefix
          + "</Prefix><KeyCount>"
          + count
          + "</KeyCount><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter>"
          + "<IsTruncated>false</IsTruncated>"
          + contents
          + "</ListBucketResult>";
    }

    private static String queryParameter(String query, String name) {
      for (String pair : query.split("&")) {
        int equals = pair.indexOf('=');
        String pairName = equals < 0 ? pair : pair.substring(0, equals);
        if (pairName.equals(name)) {
          return equals < 0
              ? ""
              : URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8);
        }
      }
      return null;
    }

    private static void sendXml(HttpExchange exchange, String body) throws IOException {
      byte[] bytes =
          ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + body).getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/xml");
      exchange.sendResponseHeaders(200, bytes.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    }

    private void reject(HttpExchange exchange, String request) throws IOException {
      unexpected.add(request);
      exchange.sendResponseHeaders(501, -1);
    }
  }
}
