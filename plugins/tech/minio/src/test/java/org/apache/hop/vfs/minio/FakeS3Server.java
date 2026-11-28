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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.minio.metadata.MinioMeta;

/**
 * The few S3 calls the MinIO VFS provider makes, on one bucket of objects kept in memory, with a
 * flat key space as on AWS: a folder is a marker object whose key ends in {@code /}. Every answer
 * that carries a timestamp writes {@link #lastModified} as given, so a test can answer the way
 * SeaweedFS does. Authentication is not checked.
 */
final class FakeS3Server {
  private static final String S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/";
  private static final DateTimeFormatter HTTP_DATE =
      DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC);
  private static final Pattern DELETE_KEY = Pattern.compile("<Key>(.*?)</Key>");

  final Map<String, byte[]> objects = new ConcurrentHashMap<>();

  /** The prefix of every listing, {@code ""} for the top level of the bucket. */
  final List<String> listedPrefixes = new CopyOnWriteArrayList<>();

  /** Requests this server does not serve. */
  final List<String> unexpected = new CopyOnWriteArrayList<>();

  volatile String lastModified = "2026-11-27T17:44:37.450Z";

  private final String bucket;
  private final HttpServer server;

  FakeS3Server(String bucket) throws IOException {
    this.bucket = bucket;
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  /** A file system manager with a MinIO connection named {@code scheme} to this server. */
  DefaultFileSystemManager fileSystemManager(String scheme) throws FileSystemException {
    MinioMeta meta = new MinioMeta();
    meta.setName(scheme);
    meta.setEndPointHostname(server.getAddress().getHostString());
    meta.setEndPointPort(Integer.toString(server.getAddress().getPort()));
    meta.setEndPointSecure(false);
    meta.setAccessKey("access");
    meta.setSecretKey("secret");
    meta.setRegion("us-east-1");

    DefaultFileSystemManager manager = new DefaultFileSystemManager();
    manager.addProvider(scheme, new MinioFileProvider(new Variables(), meta));
    manager.init();
    return manager;
  }

  void stop() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    try (exchange) {
      byte[] body = exchange.getRequestBody().readAllBytes();
      String method = exchange.getRequestMethod();
      String path =
          URLDecoder.decode(exchange.getRequestURI().getRawPath(), StandardCharsets.UTF_8);
      String query = exchange.getRequestURI().getRawQuery();

      if (!path.startsWith("/" + bucket)) {
        reject(exchange, method + " " + path);
        return;
      }
      String key = path.length() > bucket.length() + 2 ? path.substring(bucket.length() + 2) : "";

      if (key.isEmpty()) {
        if ("HEAD".equals(method)) {
          exchange.sendResponseHeaders(200, -1);
        } else if ("GET".equals(method) && query != null && query.contains("list-type=2")) {
          sendXml(
              exchange,
              listBucketResult(
                  queryParameter(query, "prefix"), queryParameter(query, "delimiter")));
        } else if ("POST".equals(method) && query != null && query.startsWith("delete")) {
          deleteObjects(exchange, new String(body, StandardCharsets.UTF_8));
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

  private void copyObject(HttpExchange exchange, String copySource, String key) throws IOException {
    String source = URLDecoder.decode(copySource, StandardCharsets.UTF_8);
    source = source.startsWith("/") ? source.substring(1) : source;
    if (!source.startsWith(bucket + "/")
        || !objects.containsKey(source.substring(bucket.length() + 1))) {
      reject(exchange, "copy from unknown source " + copySource);
      return;
    }
    objects.put(key, objects.get(source.substring(bucket.length() + 1)));
    sendXml(
        exchange,
        "<CopyObjectResult xmlns=\""
            + S3_NAMESPACE
            + "\"><LastModified>"
            + lastModified
            + "</LastModified><ETag>\"etag\"</ETag></CopyObjectResult>");
  }

  /** The S3 multi-object delete: every key in the request goes. */
  private void deleteObjects(HttpExchange exchange, String request) throws IOException {
    Matcher matcher = DELETE_KEY.matcher(request);
    while (matcher.find()) {
      objects.remove(matcher.group(1));
    }
    sendXml(exchange, "<DeleteResult xmlns=\"" + S3_NAMESPACE + "\"></DeleteResult>");
  }

  /** ListObjectsV2: with a delimiter, keys below the next one fold into a common prefix. */
  private String listBucketResult(String prefix, String delimiter) {
    String effectivePrefix = prefix == null ? "" : prefix;
    listedPrefixes.add(effectivePrefix);
    boolean folds = delimiter != null && !delimiter.isEmpty();
    StringBuilder contents = new StringBuilder();
    TreeSet<String> commonPrefixes = new TreeSet<>();
    for (Map.Entry<String, byte[]> entry : new TreeMap<>(objects).entrySet()) {
      String name = entry.getKey();
      if (!name.startsWith(effectivePrefix)) {
        continue;
      }
      int next = folds ? name.indexOf(delimiter, effectivePrefix.length()) : -1;
      if (next >= 0) {
        commonPrefixes.add(name.substring(0, next + delimiter.length()));
        continue;
      }
      contents
          .append("<Contents><Key>")
          .append(name)
          .append("</Key><LastModified>")
          .append(lastModified)
          .append("</LastModified><ETag>\"etag\"</ETag><Size>")
          .append(entry.getValue().length)
          .append("</Size><StorageClass>STANDARD</StorageClass></Contents>");
    }
    StringBuilder prefixes = new StringBuilder();
    for (String commonPrefix : commonPrefixes) {
      prefixes
          .append("<CommonPrefixes><Prefix>")
          .append(commonPrefix)
          .append("</Prefix></CommonPrefixes>");
    }
    return "<ListBucketResult xmlns=\""
        + S3_NAMESPACE
        + "\"><Name>"
        + bucket
        + "</Name><Prefix>"
        + effectivePrefix
        + "</Prefix><MaxKeys>1000</MaxKeys>"
        + (folds ? "<Delimiter>" + delimiter + "</Delimiter>" : "")
        + "<IsTruncated>false</IsTruncated>"
        + contents
        + prefixes
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
