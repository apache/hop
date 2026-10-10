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

package org.apache.hop.vfs.gs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.hop.vfs.gs.config.GoogleCloudConfigSingleton;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Google Cloud Storage provider against an in-process fake of the JSON API: object names are
 * flat, a "folder" is only a name prefix, and a missing object is a 404. Runs through a real file
 * system manager with the ON_RESOLVE cache strategy Hop uses, and the client Hop builds.
 */
class GoogleStorageFileObjectStoreSemanticsTest {

  private static final String BUCKET = "bucket";

  /** The bucket's objects: name to content. */
  private final TreeMap<String, byte[]> objects = new TreeMap<>();

  /** Requests the fake couldn't answer, to show what a failing test needed. */
  private final List<String> unhandled = new ArrayList<>();

  private HttpServer server;
  private DefaultFileSystemManager manager;

  /**
   * The bucket, on the file system that holds the fake client. Every file is resolved from it and
   * checked to stay there: any other file system would build a real client from the machine's
   * credentials.
   */
  private FileObject bucket;

  @BeforeEach
  void setUp() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
    manager = new DefaultFileSystemManager();
    manager.setFilesCache(new SoftRefFilesCache());
    manager.setCacheStrategy(CacheStrategy.ON_RESOLVE);
    manager.addProvider("gs", new GoogleStorageFileProvider());
    manager.init();
    Storage storage =
        GoogleStorageFileSystem.buildStorageOptions(GoogleCloudConfigSingleton.getConfig())
            .setHost("http://127.0.0.1:" + server.getAddress().getPort())
            .setProjectId("hop-test")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();
    bucket = manager.resolveFile("gs://" + BUCKET);
    ((GoogleStorageFileSystem) bucket.getFileSystem()).useStorage(storage);
  }

  @AfterEach
  void tearDown() {
    manager.close();
    server.stop(0);
  }

  // ---------------------------------------------------------------- the fake JSON API

  private void handle(HttpExchange exchange) throws IOException {
    try (exchange) {
      String path = exchange.getRequestURI().getRawPath();
      Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
      String bucketPrefix = "/b/" + BUCKET;
      int at = path.indexOf(bucketPrefix);
      if (at < 0 || !"GET".equals(exchange.getRequestMethod())) {
        unhandled(exchange);
        return;
      }
      String rest = path.substring(at + bucketPrefix.length());
      if (rest.isEmpty()) {
        json(exchange, 200, "{\"kind\":\"storage#bucket\",\"name\":\"" + BUCKET + "\"}");
      } else if (rest.equals("/o")) {
        json(exchange, 200, list(query.getOrDefault("prefix", ""), query.get("delimiter")));
      } else if (rest.startsWith("/o/")) {
        String name = decode(rest.substring(3));
        byte[] content = objects.get(name);
        if (content == null) {
          json(exchange, 404, "{\"error\":{\"code\":404,\"message\":\"Not Found\"}}");
        } else if ("media".equals(query.get("alt"))) {
          media(exchange, content);
        } else {
          json(exchange, 200, object(name, content));
        }
      } else {
        unhandled(exchange);
      }
    }
  }

  private void unhandled(HttpExchange exchange) throws IOException {
    unhandled.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
    json(exchange, 501, "{\"error\":{\"code\":501,\"message\":\"Not in the fake\"}}");
  }

  private static Map<String, String> query(String raw) {
    Map<String, String> query = new HashMap<>();
    if (raw != null) {
      for (String pair : raw.split("&")) {
        int eq = pair.indexOf('=');
        if (eq > 0) {
          query.put(decode(pair.substring(0, eq)), decode(pair.substring(eq + 1)));
        }
      }
    }
    return query;
  }

  private static String decode(String value) {
    return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
  }

  private static String quote(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static String object(String name, byte[] content) {
    return "{\"kind\":\"storage#object\",\"bucket\":"
        + quote(BUCKET)
        + ",\"name\":"
        + quote(name)
        + ",\"size\":\""
        + content.length
        + "\",\"generation\":\"1\",\"metageneration\":\"1\"}";
  }

  /** Objects.list with a delimiter: direct objects as items, deeper names as prefixes. */
  private String list(String prefix, String delimiter) {
    List<String> items = new ArrayList<>();
    TreeSet<String> prefixes = new TreeSet<>();
    for (Map.Entry<String, byte[]> entry : objects.entrySet()) {
      String name = entry.getKey();
      if (!name.startsWith(prefix)) {
        continue;
      }
      int slash = delimiter == null ? -1 : name.indexOf(delimiter, prefix.length());
      if (slash >= 0) {
        prefixes.add(quote(name.substring(0, slash + 1)));
      } else {
        items.add(object(name, entry.getValue()));
      }
    }
    return "{\"kind\":\"storage#objects\",\"items\":["
        + String.join(",", items)
        + "],\"prefixes\":["
        + String.join(",", prefixes)
        + "]}";
  }

  private static void media(HttpExchange exchange, byte[] content) throws IOException {
    int from = 0;
    int to = content.length;
    String range = exchange.getRequestHeaders().getFirst("Range");
    if (range != null && range.startsWith("bytes=")) {
      String[] parts = range.substring(6).split("-", -1);
      from = Math.min(Integer.parseInt(parts[0]), content.length);
      if (parts.length > 1 && !parts[1].isEmpty()) {
        to = Math.min(Integer.parseInt(parts[1]) + 1, content.length);
      }
    }
    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
    exchange.getResponseHeaders().add("x-goog-generation", "1");
    exchange.sendResponseHeaders(range == null ? 200 : 206, to - from == 0 ? -1 : to - from);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(content, from, to - from);
    }
  }

  private static void json(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  // ---------------------------------------------------------------- tests

  private void put(String name, String content) {
    objects.put(name, content.getBytes(StandardCharsets.UTF_8));
  }

  private FileObject resolve(String path) throws FileSystemException {
    FileObject file = bucket.resolveFile(path);
    assertSame(bucket.getFileSystem(), file.getFileSystem(), "must stay on the fake client");
    return file;
  }

  private static List<String> names(FileObject[] children) throws FileSystemException {
    List<String> names = new ArrayList<>();
    for (FileObject child : children) {
      // Base names keep VFS's %nn escapes, as they do for local files
      names.add(UriParser.decode(child.getName().getBaseName()));
    }
    return names;
  }

  private static String read(FileObject file) throws IOException {
    try (InputStream in = file.getContent().getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  void listsAndReadsObjectsWithAPercentInTheirName() throws Exception {
    put("data/file%.txt", "percent");
    put("data/100% sure/inner.txt", "inner");

    FileObject[] children = resolve("data").getChildren();

    assertEquals(
        new TreeSet<>(List.of("file%.txt", "100% sure")),
        new TreeSet<>(names(children)),
        unhandled.toString());
    for (FileObject child : children) {
      if (child.getType() == FileType.FILE) {
        assertEquals("percent", read(child));
      } else {
        assertEquals(List.of("inner.txt"), names(child.getChildren()));
      }
    }
  }

  @Test
  void resolvesAPercentNameByItsEscapedForm() throws Exception {
    put("data/file%.txt", "percent");

    FileObject file = resolve("data/file%25.txt");

    assertTrue(file.exists(), unhandled.toString());
    assertEquals("percent", read(file));
  }

  @Test
  void readingAFolderSaysItIsNotAFile() throws Exception {
    put("data/sub/file.txt", "x");
    FileObject sub = resolve("data/sub");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> sub.getContent().getInputStream());

    assertEquals("vfs.provider/read-not-file.error", e.getCode(), unhandled.toString());
  }

  @Test
  void readingAMissingObjectSaysItIsNotAFile() throws Exception {
    FileObject missing = resolve("data/missing.txt");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> missing.getContent().getInputStream());

    assertEquals("vfs.provider/read-not-file.error", e.getCode(), unhandled.toString());
  }

  @Test
  void listingAFileSaysItIsNotAFolder() throws Exception {
    put("data/plain.txt", "content");
    FileObject file = resolve("data/plain.txt");

    FileSystemException e = assertThrows(FileSystemException.class, file::getChildren);

    assertEquals("vfs.provider/list-children-not-folder.error", e.getCode(), unhandled.toString());
  }

  /** Resolving gs:// URIs reuses the provider's file system instead of building a new one. */
  @Test
  void resolvingAgainReusesTheFileSystem() throws Exception {
    FileObject first = manager.resolveFile("gs://" + BUCKET + "/data/a.txt");
    FileObject second = manager.resolveFile("gs://" + BUCKET + "/other/b.txt");
    FileObject third = manager.resolveFile("gs://another-bucket/c.txt");

    assertSame(bucket.getFileSystem(), first.getFileSystem());
    assertSame(bucket.getFileSystem(), second.getFileSystem());
    assertSame(bucket.getFileSystem(), third.getFileSystem());
  }

  @Test
  void stillListsAndReadsAPlainFile() throws Exception {
    put("data/plain.txt", "content");

    assertEquals(List.of("plain.txt"), names(resolve("data").getChildren()), unhandled.toString());
    assertEquals("content", read(resolve("data/plain.txt")));
  }
}
