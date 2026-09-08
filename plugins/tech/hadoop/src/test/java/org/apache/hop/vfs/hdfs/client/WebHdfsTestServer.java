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
package org.apache.hop.vfs.hdfs.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/** Minimal WebHDFS/HttpFS stub for unit tests. In-memory files, no Hadoop. */
public class WebHdfsTestServer {
  private final Map<String, byte[]> files = new ConcurrentHashMap<>();
  private final Map<String, Boolean> dirs = new ConcurrentHashMap<>();
  private HttpServer server;
  private int port;

  public WebHdfsTestServer() {
    dirs.put("/", true);
  }

  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/webhdfs/v1", this::handle);
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
    port = server.getAddress().getPort();
  }

  public void stop() {
    if (server != null) {
      server.stop(0);
    }
  }

  public int getPort() {
    return port;
  }

  public String endpoint() {
    return "127.0.0.1:" + port;
  }

  public byte[] file(String path) {
    return files.get(normalize(path));
  }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      String path = exchange.getRequestURI().getPath();
      String hdfsPath = path.substring("/webhdfs/v1".length());
      if (hdfsPath.isEmpty()) {
        hdfsPath = "/";
      }
      hdfsPath = normalize(hdfsPath);
      Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
      String op = query.getOrDefault("op", "");
      switch (op) {
        case "GETFILESTATUS" -> getFileStatus(exchange, hdfsPath);
        case "LISTSTATUS" -> listStatus(exchange, hdfsPath);
        case "MKDIRS" -> mkdirs(exchange, hdfsPath);
        case "CREATE" -> create(exchange, hdfsPath, query);
        case "OPEN" -> open(exchange, hdfsPath);
        case "DELETE" -> delete(exchange, hdfsPath);
        case "RENAME" -> rename(exchange, hdfsPath, query.get("destination"));
        default -> send(exchange, 400, "unknown op " + op);
      }
    } catch (Exception e) {
      send(exchange, 500, e.getMessage() == null ? "error" : e.getMessage());
    }
  }

  private void getFileStatus(HttpExchange exchange, String path) throws IOException {
    if (dirs.containsKey(path)) {
      send(exchange, 200, "{\"FileStatus\":" + fileStatusJson(path, true, 0, "") + "}");
      return;
    }
    byte[] data = files.get(path);
    if (data != null) {
      send(
          exchange,
          200,
          "{\"FileStatus\":" + fileStatusJson(path, false, data.length, suffix(path)) + "}");
      return;
    }
    send(exchange, 404, notFound(path));
  }

  private void listStatus(HttpExchange exchange, String path) throws IOException {
    if (!dirs.containsKey(path) && !"/".equals(path)) {
      send(exchange, 404, notFound(path));
      return;
    }
    String prefix = "/".equals(path) ? "/" : path.endsWith("/") ? path : path + "/";
    StringBuilder array = new StringBuilder();
    for (String dir : dirs.keySet()) {
      if (isDirectChild(prefix, dir) && !dir.equals(path) && !dir.equals("/")) {
        if (array.length() > 0) {
          array.append(',');
        }
        array.append(fileStatusJson(dir, true, 0, suffix(dir)));
      }
    }
    for (Map.Entry<String, byte[]> entry : files.entrySet()) {
      if (isDirectChild(prefix, entry.getKey())) {
        if (array.length() > 0) {
          array.append(',');
        }
        array.append(
            fileStatusJson(entry.getKey(), false, entry.getValue().length, suffix(entry.getKey())));
      }
    }
    send(exchange, 200, "{\"FileStatuses\":{\"FileStatus\":[" + array + "]}}");
  }

  private void mkdirs(HttpExchange exchange, String path) throws IOException {
    String current = "";
    for (String part : path.split("/")) {
      if (part.isEmpty()) {
        continue;
      }
      current = current + "/" + part;
      dirs.put(current, true);
    }
    dirs.put("/", true);
    send(exchange, 200, "{\"boolean\":true}");
  }

  private void create(HttpExchange exchange, String path, Map<String, String> query)
      throws IOException {
    if ("true".equalsIgnoreCase(query.get("noredirect"))) {
      String location =
          "http://127.0.0.1:" + port + "/webhdfs/v1" + path + "?op=CREATE&data=true&overwrite=true";
      send(exchange, 201, "{\"Location\":\"" + location + "\"}");
      return;
    }
    byte[] body = readAll(exchange.getRequestBody());
    files.put(path, body);
    parentDir(path);
    send(exchange, 201, "");
  }

  private void open(HttpExchange exchange, String path) throws IOException {
    byte[] data = files.get(path);
    if (data == null) {
      send(exchange, 404, notFound(path));
      return;
    }
    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
    exchange.sendResponseHeaders(200, data.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(data);
    }
  }

  private void delete(HttpExchange exchange, String path) throws IOException {
    files.remove(path);
    dirs.remove(path);
    send(exchange, 200, "{\"boolean\":true}");
  }

  private void rename(HttpExchange exchange, String source, String destination) throws IOException {
    byte[] data = files.remove(source);
    if (data != null) {
      files.put(normalize(destination), data);
    }
    send(exchange, 200, "{\"boolean\":true}");
  }

  private void parentDir(String path) {
    int slash = path.lastIndexOf('/');
    if (slash > 0) {
      dirs.put(path.substring(0, slash), true);
    }
    dirs.put("/", true);
  }

  private static boolean isDirectChild(String prefix, String candidate) {
    if (!candidate.startsWith(prefix)) {
      return false;
    }
    String rest = candidate.substring(prefix.length());
    return !rest.isEmpty() && !rest.contains("/");
  }

  private static String suffix(String path) {
    int slash = path.lastIndexOf('/');
    return slash < 0 ? path : path.substring(slash + 1);
  }

  private static String fileStatusJson(String path, boolean dir, long length, String suffix) {
    return "{\"pathSuffix\":\""
        + suffix
        + "\",\"type\":\""
        + (dir ? "DIRECTORY" : "FILE")
        + "\",\"length\":"
        + length
        + ",\"modificationTime\":1}";
  }

  private static String notFound(String path) {
    return "{\"RemoteException\":{\"exception\":\"FileNotFoundException\",\"message\":\"File does not exist: "
        + path
        + "\",\"javaClassName\":\"java.io.FileNotFoundException\"}}";
  }

  private static void send(HttpExchange exchange, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(code, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    in.transferTo(out);
    return out.toByteArray();
  }

  private static String normalize(String path) {
    if (path == null || path.isEmpty()) {
      return "/";
    }
    return path.startsWith("/") ? path : "/" + path;
  }

  private static Map<String, String> parseQuery(String raw) {
    Map<String, String> map = new HashMap<>();
    if (raw == null || raw.isEmpty()) {
      return map;
    }
    for (String part : raw.split("&")) {
      int eq = part.indexOf('=');
      if (eq < 0) {
        map.put(part, "");
      } else {
        map.put(
            URLDecoder.decode(part.substring(0, eq), StandardCharsets.UTF_8),
            URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8));
      }
    }
    return map;
  }
}
