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

package org.apache.hop.databricks.client;

import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Files API client for UC Volumes and Workspace paths ({@code PUT/GET/DELETE /api/2.0/fs/files…},
 * {@code /api/2.0/fs/directories…}). Bearer PAT auth; does not log tokens.
 */
public final class RestDatabricksFilesClient implements DatabricksFilesClient {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final Duration UPLOAD_TIMEOUT = Duration.ofMinutes(30);
  private static final String FILES_PREFIX = "/api/2.0/fs/files";
  private static final String DIRS_PREFIX = "/api/2.0/fs/directories";

  private final String hostBase;
  private final String token;
  private final HttpClient httpClient;
  private final JSONParser parser = new JSONParser();

  public RestDatabricksFilesClient(String hostBase, String token, HttpClient httpClient) {
    this.hostBase = normalizeHost(hostBase);
    this.token = Objects.requireNonNull(token, "token");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
  }

  public static RestDatabricksFilesClient create(
      DatabricksConnection connection, IVariables variables) throws HopException {
    if (connection == null) {
      throw new HopException("Databricks connection is required");
    }
    String host = resolve(variables, connection.getHost());
    String token =
        Encr.decryptPasswordOptionallyEncrypted(resolve(variables, connection.getToken()));
    if (StringUtils.isBlank(host)) {
      throw new HopException("Databricks workspace host is required");
    }
    if (looksUnresolved(host)) {
      throw new HopException(
          "Databricks workspace host still contains unresolved variables after resolve: '"
              + host
              + "'. Set DATABRICKS_HOST (or the variables used in the connection) on the process "
              + "that opens the VFS scheme (Hop GUI environment, or MainSpark --HopConfigFile / job env).");
    }
    if (StringUtils.isBlank(token)) {
      throw new HopException("Databricks personal access token is required");
    }
    if (looksUnresolved(token) || token.contains("${")) {
      throw new HopException(
          "Databricks personal access token still contains unresolved variables. Set DATABRICKS_TOKEN "
              + "(or the variables used in the connection) on the process that opens the VFS scheme.");
    }
    HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    return new RestDatabricksFilesClient(host, token, client);
  }

  private static boolean looksUnresolved(String value) {
    return value != null && (value.contains("${") || value.contains("%%"));
  }

  /** Visible for tests with a custom {@link HttpClient}. */
  public static RestDatabricksFilesClient createForTest(
      String hostBase, String token, HttpClient httpClient) {
    return new RestDatabricksFilesClient(hostBase, token, httpClient);
  }

  @Override
  public void upload(Path localFile, String workspacePath) throws HopException {
    if (localFile == null || !Files.isRegularFile(localFile)) {
      throw new HopException("Local file for workspace upload does not exist: " + localFile);
    }
    String path = requireFilesApiPath(workspacePath);
    try {
      String encodedPath = encodeFilesApiPath(path);
      String url = hostBase + FILES_PREFIX + encodedPath + "?overwrite=true";
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(UPLOAD_TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/octet-stream")
              .PUT(HttpRequest.BodyPublishers.ofFile(localFile))
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      int code = response.statusCode();
      String body = response.body() == null ? "" : response.body();
      if (code < 200 || code >= 300) {
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for PUT "
                + FILES_PREFIX
                + path
                + ": "
                + sanitizeError(body));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to upload " + localFile + " to " + path + " via Files API", e);
    }
  }

  @Override
  public void uploadText(String workspacePath, String text) throws HopException {
    if (text == null) {
      text = "";
    }
    try {
      Path tmp = Files.createTempFile("hop-dbx-text-", ".txt");
      try {
        Files.writeString(tmp, text, StandardCharsets.UTF_8);
        upload(tmp, workspacePath);
      } finally {
        Files.deleteIfExists(tmp);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to upload text to " + workspacePath, e);
    }
  }

  @Override
  public WorkspaceFileMetadata getFileMetadata(String workspacePath) throws HopException {
    String path = requireFilesApiPath(workspacePath);
    try {
      String encodedPath = encodeFilesApiPath(path);
      String url = hostBase + FILES_PREFIX + encodedPath;
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .method("HEAD", HttpRequest.BodyPublishers.noBody())
              .build();
      HttpResponse<Void> response =
          httpClient.send(request, HttpResponse.BodyHandlers.discarding());
      int code = response.statusCode();
      if (code == 404) {
        return WorkspaceFileMetadata.missing();
      }
      if (code < 200 || code >= 300) {
        return getFileMetadataViaGetRange(path);
      }
      long size = contentLength(response.headers().firstValue("Content-Length").orElse(null));
      if (size < 0) {
        size = contentLength(response.headers().firstValue("content-length").orElse(null));
      }
      if (size < 0) {
        return getFileMetadataViaGetRange(path);
      }
      return WorkspaceFileMetadata.ofFile(size);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to get metadata for " + path, e);
    }
  }

  private WorkspaceFileMetadata getFileMetadataViaGetRange(String absolutePath)
      throws HopException {
    try {
      String encodedPath = encodeFilesApiPath(absolutePath);
      String url = hostBase + FILES_PREFIX + encodedPath;
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .header("Range", "bytes=0-0")
              .GET()
              .build();
      HttpResponse<byte[]> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      if (code == 404) {
        return WorkspaceFileMetadata.missing();
      }
      if (code != 200 && code != 206) {
        String errBody =
            response.body() == null ? "" : new String(response.body(), StandardCharsets.UTF_8);
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for GET "
                + FILES_PREFIX
                + absolutePath
                + ": "
                + sanitizeError(errBody));
      }
      Optional<String> contentRange = response.headers().firstValue("Content-Range");
      if (contentRange.isPresent()) {
        String cr = contentRange.get();
        int slash = cr.lastIndexOf('/');
        if (slash > 0 && slash < cr.length() - 1) {
          try {
            return WorkspaceFileMetadata.ofFile(Long.parseLong(cr.substring(slash + 1).trim()));
          } catch (NumberFormatException ignored) {
            // fall through
          }
        }
      }
      long size = contentLength(response.headers().firstValue("Content-Length").orElse(null));
      if (size >= 0 && code == 200) {
        return WorkspaceFileMetadata.ofFile(size);
      }
      return WorkspaceFileMetadata.missing();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to get metadata for " + absolutePath, e);
    }
  }

  @Override
  public Optional<String> downloadTextIfExists(String workspacePath) throws HopException {
    String path = requireFilesApiPath(workspacePath);
    try {
      String encodedPath = encodeFilesApiPath(path);
      String url = hostBase + FILES_PREFIX + encodedPath;
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();
      HttpResponse<byte[]> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
      int code = response.statusCode();
      if (code == 404) {
        return Optional.empty();
      }
      if (code < 200 || code >= 300) {
        String errBody =
            response.body() == null ? "" : new String(response.body(), StandardCharsets.UTF_8);
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for GET "
                + FILES_PREFIX
                + path
                + ": "
                + sanitizeError(errBody));
      }
      byte[] bytes = response.body() == null ? new byte[0] : response.body();
      return Optional.of(new String(bytes, StandardCharsets.UTF_8));
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to download " + path, e);
    }
  }

  @Override
  public InputStream openInputStream(String workspacePath) throws HopException {
    String path = requireFilesApiPath(workspacePath);
    try {
      String encodedPath = encodeFilesApiPath(path);
      String url = hostBase + FILES_PREFIX + encodedPath;
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(UPLOAD_TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();
      HttpResponse<InputStream> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
      int code = response.statusCode();
      if (code == 404) {
        response.body().close();
        throw new HopException("File not found: " + path);
      }
      if (code < 200 || code >= 300) {
        String errBody;
        try (InputStream err = response.body()) {
          errBody = new String(err.readAllBytes(), StandardCharsets.UTF_8);
        }
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for GET "
                + FILES_PREFIX
                + path
                + ": "
                + sanitizeError(errBody));
      }
      return response.body();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to open stream for " + path, e);
    }
  }

  @Override
  public List<DirectoryEntry> listDirectory(String workspacePath) throws HopException {
    String path = requireFilesApiPath(workspacePath);
    List<DirectoryEntry> all = new ArrayList<>();
    String pageToken = null;
    try {
      do {
        String encodedPath = encodeFilesApiPath(path);
        StringBuilder url = new StringBuilder(hostBase).append(DIRS_PREFIX).append(encodedPath);
        if (pageToken != null) {
          url.append("?page_token=").append(URLEncoder.encode(pageToken, StandardCharsets.UTF_8));
        }
        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(URI.create(url.toString()))
                .timeout(TIMEOUT)
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        int code = response.statusCode();
        String body = response.body() == null ? "" : response.body();
        if (code < 200 || code >= 300) {
          throw new HopException(
              "Databricks API HTTP "
                  + code
                  + " for GET "
                  + DIRS_PREFIX
                  + path
                  + ": "
                  + sanitizeError(body));
        }
        JSONObject json = parseObject(body);
        JSONArray contents = (JSONArray) json.get("contents");
        if (contents != null) {
          for (Object item : contents) {
            if (item instanceof JSONObject entry) {
              all.add(parseDirectoryEntry(entry, path));
            }
          }
        }
        Object next = json.get("next_page_token");
        pageToken =
            next != null && StringUtils.isNotBlank(next.toString()) ? next.toString() : null;
      } while (pageToken != null);
      return all;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to list directory " + path, e);
    }
  }

  private static DirectoryEntry parseDirectoryEntry(JSONObject entry, String parentPath) {
    String name = entry.get("name") != null ? entry.get("name").toString() : "";
    String entryPath =
        entry.get("path") != null
            ? entry.get("path").toString()
            : (parentPath.endsWith("/") ? parentPath + name : parentPath + "/" + name);
    boolean isDir = Boolean.TRUE.equals(entry.get("is_directory"));
    long size = 0L;
    if (entry.get("file_size") instanceof Number n) {
      size = n.longValue();
    } else if (entry.get("file_size") != null) {
      try {
        size = Long.parseLong(entry.get("file_size").toString());
      } catch (NumberFormatException ignored) {
        size = 0L;
      }
    }
    long lastMod = 0L;
    if (entry.get("last_modified") instanceof Number n) {
      lastMod = n.longValue();
    } else if (entry.get("last_modified") != null) {
      try {
        lastMod = Long.parseLong(entry.get("last_modified").toString());
      } catch (NumberFormatException ignored) {
        lastMod = 0L;
      }
    }
    if (StringUtils.isBlank(name) && StringUtils.isNotBlank(entryPath)) {
      int slash = entryPath.lastIndexOf('/');
      name = slash >= 0 ? entryPath.substring(slash + 1) : entryPath;
    }
    return isDir
        ? DirectoryEntry.ofDirectory(name, entryPath, lastMod)
        : DirectoryEntry.ofFile(name, entryPath, size, lastMod);
  }

  @Override
  public void createDirectory(String workspacePath) throws HopException {
    String path = requireFilesApiPath(workspacePath);
    try {
      String encodedPath = encodeFilesApiPath(path);
      // Trailing slash required by Files API for directories
      if (!encodedPath.endsWith("/")) {
        encodedPath = encodedPath + "/";
      }
      String url = hostBase + DIRS_PREFIX + encodedPath;
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .PUT(HttpRequest.BodyPublishers.noBody())
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      int code = response.statusCode();
      String body = response.body() == null ? "" : response.body();
      // 200/201/204 success; some workspaces return 409 if already exists — treat as success
      if (code == 409) {
        return;
      }
      if (code < 200 || code >= 300) {
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for PUT "
                + DIRS_PREFIX
                + path
                + ": "
                + sanitizeError(body));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to create directory " + path, e);
    }
  }

  @Override
  public void deleteFile(String workspacePath) throws HopException {
    String path = requireFilesApiPath(workspacePath);
    try {
      String encodedPath = encodeFilesApiPath(path);
      String url = hostBase + FILES_PREFIX + encodedPath;
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .DELETE()
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      int code = response.statusCode();
      String body = response.body() == null ? "" : response.body();
      if (code == 404) {
        return;
      }
      if (code < 200 || code >= 300) {
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for DELETE "
                + FILES_PREFIX
                + path
                + ": "
                + sanitizeError(body));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to delete file " + path, e);
    }
  }

  @Override
  public void deleteDirectory(String workspacePath, boolean recursive) throws HopException {
    String path = requireFilesApiPath(workspacePath);
    // Workspace Files API rejects ?recursive=… (FILES_API_UNEXPECTED_QUERY_PARAMETERS).
    // Recursive delete: remove children first, then the empty directory.
    if (recursive) {
      List<DirectoryEntry> children = listDirectory(path);
      for (DirectoryEntry child : children) {
        if (child.directory()) {
          deleteDirectory(child.path(), true);
        } else {
          deleteFile(child.path());
        }
      }
    }
    deleteDirectoryEmpty(path);
  }

  /** DELETE {@code /api/2.0/fs/directories{path}/} with no query parameters (empty dir only). */
  private void deleteDirectoryEmpty(String path) throws HopException {
    try {
      String encodedPath = encodeFilesApiPath(path);
      if (!encodedPath.endsWith("/")) {
        encodedPath = encodedPath + "/";
      }
      String url = hostBase + DIRS_PREFIX + encodedPath;
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(TIMEOUT)
              .header("Authorization", "Bearer " + token)
              .DELETE()
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      int code = response.statusCode();
      String body = response.body() == null ? "" : response.body();
      if (code == 404) {
        return;
      }
      if (code < 200 || code >= 300) {
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for DELETE "
                + DIRS_PREFIX
                + path
                + ": "
                + sanitizeError(body));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to delete directory " + path, e);
    }
  }

  @Override
  public void close() {
    // HttpClient does not require close
  }

  /**
   * UC Volumes and Workspace files must use the Files API. Classic DBFS roots use the legacy DBFS
   * block API (not this client).
   */
  public static boolean isFilesApiPath(String absolutePath) {
    if (StringUtils.isBlank(absolutePath)) {
      return false;
    }
    String p = absolutePath.trim();
    return p.startsWith("/Volumes/")
        || p.equals("/Volumes")
        || p.startsWith("/Workspace/")
        || p.equals("/Workspace");
  }

  /**
   * Normalize workspace paths: strip optional {@code dbfs:} scheme, ensure a leading slash. {@code
   * dbfs:/Volumes/…} becomes {@code /Volumes/…}.
   */
  public static String normalizeWorkspacePath(String workspacePath) throws HopException {
    if (StringUtils.isBlank(workspacePath)) {
      throw new HopException("Upload path is required");
    }
    String p = workspacePath.trim();
    if (p.startsWith("dbfs:")) {
      p = p.substring("dbfs:".length());
    }
    if (!p.startsWith("/")) {
      p = "/" + p;
    }
    return p;
  }

  static String requireFilesApiPath(String workspacePath) throws HopException {
    String path = normalizeWorkspacePath(workspacePath);
    if (!isFilesApiPath(path)) {
      throw new HopException(
          "Path is not a Databricks Files API location (expected /Volumes/… or /Workspace/…): "
              + path
              + ". Classic DBFS paths are not supported by this client.");
    }
    return path;
  }

  /**
   * Encode an absolute workspace path for the Files API URL path (keep {@code /} separators, encode
   * each segment).
   */
  public static String encodeFilesApiPath(String absolutePath) {
    String p = absolutePath.startsWith("/") ? absolutePath : "/" + absolutePath;
    String[] parts = p.split("/", -1);
    StringBuilder sb = new StringBuilder();
    for (String part : parts) {
      if (part.isEmpty()) {
        continue;
      }
      sb.append('/').append(URLEncoder.encode(part, StandardCharsets.UTF_8).replace("+", "%20"));
    }
    return sb.length() == 0 ? "/" : sb.toString();
  }

  /** Strip likely secrets from error payloads before logging. */
  public static String sanitizeError(String body) {
    if (body == null) {
      return "";
    }
    String trimmed = body.length() > 500 ? body.substring(0, 500) + "…" : body;
    return trimmed.replaceAll("(?i)\\b(token|authorization|bearer)\\b\\s*[:=]?\\s*\\S+", "$1=***");
  }

  public static String normalizeHost(String host) {
    String h = host.trim();
    if (!h.startsWith("http://") && !h.startsWith("https://")) {
      h = "https://" + h;
    }
    while (h.endsWith("/")) {
      h = h.substring(0, h.length() - 1);
    }
    return h;
  }

  private static long contentLength(String header) {
    if (StringUtils.isBlank(header)) {
      return -1L;
    }
    try {
      return Long.parseLong(header.trim());
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  private JSONObject parseObject(String body) throws HopException {
    try {
      Object parsed = parser.parse(body);
      if (!(parsed instanceof JSONObject)) {
        throw new HopException("Expected JSON object from Databricks API");
      }
      return (JSONObject) parsed;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to parse Databricks API response", e);
    }
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null) {
      return null;
    }
    return variables != null ? variables.resolve(value) : value;
  }
}
