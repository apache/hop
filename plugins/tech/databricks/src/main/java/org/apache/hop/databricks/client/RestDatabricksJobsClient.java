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
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Databricks Jobs API client using {@link HttpClient} and Jobs REST endpoints (default base {@code
 * /api/2.1}). Does not log tokens. Uploads use the Files API for UC Volumes / Workspace paths
 * (delegated to {@link RestDatabricksFilesClient}) and the legacy DBFS API for classic {@code
 * dbfs:/} roots.
 */
public final class RestDatabricksJobsClient implements DatabricksJobsClient {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final Duration UPLOAD_TIMEOUT = Duration.ofMinutes(30);

  /** DBFS add-block max is 1 MiB of base64-decoded data. */
  private static final int DBFS_BLOCK_BYTES = 1024 * 1024;

  private final String hostBase;
  private final String apiBase;
  private final String token;
  private final HttpClient httpClient;
  private final RestDatabricksFilesClient filesClient;
  private final JSONParser parser = new JSONParser();

  public RestDatabricksJobsClient(
      String hostBase, String apiBase, String token, HttpClient httpClient) {
    this.hostBase = normalizeHost(hostBase);
    this.apiBase = normalizeApiBase(apiBase);
    this.token = Objects.requireNonNull(token, "token");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    this.filesClient = new RestDatabricksFilesClient(this.hostBase, this.token, this.httpClient);
  }

  public static RestDatabricksJobsClient create(
      DatabricksConnection connection, IVariables variables) throws HopException {
    if (connection == null) {
      throw new HopException("Databricks connection is required");
    }
    String host = resolve(variables, connection.getHost());
    // PAT may be a literal, ${variable}, or Encrypted… (incl. after variable resolve)
    String token =
        Encr.decryptPasswordOptionallyEncrypted(resolve(variables, connection.getToken()));
    String apiBase = resolve(variables, connection.getApiBasePath());
    if (StringUtils.isBlank(host)) {
      throw new HopException("Databricks workspace host is required");
    }
    if (StringUtils.isBlank(token)) {
      throw new HopException("Databricks personal access token is required");
    }
    HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    return new RestDatabricksJobsClient(host, apiBase, token, client);
  }

  /** Visible for tests with a custom {@link HttpClient}. */
  public static RestDatabricksJobsClient createForTest(
      String hostBase, String apiBase, String token, HttpClient httpClient) {
    return new RestDatabricksJobsClient(hostBase, apiBase, token, httpClient);
  }

  /** Files API client shared for Volume / Workspace paths (deploy + VFS). */
  public RestDatabricksFilesClient files() {
    return filesClient;
  }

  @Override
  public String testConnection() throws HopException {
    // Prefer SCIM me when available; fall back to listing one job.
    try {
      String body = get("/api/2.0/preview/scim/v2/Me");
      JSONObject json = parseObject(body);
      Object userName = json.get("userName");
      if (userName != null) {
        return userName.toString();
      }
      Object display = json.get("displayName");
      if (display != null) {
        return display.toString();
      }
    } catch (HopException ignored) {
      // SCIM may be disabled; use jobs/list
    }
    String body = get(apiBase + "/jobs/list?limit=1");
    parseObject(body); // validates JSON / auth
    return hostBase;
  }

  @Override
  public long runNow(long jobId, Map<String, String> notebookOrJarParams) throws HopException {
    JSONObject body = new JSONObject();
    body.put("job_id", jobId);
    if (notebookOrJarParams != null && !notebookOrJarParams.isEmpty()) {
      // Jobs API: notebook_params / jar_params / python_params — use notebook_params as generic map
      JSONObject params = new JSONObject();
      params.putAll(notebookOrJarParams);
      body.put("notebook_params", params);
    }
    String response = postJson(apiBase + "/jobs/run-now", body.toJSONString());
    JSONObject json = parseObject(response);
    return requireLong(json, "run_id");
  }

  @Override
  public long submitRun(String submitRunJsonBody) throws HopException {
    String response = postJson(apiBase + "/jobs/runs/submit", submitRunJsonBody);
    JSONObject json = parseObject(response);
    return requireLong(json, "run_id");
  }

  @Override
  public long createJob(String createJobJsonBody) throws HopException {
    String response = postJson(apiBase + "/jobs/create", createJobJsonBody);
    JSONObject json = parseObject(response);
    return requireLong(json, "job_id");
  }

  @Override
  public void resetJob(String resetJobJsonBody) throws HopException {
    postJson(apiBase + "/jobs/reset", resetJobJsonBody);
  }

  @Override
  public DatabricksRunStatus getRun(long runId) throws HopException {
    String response =
        get(
            apiBase
                + "/jobs/runs/get?run_id="
                + URLEncoder.encode(Long.toString(runId), StandardCharsets.UTF_8));
    JSONObject json = parseObject(response);
    long id = requireLong(json, "run_id");
    Long jobId = null;
    if (json.get("job_id") != null) {
      jobId = ((Number) json.get("job_id")).longValue();
    }
    String pageUrl = json.get("run_page_url") != null ? json.get("run_page_url").toString() : null;
    JSONObject state = (JSONObject) json.get("state");
    DatabricksRunLifeCycleState life = DatabricksRunLifeCycleState.UNKNOWN;
    String resultState = null;
    String stateMessage = null;
    if (state != null) {
      if (state.get("life_cycle_state") != null) {
        life = DatabricksRunLifeCycleState.fromApi(state.get("life_cycle_state").toString());
      }
      if (state.get("result_state") != null) {
        resultState = state.get("result_state").toString();
      }
      if (state.get("state_message") != null) {
        stateMessage = state.get("state_message").toString();
      }
    }
    return new DatabricksRunStatus(id, jobId, life, resultState, stateMessage, pageUrl);
  }

  @Override
  public void cancelRun(long runId) throws HopException {
    JSONObject body = new JSONObject();
    body.put("run_id", runId);
    postJson(apiBase + "/jobs/runs/cancel", body.toJSONString());
  }

  @Override
  public void uploadToDbfs(Path localFile, String dbfsPath) throws HopException {
    if (localFile == null || !Files.isRegularFile(localFile)) {
      throw new HopException("Local file for workspace upload does not exist: " + localFile);
    }
    String path = normalizeDbfsPath(dbfsPath);
    if (isFilesApiPath(path)) {
      filesClient.upload(localFile, path);
    } else {
      uploadViaDbfsApi(localFile, path);
    }
  }

  @Override
  public WorkspaceFileMetadata getFileMetadata(String workspacePath) throws HopException {
    String path = normalizeDbfsPath(workspacePath);
    if (isFilesApiPath(path)) {
      return filesClient.getFileMetadata(path);
    }
    return getFileMetadataDbfs(path);
  }

  @Override
  public Optional<String> downloadTextIfExists(String workspacePath) throws HopException {
    String path = normalizeDbfsPath(workspacePath);
    if (isFilesApiPath(path)) {
      return filesClient.downloadTextIfExists(path);
    }
    return downloadTextDbfs(path);
  }

  @Override
  public void uploadText(String workspacePath, String text) throws HopException {
    String path = normalizeDbfsPath(workspacePath);
    if (isFilesApiPath(path)) {
      filesClient.uploadText(path, text);
      return;
    }
    if (text == null) {
      text = "";
    }
    try {
      Path tmp = Files.createTempFile("hop-dbx-text-", ".txt");
      try {
        Files.writeString(tmp, text, StandardCharsets.UTF_8);
        uploadViaDbfsApi(tmp, path);
      } finally {
        Files.deleteIfExists(tmp);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to upload text to " + workspacePath, e);
    }
  }

  /**
   * UC Volumes and Workspace files must use the Files API. Classic DBFS roots (FileStore, etc.) use
   * the legacy DBFS block API.
   */
  static boolean isFilesApiPath(String absolutePath) {
    return RestDatabricksFilesClient.isFilesApiPath(absolutePath);
  }

  private WorkspaceFileMetadata getFileMetadataDbfs(String path) throws HopException {
    try {
      String q = "/api/2.0/dbfs/get-status?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
      String body = get(q);
      JSONObject json = parseObject(body);
      if (Boolean.TRUE.equals(json.get("is_dir"))) {
        return WorkspaceFileMetadata.missing();
      }
      long size = requireLong(json, "file_size");
      return WorkspaceFileMetadata.ofFile(size);
    } catch (HopException e) {
      String msg = e.getMessage() == null ? "" : e.getMessage();
      if (msg.contains("404")
          || msg.contains("RESOURCE_DOES_NOT_EXIST")
          || msg.contains("File not found")
          || msg.contains("does not exist")) {
        return WorkspaceFileMetadata.missing();
      }
      throw e;
    }
  }

  private Optional<String> downloadTextDbfs(String path) throws HopException {
    try {
      // Single-block read: small sidecar files only
      String q =
          "/api/2.0/dbfs/read?path="
              + URLEncoder.encode(path, StandardCharsets.UTF_8)
              + "&offset=0&length="
              + (1024 * 1024);
      String resp = get(q);
      JSONObject json = parseObject(resp);
      Object data = json.get("data");
      if (data == null) {
        return Optional.of("");
      }
      byte[] decoded = Base64.getDecoder().decode(data.toString());
      return Optional.of(new String(decoded, StandardCharsets.UTF_8));
    } catch (HopException e) {
      String msg = e.getMessage() == null ? "" : e.getMessage();
      if (msg.contains("404")
          || msg.contains("RESOURCE_DOES_NOT_EXIST")
          || msg.contains("File not found")
          || msg.contains("does not exist")) {
        return Optional.empty();
      }
      throw e;
    }
  }

  /** Legacy DBFS create / add-block / close for classic {@code dbfs:/} paths. */
  private void uploadViaDbfsApi(Path localFile, String path) throws HopException {
    try {
      JSONObject create = new JSONObject();
      create.put("path", path);
      create.put("overwrite", true);
      String createResp = postJson("/api/2.0/dbfs/create", create.toJSONString(), UPLOAD_TIMEOUT);
      JSONObject createJson = parseObject(createResp);
      long handle = requireLong(createJson, "handle");

      try (InputStream in = Files.newInputStream(localFile)) {
        byte[] buf = new byte[DBFS_BLOCK_BYTES];
        int n;
        while ((n = in.read(buf)) >= 0) {
          if (n == 0) {
            continue;
          }
          byte[] chunk = n == buf.length ? buf : java.util.Arrays.copyOf(buf, n);
          JSONObject add = new JSONObject();
          add.put("handle", handle);
          add.put("data", Base64.getEncoder().encodeToString(chunk));
          postJson("/api/2.0/dbfs/add-block", add.toJSONString(), UPLOAD_TIMEOUT);
        }
      }

      JSONObject close = new JSONObject();
      close.put("handle", handle);
      postJson("/api/2.0/dbfs/close", close.toJSONString(), UPLOAD_TIMEOUT);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to upload " + localFile + " to " + path + " via DBFS API", e);
    }
  }

  @Override
  public void close() {
    filesClient.close();
  }

  /**
   * Normalize upload paths: strip optional {@code dbfs:} scheme, ensure a leading slash. {@code
   * dbfs:/Volumes/…} becomes {@code /Volumes/…} so the Files API route is selected.
   */
  static String normalizeDbfsPath(String dbfsPath) throws HopException {
    return RestDatabricksFilesClient.normalizeWorkspacePath(dbfsPath);
  }

  /**
   * Encode an absolute workspace path for the Files API URL path (keep {@code /} separators, encode
   * each segment).
   */
  static String encodeFilesApiPath(String absolutePath) {
    return RestDatabricksFilesClient.encodeFilesApiPath(absolutePath);
  }

  private String get(String path) throws HopException {
    return exchange("GET", path, null, TIMEOUT);
  }

  private String postJson(String path, String jsonBody) throws HopException {
    return postJson(path, jsonBody, TIMEOUT);
  }

  private String postJson(String path, String jsonBody, Duration timeout) throws HopException {
    return exchange("POST", path, jsonBody, timeout);
  }

  private String exchange(String method, String path, String jsonBody, Duration timeout)
      throws HopException {
    try {
      String url = hostBase + (path.startsWith("/") ? path : "/" + path);
      HttpRequest.Builder builder =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(timeout)
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/json");
      if ("GET".equalsIgnoreCase(method)) {
        builder.GET();
      } else {
        builder.method(
            method,
            HttpRequest.BodyPublishers.ofString(
                jsonBody == null ? "" : jsonBody, StandardCharsets.UTF_8));
      }
      HttpResponse<String> response =
          httpClient.send(
              builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      int code = response.statusCode();
      String body = response.body() == null ? "" : response.body();
      if (code < 200 || code >= 300) {
        throw new HopException(
            "Databricks API HTTP "
                + code
                + " for "
                + method
                + " "
                + path
                + ": "
                + sanitizeError(body));
      }
      return body;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Databricks API call failed: " + method + " " + path, e);
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

  private static long requireLong(JSONObject json, String key) throws HopException {
    Object v = json.get(key);
    if (v instanceof Number number) {
      return number.longValue();
    }
    if (v != null) {
      try {
        return Long.parseLong(v.toString());
      } catch (NumberFormatException ignored) {
        // fall through
      }
    }
    throw new HopException("Databricks API response missing '" + key + "'");
  }

  /** Strip likely secrets from error payloads before logging. */
  static String sanitizeError(String body) {
    return RestDatabricksFilesClient.sanitizeError(body);
  }

  static String normalizeHost(String host) {
    return RestDatabricksFilesClient.normalizeHost(host);
  }

  static String normalizeApiBase(String apiBase) {
    if (StringUtils.isBlank(apiBase)) {
      return "/api/2.1";
    }
    String b = apiBase.trim();
    if (!b.startsWith("/")) {
      b = "/" + b;
    }
    while (b.endsWith("/") && b.length() > 1) {
      b = b.substring(0, b.length() - 1);
    }
    return b;
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null) {
      return null;
    }
    return variables != null ? variables.resolve(value) : value;
  }

  /** Build jar_params style list JSON helper for callers (not used by run-now map). */
  public static Map<String, String> copyParams(Map<String, String> in) {
    return in == null ? Map.of() : new LinkedHashMap<>(in);
  }
}
