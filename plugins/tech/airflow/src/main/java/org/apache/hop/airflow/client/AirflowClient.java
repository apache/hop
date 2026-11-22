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

package org.apache.hop.airflow.client;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.airflow.metadata.AirflowAuthenticationType;
import org.apache.hop.airflow.metadata.AirflowConnection;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Client for the Apache Airflow 3 REST API (v2).
 *
 * <p>With {@link AirflowAuthenticationType#USERNAME_PASSWORD} the client exchanges the credentials
 * for a token at {@code /auth/token} on first use. Airflow tokens expire, so when Airflow answers
 * 401 the client gets a new token once and repeats the request.
 */
public class AirflowClient {

  static final String API_PATH = "/api/v2";

  private final HttpClient httpClient;
  private final String baseUrl;
  private final AirflowAuthenticationType authenticationType;
  private final String username;
  private final String password;
  private final Duration requestTimeout;

  private String accessToken;

  public AirflowClient(
      HttpClient httpClient,
      String baseUrl,
      AirflowAuthenticationType authenticationType,
      String username,
      String password,
      String token,
      Duration requestTimeout) {
    this.httpClient = httpClient;
    this.baseUrl = normalizeBaseUrl(baseUrl);
    this.authenticationType =
        authenticationType == null ? AirflowAuthenticationType.NONE : authenticationType;
    this.username = username;
    this.password = password;
    this.accessToken = this.authenticationType == AirflowAuthenticationType.TOKEN ? token : null;
    this.requestTimeout = requestTimeout;
  }

  /** A client for the connection, with variables resolved and passwords decrypted. */
  public static AirflowClient create(AirflowConnection connection, IVariables variables)
      throws HopException {
    String url = variables.resolve(connection.getUrl());
    if (StringUtils.isBlank(url)) {
      throw new HopException("The Airflow connection has no URL");
    }
    int connectTimeout = Const.toInt(variables.resolve(connection.getConnectTimeoutSeconds()), 10);
    int requestTimeout = Const.toInt(variables.resolve(connection.getRequestTimeoutSeconds()), 60);
    // HTTP/1.1: on a plain http:// URL the default asks to upgrade to HTTP/2, and the Airflow API
    // server (uvicorn) answers that request with HTTP 400.
    HttpClient httpClient =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(Math.max(1, connectTimeout)))
            // Never follow redirects: Java turns a redirected POST into a GET, which would list the
            // DAG runs instead of starting one.
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    try {
      return new AirflowClient(
          httpClient,
          url,
          connection.getAuthenticationType(),
          variables.resolve(connection.getUsername()),
          Encr.decryptPasswordOptionallyEncrypted(variables.resolve(connection.getPassword())),
          Encr.decryptPasswordOptionallyEncrypted(variables.resolve(connection.getToken())),
          Duration.ofSeconds(Math.max(1, requestTimeout)));
    } catch (IllegalArgumentException e) {
      throw new HopException(e.getMessage(), e);
    }
  }

  static String normalizeBaseUrl(String url) {
    String trimmed = StringUtils.trimToEmpty(url);
    if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
      throw new IllegalArgumentException(
          "The Airflow URL must start with http:// or https://, not '" + trimmed + "'");
    }
    trimmed = StringUtils.removeEnd(trimmed, "/");
    trimmed = StringUtils.removeEnd(trimmed, API_PATH);
    return StringUtils.removeEnd(trimmed, "/");
  }

  /**
   * Checks the URL and the credentials. The version endpoint answers without credentials, so this
   * also reads the DAG list.
   *
   * @return the Airflow version
   */
  public String testConnection() throws HopException {
    JSONObject version = getJson(API_PATH + "/version");
    getJson(API_PATH + "/dags?limit=1");
    return String.valueOf(version.get("version"));
  }

  /**
   * @return true when the DAG is paused, so that a new run stays queued
   * @throws HopException when the DAG does not exist or Airflow cannot be reached
   */
  public boolean isDagPaused(String dagId) throws HopException {
    JSONObject dag = getJson(API_PATH + "/dags/" + encode(dagId));
    return Boolean.TRUE.equals(dag.get("is_paused"));
  }

  /**
   * Starts a DAG run.
   *
   * @param dagRunId the id of the new run, or empty to let Airflow choose one
   * @param logicalDate an ISO 8601 date and time, or empty for none
   * @param conf the run configuration, available to the tasks as {@code dag_run.conf}
   */
  @SuppressWarnings("unchecked")
  public AirflowDagRun triggerDagRun(
      String dagId, String dagRunId, String logicalDate, Map<String, Object> conf)
      throws HopException {
    JSONObject body = new JSONObject();
    if (StringUtils.isNotBlank(dagRunId)) {
      body.put("dag_run_id", dagRunId);
    }
    // Airflow 3 requires the key, even when the value is empty.
    body.put("logical_date", StringUtils.isBlank(logicalDate) ? null : logicalDate);
    JSONObject confJson = new JSONObject();
    if (conf != null) {
      confJson.putAll(conf);
    }
    body.put("conf", confJson);
    String path = API_PATH + "/dags/" + encode(dagId) + "/dagRuns";
    JSONObject run = sendJson("POST", path, body.toJSONString());
    return toDagRun(run, dagId, "POST", path);
  }

  public AirflowDagRun getDagRun(String dagId, String dagRunId) throws HopException {
    String path = API_PATH + "/dags/" + encode(dagId) + "/dagRuns/" + encode(dagRunId);
    return toDagRun(getJson(path), dagId, "GET", path);
  }

  /** The ids of the tasks of a DAG run that are in one of the given states. */
  public List<String> getTaskIds(String dagId, String dagRunId, Set<String> states)
      throws HopException {
    JSONObject response =
        getJson(
            API_PATH
                + "/dags/"
                + encode(dagId)
                + "/dagRuns/"
                + encode(dagRunId)
                + "/taskInstances");
    List<String> taskIds = new ArrayList<>();
    Object instances = response.get("task_instances");
    if (instances instanceof JSONArray array) {
      for (Object item : array) {
        if (item instanceof JSONObject instance
            && states.contains(String.valueOf(instance.get("state")))) {
          taskIds.add(String.valueOf(instance.get("task_id")));
        }
      }
    }
    return taskIds;
  }

  private static AirflowDagRun toDagRun(JSONObject run, String dagId, String method, String path)
      throws HopException {
    Object runDagId = run.get("dag_id");
    Object runId = run.get("dag_run_id");
    Object state = run.get("state");
    if (runId == null || state == null) {
      throw new HopException(
          "Airflow answered "
              + method
              + " "
              + path
              + " without a DAG run id and state: "
              + StringUtils.abbreviate(run.toJSONString(), 300));
    }
    return new AirflowDagRun(
        runDagId == null ? dagId : String.valueOf(runDagId),
        String.valueOf(runId),
        String.valueOf(state));
  }

  private JSONObject getJson(String path) throws HopException {
    return sendJson("GET", path, null);
  }

  private JSONObject sendJson(String method, String path, String body) throws HopException {
    HttpResponse<String> response = send(method, path, body, true);
    return parseObject(response.body(), method, path);
  }

  private HttpResponse<String> send(String method, String path, String body, boolean retryOn401)
      throws HopException {
    if (authenticationType == AirflowAuthenticationType.USERNAME_PASSWORD && accessToken == null) {
      accessToken = requestToken();
    }
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(baseUrl + path))
            .timeout(requestTimeout)
            .header("Accept", "application/json");
    if (StringUtils.isNotEmpty(accessToken)) {
      builder.header("Authorization", "Bearer " + accessToken);
    }
    if (body == null) {
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      builder
          .header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    }
    HttpResponse<String> response = execute(builder.build(), method, path);
    if (response.statusCode() == 401
        && retryOn401
        && authenticationType == AirflowAuthenticationType.USERNAME_PASSWORD) {
      accessToken = null;
      return send(method, path, body, false);
    }
    checkRedirect(response, method, path);
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new HopException(
          "Airflow answered HTTP "
              + response.statusCode()
              + " to "
              + method
              + " "
              + path
              + ": "
              + errorDetail(response.body()));
    }
    return response;
  }

  @SuppressWarnings("unchecked")
  private String requestToken() throws HopException {
    JSONObject credentials = new JSONObject();
    credentials.put("username", StringUtils.defaultString(username));
    credentials.put("password", StringUtils.defaultString(password));
    String path = "/auth/token";
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(baseUrl + path))
            .timeout(requestTimeout)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    credentials.toJSONString(), StandardCharsets.UTF_8))
            .build();
    HttpResponse<String> response = execute(request, "POST", path);
    checkRedirect(response, "POST", path);
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new HopException(
          "Airflow refused the username and password (HTTP "
              + response.statusCode()
              + "): "
              + errorDetail(response.body()));
    }
    Object token = parseObject(response.body(), "POST", path).get("access_token");
    if (token == null || StringUtils.isBlank(token.toString())) {
      throw new HopException("Airflow answered " + path + " without an access token");
    }
    return token.toString();
  }

  /** Redirects are not followed: say where the server points to, so the URL can be fixed. */
  private void checkRedirect(HttpResponse<String> response, String method, String path)
      throws HopException {
    int status = response.statusCode();
    if (status >= 300 && status < 400) {
      throw new HopException(
          "Airflow answered HTTP "
              + status
              + " to "
              + method
              + " "
              + path
              + " and redirects to "
              + response.headers().firstValue("Location").orElse("(no location)")
              + ". Hop does not follow redirects: use that address in the Airflow connection.");
    }
  }

  private HttpResponse<String> execute(HttpRequest request, String method, String path)
      throws HopException {
    try {
      return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted while calling Airflow at " + baseUrl + path, e);
    } catch (IOException e) {
      throw new HopException(
          "Unable to reach Airflow at " + baseUrl + " (" + method + " " + path + ")", e);
    }
  }

  private static JSONObject parseObject(String body, String method, String path)
      throws HopException {
    try {
      Object parsed = new JSONParser().parse(StringUtils.defaultIfBlank(body, "{}"));
      if (parsed instanceof JSONObject object) {
        return object;
      }
    } catch (ParseException e) {
      // Reported below
    }
    throw new HopException(
        "Airflow answered "
            + method
            + " "
            + path
            + " with something other than a JSON object: "
            + StringUtils.abbreviate(body, 300));
  }

  /** The most useful part of an Airflow error body: FastAPI puts it in "detail". */
  static String errorDetail(String body) {
    if (StringUtils.isBlank(body)) {
      return "(no details)";
    }
    try {
      Object parsed = new JSONParser().parse(body);
      if (parsed instanceof JSONObject object) {
        Object detail = object.get("detail");
        if (detail instanceof String text) {
          return text;
        }
        if (detail instanceof JSONObject detailObject) {
          Object reason = detailObject.get("reason");
          if (reason != null) {
            return reason.toString();
          }
          return detailObject.toJSONString();
        }
        if (detail instanceof JSONArray errors) {
          List<String> messages = new ArrayList<>();
          for (Object error : errors) {
            if (error instanceof JSONObject errorObject) {
              messages.add(errorObject.get("loc") + ": " + errorObject.get("msg"));
            }
          }
          if (!messages.isEmpty()) {
            return String.join("; ", messages);
          }
        }
      }
    } catch (ParseException e) {
      // Not JSON: show the start of the body
    }
    return StringUtils.abbreviate(body, 300);
  }

  static String encode(String pathSegment) {
    return URLEncoder.encode(pathSegment, StandardCharsets.UTF_8).replace("+", "%20");
  }

  public String getBaseUrl() {
    return baseUrl;
  }
}
