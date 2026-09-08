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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.HdfsTransport;
import org.apache.hop.vfs.hdfs.kerberos.HdfsKerberosSession;
import org.ietf.jgss.GSSException;

/**
 * WebHDFS REST client. Speaks HTTP only; no {@code org.apache.hadoop} types. Used for HttpFS, Knox
 * and NameNode WebHDFS.
 */
public class HdfsWebHdfsClient {
  private static final Class<?> PKG = HdfsTransport.class;
  private static final String DEFAULT_BASE_PATH = "/webhdfs/v1";

  private final CloseableHttpClient httpClient;
  private final HdfsTransport transport;
  private final List<String> endpoints;
  private final String httpScheme;
  private final String basePath;
  private final String simpleUser;
  private final boolean kerberos;
  private final HdfsKerberosSession kerberosSession;
  private final ExecutorService executor;
  private final ObjectMapper mapper = HopJson.newMapper();

  public HdfsWebHdfsClient(
      CloseableHttpClient httpClient,
      HdfsTransport transport,
      List<String> endpoints,
      String httpScheme,
      String basePath,
      String simpleUser,
      boolean kerberos,
      HdfsKerberosSession kerberosSession,
      ExecutorService executor) {
    this.httpClient = httpClient;
    this.transport = transport;
    this.endpoints = endpoints;
    this.httpScheme = httpScheme;
    String prefix = basePath == null || basePath.isBlank() ? DEFAULT_BASE_PATH : basePath.trim();
    if (prefix.endsWith("/")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }
    if (!prefix.startsWith("/")) {
      prefix = "/" + prefix;
    }
    this.basePath = prefix;
    this.simpleUser = simpleUser;
    this.kerberos = kerberos;
    this.kerberosSession = kerberosSession;
    this.executor = executor;
  }

  public HdfsFileStatus getFileStatus(String path) throws IOException {
    String body = executeString("GET", path, Map.of("op", "GETFILESTATUS"), null);
    JsonNode status = mapper.readTree(body).path("FileStatus");
    if (status.isMissingNode()) {
      throw new IOException("GETFILESTATUS returned no FileStatus for " + path);
    }
    return readStatus(status);
  }

  public List<HdfsFileStatus> listStatus(String path) throws IOException {
    String body = executeString("GET", path, Map.of("op", "LISTSTATUS"), null);
    JsonNode array = mapper.readTree(body).path("FileStatuses").path("FileStatus");
    List<HdfsFileStatus> result = new ArrayList<>();
    if (array.isArray()) {
      for (JsonNode node : array) {
        result.add(readStatus(node));
      }
    }
    return result;
  }

  public void mkdirs(String path) throws IOException {
    executeString("PUT", path, Map.of("op", "MKDIRS"), null);
  }

  public void delete(String path, boolean recursive) throws IOException {
    executeString(
        "DELETE", path, Map.of("op", "DELETE", "recursive", Boolean.toString(recursive)), null);
  }

  public void rename(String source, String destination) throws IOException {
    executeString("PUT", source, Map.of("op", "RENAME", "destination", destination), null);
  }

  public InputStream open(String path) throws IOException {
    return executeStream("GET", path, Map.of("op", "OPEN"));
  }

  /**
   * Start a CREATE. Bytes written to the returned stream are sent as the PUT body. Close the stream
   * to finish the upload.
   */
  public OutputStream create(String path, boolean overwrite) throws IOException {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("op", "CREATE");
    params.put("overwrite", Boolean.toString(overwrite));
    if (transport.dataOnCreateRequest()) {
      params.put("data", "true");
      String uri = firstWorkingUri(path, params);
      return HdfsStreamingOutputStream.start(executor, in -> putStream(uri, in), path);
    }
    params.put("noredirect", "true");
    String body = executeString("PUT", path, params, null);
    String location = locationFromCreate(body);
    if (location == null || location.isBlank()) {
      throw new IOException("WebHDFS CREATE did not return a DataNode Location for " + path);
    }
    return HdfsStreamingOutputStream.start(executor, in -> putStream(location, in), path);
  }

  private String locationFromCreate(String body) throws IOException {
    if (body == null || body.isBlank()) {
      return null;
    }
    JsonNode node = mapper.readTree(body);
    if (node.hasNonNull("Location")) {
      return node.get("Location").asText();
    }
    return null;
  }

  private HdfsFileStatus readStatus(JsonNode node) {
    HdfsFileStatus status = new HdfsFileStatus();
    status.setPathSuffix(node.path("pathSuffix").asText(""));
    status.setType(node.path("type").asText("FILE"));
    status.setLength(node.path("length").asLong(0));
    status.setModificationTime(node.path("modificationTime").asLong(0));
    return status;
  }

  private void putStream(String uri, InputStream body) throws IOException {
    HttpPut put = new HttpPut(uri);
    put.setEntity(new InputStreamEntity(body, ContentType.APPLICATION_OCTET_STREAM));
    put.setHeader("Content-Type", "application/octet-stream");
    addSpnego(put, uri);
    executeRequest(put);
  }

  private InputStream executeStream(String method, String path, Map<String, String> params)
      throws IOException {
    IOException last = null;
    for (String endpoint : endpoints) {
      try {
        String uri = buildUri(endpoint, path, params);
        HttpUriRequestBase request = request(method, uri);
        return privileged(
            () -> {
              CloseableHttpResponse response = httpClient.execute(request);
              int code = response.getCode();
              if (code >= 400) {
                try {
                  String error = readBody(response);
                  throw new IOException(errorMessage(method, uri, code, error));
                } finally {
                  response.close();
                }
              }
              HttpEntity entity = response.getEntity();
              if (entity == null) {
                response.close();
                throw new IOException("No entity for " + uri);
              }
              return new FilterInputStream(entity.getContent()) {
                @Override
                public void close() throws IOException {
                  try {
                    super.close();
                  } finally {
                    response.close();
                  }
                }
              };
            });
      } catch (IOException e) {
        last = e;
        LogChannel.GENERAL.logDebug("HDFS VFS: " + endpoint + " failed: " + e.getMessage());
      } catch (Exception e) {
        last = new IOException(e);
      }
    }
    throw last == null ? new IOException("No HDFS endpoints configured") : last;
  }

  private String executeString(String method, String path, Map<String, String> params, byte[] body)
      throws IOException {
    IOException last = null;
    for (String endpoint : endpoints) {
      try {
        String uri = buildUri(endpoint, path, params);
        HttpUriRequestBase request = request(method, uri);
        if (body != null) {
          request.setEntity(
              EntityBuilder.create()
                  .setBinary(body)
                  .setContentType(ContentType.APPLICATION_OCTET_STREAM)
                  .build());
        }
        return executeRequest(request);
      } catch (IOException e) {
        last = e;
        LogChannel.GENERAL.logDebug("HDFS VFS: " + endpoint + " failed: " + e.getMessage());
      }
    }
    throw last == null ? new IOException("No HDFS endpoints configured") : last;
  }

  private String firstWorkingUri(String path, Map<String, String> params) throws IOException {
    IOException last = null;
    for (String endpoint : endpoints) {
      try {
        return buildUri(endpoint, path, params);
      } catch (IOException e) {
        last = e;
      }
    }
    throw last == null ? new IOException("No HDFS endpoints configured") : last;
  }

  private String executeRequest(ClassicHttpRequest request) throws IOException {
    try {
      return privileged(
          () ->
              httpClient.execute(
                  request,
                  response -> {
                    int code = response.getCode();
                    String body = readBody(response);
                    if (code >= 400) {
                      throw new IOException(
                          errorMessage(request.getMethod(), request.getRequestUri(), code, body));
                    }
                    return body == null ? "" : body;
                  }));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private <T> T privileged(PrivilegedExceptionAction<T> action) throws Exception {
    if (kerberos && kerberosSession != null) {
      return kerberosSession.doAs(action);
    }
    return action.run();
  }

  private HttpUriRequestBase request(String method, String uri) throws IOException {
    HttpUriRequestBase request =
        switch (method) {
          case "GET" -> new HttpGet(uri);
          case "PUT" -> new HttpPut(uri);
          case "DELETE" -> new HttpDelete(uri);
          default -> throw new IOException("Unsupported HTTP method " + method);
        };
    addSpnego(request, uri);
    return request;
  }

  private void addSpnego(HttpUriRequestBase request, String uri) throws IOException {
    if (!kerberos || kerberosSession == null) {
      return;
    }
    try {
      request.setHeader("Authorization", HdfsSpnego.authorizationHeader(hostOf(uri)));
    } catch (GSSException | URISyntaxException e) {
      throw new IOException("SPNEGO token failed for " + uri, e);
    }
  }

  private static String hostOf(String uri) throws URISyntaxException {
    URI parsed = new URI(uri);
    String host = parsed.getHost();
    return host == null ? "" : host;
  }

  String buildUri(String endpoint, String path, Map<String, String> params) throws IOException {
    String encodedPath = encodePath(path);
    StringBuilder query = new StringBuilder();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (query.length() > 0) {
        query.append('&');
      }
      query.append(urlEncode(entry.getKey())).append('=').append(urlEncode(entry.getValue()));
    }
    if (!kerberos && simpleUser != null && !simpleUser.isBlank()) {
      if (query.length() > 0) {
        query.append('&');
      }
      query.append("user.name=").append(urlEncode(simpleUser));
    }
    String hostPort = endpoint;
    if (!hostPort.contains("://")) {
      hostPort = httpScheme + "://" + hostPort;
    }
    return hostPort + basePath + encodedPath + "?" + query;
  }

  static String encodePath(String path) {
    if (path == null || path.isEmpty() || "/".equals(path)) {
      return "/";
    }
    String normalized = path.startsWith("/") ? path : "/" + path;
    StringBuilder encoded = new StringBuilder();
    for (String segment : normalized.split("/")) {
      if (segment.isEmpty()) {
        continue;
      }
      encoded.append('/').append(urlEncode(segment).replace("+", "%20"));
    }
    return encoded.length() == 0 ? "/" : encoded.toString();
  }

  private static String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private String readBody(ClassicHttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    if (entity == null) {
      return "";
    }
    try {
      return EntityUtils.toString(entity, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private String errorMessage(String method, String uri, int code, String body) {
    String detail = body;
    try {
      JsonNode exception = mapper.readTree(body).path("RemoteException");
      if (!exception.isMissingNode()) {
        detail = exception.path("message").asText(body);
      }
    } catch (Exception ignored) {
      // keep raw body
    }
    return BaseMessages.getString(
        PKG, "Hdfs.Error.Http", method, uri, Integer.toString(code), detail);
  }

  public static String joinHostPort(String host, int port) {
    if (host.contains(":")) {
      return host;
    }
    return host + ":" + port;
  }

  public static List<String> endpointList(String primaryHost, int port, String extra) {
    List<String> list = new ArrayList<>();
    if (primaryHost != null && !primaryHost.isBlank()) {
      list.add(joinHostPort(primaryHost.trim(), port));
    }
    if (extra != null) {
      for (String line : extra.split("\\R")) {
        String trimmed = line.trim();
        if (!trimmed.isEmpty() && !list.contains(trimmed)) {
          list.add(trimmed.contains(":") ? trimmed : joinHostPort(trimmed, port));
        }
      }
    }
    return list;
  }
}
