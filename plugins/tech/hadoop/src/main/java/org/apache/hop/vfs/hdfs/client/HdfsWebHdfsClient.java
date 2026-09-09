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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.security.auth.login.LoginException;
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
        logEndpointFailure(endpoint, e);
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
        logEndpointFailure(endpoint, e);
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
    String host;
    try {
      host = hostOf(uri);
    } catch (URISyntaxException e) {
      throw new IOException("SPNEGO token failed for " + uri, e);
    }
    try {
      // GSS reads the TGT from the current Subject. useSubjectCredsOnly=true, so this
      // must run inside session.doAs, not on the calling thread after a separate login.
      String header = privileged(() -> HdfsSpnego.authorizationHeader(host));
      request.setHeader("Authorization", header);
    } catch (GSSException e) {
      throw new IOException(
          "SPNEGO token failed for HTTP@" + host + " at " + uri + ": " + e.getMessage(), e);
    } catch (LoginException e) {
      throw new IOException(
          "Kerberos login failed before SPNEGO for HTTP@" + host + ": " + e.getMessage(), e);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(
          "SPNEGO token failed for HTTP@" + host + " at " + uri + ": " + e.getMessage(), e);
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

  /**
   * Hostname part of {@code host:port} (or a URL). Used so TLS/SPNEGO probes hit the first HA host
   * when the field contains {@code master1,master2}.
   */
  public static String hostOfEndpoint(String endpoint) {
    if (endpoint == null || endpoint.isBlank()) {
      return "";
    }
    String hostPort = endpoint.trim();
    int scheme = hostPort.indexOf("://");
    if (scheme >= 0) {
      hostPort = hostPort.substring(scheme + 3);
    }
    int slash = hostPort.indexOf('/');
    if (slash >= 0) {
      hostPort = hostPort.substring(0, slash);
    }
    int colon = hostPort.lastIndexOf(':');
    if (colon > 0 && hostPort.substring(colon + 1).chars().allMatch(Character::isDigit)) {
      return hostPort.substring(0, colon);
    }
    return hostPort;
  }

  /**
   * True when the NameNode refused the call because it is HA standby ({@code
   * https://s.apache.org/sbnn-error}).
   */
  public static boolean isStandbyNameNode(Throwable error) {
    Throwable current = error;
    while (current != null) {
      String message = current.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("state standby") || lower.contains("sbnn-error")) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Build the failover list. {@code primaryHost} accepts one host or an Impala-style comma (or
   * semicolon / whitespace) separated HA pair. {@code extra} is optional extra {@code host:port}
   * lines.
   */
  public static List<String> endpointList(String primaryHost, int port, String extra) {
    List<String> list = new ArrayList<>();
    addEndpoints(list, primaryHost, port);
    addEndpoints(list, extra, port);
    return list;
  }

  private static void addEndpoints(List<String> list, String spec, int port) {
    if (spec == null || spec.isBlank()) {
      return;
    }
    for (String token : spec.split("[,;\\s]+")) {
      String trimmed = token.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      String endpoint = trimmed.contains(":") ? trimmed : joinHostPort(trimmed, port);
      if (!list.contains(endpoint)) {
        list.add(endpoint);
      }
    }
  }

  private void logEndpointFailure(String endpoint, IOException error) {
    String message =
        isStandbyNameNode(error)
            ? "HDFS VFS: " + endpoint + " is a standby NameNode, trying the next HTTP endpoint"
            : "HDFS VFS: " + endpoint + " failed: " + error.getMessage();
    try {
      if (isStandbyNameNode(error)) {
        LogChannel.GENERAL.logBasic(message);
      } else {
        LogChannel.GENERAL.logDebug(message);
      }
    } catch (RuntimeException ignored) {
      // HopLogStore is not started in some unit tests
    }
  }
}
