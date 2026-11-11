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

package org.apache.hop.lineage.datahub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.openlineage.OpenLineageEmitter;
import org.apache.hop.lineage.openlineage.OpenLineageSinkConfig;

/**
 * The two DataHub GMS endpoints the sink uses: the Rest.li {@code ingestProposalBatch} action that
 * writes metadata change proposals, and GraphQL to read what DataHub already holds. Both take the
 * same personal access token, which is what DataHub's own Java and Python emitters use too.
 */
class DataHubClient {

  /** An HTTP answer; {@code status} is {@code -1} when there was no answer at all. */
  record Response(int status, String body) {
    boolean ok() {
      return status / 100 == 2;
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final URI ingestUri;
  private final URI graphqlUri;
  private final String token;
  private final Duration readTimeout;
  private final HttpClient http;

  DataHubClient(String gmsUrl, String token, OpenLineageSinkConfig http) throws HopException {
    this.ingestUri = URI.create(gmsUrl + "/aspects?action=ingestProposalBatch");
    this.graphqlUri = URI.create(gmsUrl + "/api/graphql");
    this.token = token;
    this.readTimeout = Duration.ofMillis(http.getReadTimeoutMs());
    HttpClient.Builder builder =
        HttpClient.newBuilder().connectTimeout(Duration.ofMillis(http.getConnectTimeoutMs()));
    if (!StringUtils.isBlank(http.getTrustStorePath())) {
      builder.sslContext(
          sslContext(
              http.getTrustStorePath(), http.getTrustStorePassword(), http.getKeyStoreType()));
    }
    this.http = builder.build();
  }

  /** Writes the proposals in one request. */
  Response ingest(List<Mcp> proposals) throws InterruptedException {
    ObjectNode body = MAPPER.createObjectNode();
    ArrayNode array = body.putArray("proposals");
    for (Mcp mcp : proposals) {
      ObjectNode proposal = array.addObject();
      proposal.put("entityType", mcp.entityType());
      proposal.put("entityUrn", mcp.entityUrn());
      proposal.put("changeType", mcp.changeType());
      proposal.put("aspectName", mcp.aspectName());
      ObjectNode aspect = proposal.putObject("aspect");
      aspect.put("contentType", "application/json");
      aspect.put("value", mcp.value().toString());
    }
    return post(ingestUri, body);
  }

  /**
   * DataHub could not be read. Worth retrying only when it may answer on its own a little later: it
   * is unreachable, overloaded or failing (no answer, 5xx, 408, 429). A refusal (a token without
   * the privilege to read, a proxy, a query an older DataHub does not know) stays until someone
   * changes something; retrying it would hold up every event behind it, since events are delivered
   * in order.
   */
  static final class ReadException extends IOException {
    private final boolean worthRetrying;

    ReadException(String message, boolean worthRetrying) {
      super(message);
      this.worthRetrying = worthRetrying;
    }

    boolean worthRetrying() {
      return worthRetrying;
    }
  }

  /**
   * Runs a GraphQL query and returns its {@code data}.
   *
   * @throws ReadException when DataHub could not be reached or answered with an error
   */
  JsonNode graphql(String query, ObjectNode variables) throws ReadException, InterruptedException {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("query", query);
    body.set("variables", variables);
    Response response = post(graphqlUri, body);
    if (!response.ok()) {
      throw new ReadException(
          "HTTP "
              + response.status()
              + " from "
              + graphqlUri
              + ": "
              + StringUtils.abbreviate(StringUtils.defaultString(response.body()), 300),
          OpenLineageEmitter.isTransient(response.status()));
    }
    JsonNode answer;
    try {
      answer = MAPPER.readTree(response.body());
    } catch (IOException e) {
      throw new ReadException("Unreadable answer from " + graphqlUri + ": " + e.getMessage(), true);
    }
    JsonNode errors = answer.path("errors");
    if (errors.isArray() && !errors.isEmpty()) {
      // DataHub gives an error the HTTP status it stands for (403 for a token without the
      // privilege, 500 for a failure of its own); a query it cannot run has none.
      boolean worthRetrying = false;
      for (JsonNode error : errors) {
        int code = error.path("extensions").path("code").asInt(0);
        worthRetrying |= code > 0 && OpenLineageEmitter.isTransient(code);
      }
      throw new ReadException("GraphQL error from " + graphqlUri + ": " + errors, worthRetrying);
    }
    return answer.path("data");
  }

  private Response post(URI uri, ObjectNode body) throws InterruptedException {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(uri)
            .timeout(readTimeout)
            .header("Content-Type", "application/json")
            .header("X-RestLi-Protocol-Version", "2.0.0")
            .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));
    if (token != null) {
      request.header("Authorization", "Bearer " + token);
    }
    try {
      HttpResponse<String> response =
          http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      return new Response(response.statusCode(), response.body());
    } catch (IOException e) {
      // A refused or timed-out connection often carries no message of its own.
      return new Response(
          -1,
          "cannot reach "
              + uri
              + " ("
              + e.getClass().getSimpleName()
              + (e.getMessage() == null ? "" : ": " + e.getMessage())
              + ")");
    }
  }

  private static SSLContext sslContext(String path, String password, String type)
      throws HopException {
    try (InputStream in = HopVfs.getInputStream(path)) {
      KeyStore trustStore = KeyStore.getInstance(StringUtils.defaultIfBlank(type, "PKCS12"));
      trustStore.load(in, password == null ? null : password.toCharArray());
      TrustManagerFactory trust =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trust.init(trustStore);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, trust.getTrustManagers(), null);
      return context;
    } catch (Exception e) {
      throw new HopException("Unable to load the trust store '" + path + "' for DataHub", e);
    }
  }
}
