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

package org.apache.hop.spark.submit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.cluster.ISparkClusterSubmitter;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fatjar.VfsStagingStore;

/**
 * Submits a driver to a Spark standalone master through its REST submission server ({@code
 * spark.master.rest.enabled=true}, port 6066), and reads and kills it there.
 *
 * <p>Plain HTTP: the Hop process needs no Spark distribution and no {@code SPARK_HOME}.
 */
public class SparkStandaloneRestSubmitter implements ISparkClusterSubmitter {

  /** Driver states after which the driver no longer runs. */
  static final Set<String> FINAL_STATES = Set.of("FINISHED", "FAILED", "ERROR", "KILLED");

  /** Default port of the standalone REST submission server. */
  static final int DEFAULT_REST_PORT = 6066;

  private final String baseUrl;
  private final String sparkVersion;
  private final HttpClient client;
  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * @param baseUrl for example {@code http://spark:6066}
   * @param sparkVersion the client Spark version the master checks against its own
   */
  public SparkStandaloneRestSubmitter(String baseUrl, String sparkVersion) {
    this.baseUrl = VfsStagingStore.trimSlash(baseUrl);
    this.sparkVersion = sparkVersion;
    this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
  }

  /**
   * The REST submission URL of a standalone master URL: {@code spark://host:7077} gives {@code
   * http://host:6066}. With several masters ({@code spark://a:7077,b:7077}) the first one is used.
   */
  public static String deriveSubmitUrl(String master) throws HopException {
    if (StringUtils.isBlank(master) || !master.trim().startsWith("spark://")) {
      throw new HopException(
          "Cluster mode supports a Spark standalone master (spark://host:port) for now, got '"
              + master
              + "'. Set the submit URL of the run configuration for a master behind another"
              + " address.");
    }
    String hosts = master.trim().substring("spark://".length());
    String first = hosts.split(",")[0];
    int colon = first.lastIndexOf(':');
    String host = colon > 0 ? first.substring(0, colon) : first;
    return "http://" + host + ":" + DEFAULT_REST_PORT;
  }

  /**
   * @return the submission id ({@code driver-...})
   */
  @Override
  public String submit(
      String appResource,
      String mainClass,
      List<String> appArgs,
      Map<String, String> sparkProperties,
      Map<String, String> environmentVariables)
      throws HopException {
    ObjectNode request = mapper.createObjectNode();
    request.put("action", "CreateSubmissionRequest");
    request.put("appResource", appResource);
    request.put("mainClass", mainClass);
    request.put("clientSparkVersion", sparkVersion);
    request.set("appArgs", mapper.valueToTree(appArgs));
    request.set("sparkProperties", mapper.valueToTree(sparkProperties));
    request.set("environmentVariables", mapper.valueToTree(environmentVariables));

    JsonNode response = post("/v1/submissions/create", request.toString());
    String id = text(response, "submissionId");
    if (!response.path("success").asBoolean(false) || StringUtils.isEmpty(id)) {
      throw new HopException(
          "The Spark master at "
              + baseUrl
              + " refused the submission: "
              + StringUtils.defaultIfEmpty(text(response, "message"), response.toString()));
    }
    return id;
  }

  @Override
  public SparkDriverStatus status(String submissionId) throws HopException {
    JsonNode response = send(HttpRequest.newBuilder(uri("/v1/submissions/status/" + submissionId)));
    String state = text(response, "driverState");
    if (!response.path("success").asBoolean(false) || state == null) {
      // The master no longer knows the driver (restarted without recovery, or retainedDrivers
      // dropped it): a failure to read the status, which the caller retries only for a while
      throw new HopException(
          "The Spark master at "
              + baseUrl
              + " does not know driver "
              + submissionId
              + ": "
              + StringUtils.defaultIfEmpty(text(response, "message"), response.toString()));
    }
    return new SparkDriverStatus(
        state,
        text(response, "workerHostPort"),
        text(response, "message"),
        state != null && FINAL_STATES.contains(state),
        "FINISHED".equals(state));
  }

  /**
   * @return the master's answer, for the log
   */
  @Override
  public String kill(String submissionId) throws HopException {
    JsonNode response = post("/v1/submissions/kill/" + submissionId, "");
    return StringUtils.defaultIfEmpty(text(response, "message"), response.toString());
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  @Override
  public String describe() {
    return baseUrl;
  }

  private JsonNode post(String path, String body) throws HopException {
    return send(
        HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json;charset=UTF-8")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)));
  }

  private JsonNode send(HttpRequest.Builder builder) throws HopException {
    HttpRequest request = builder.timeout(Duration.ofSeconds(60)).build();
    try {
      HttpResponse<String> response =
          client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      String body = response.body();
      if (StringUtils.isBlank(body)) {
        throw new HopException(
            "Empty answer (HTTP " + response.statusCode() + ") from " + request.uri());
      }
      // The master answers errors with a JSON ErrorResponse and an HTTP 4xx/5xx status
      return mapper.readTree(body);
    } catch (HopException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted calling " + request.uri(), e);
    } catch (Exception e) {
      throw new HopException(
          "Unable to reach the Spark REST submission server at "
              + request.uri()
              + ". Is spark.master.rest.enabled=true set on the master? "
              + e.getMessage(),
          e);
    }
  }

  private URI uri(String path) {
    return URI.create(baseUrl + path);
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }
}
