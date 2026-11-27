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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SparkStandaloneRestSubmitterTest {

  private HttpServer server;
  private final Map<String, String> requests = new ConcurrentHashMap<>();
  private volatile String createAnswer =
      "{\"action\":\"CreateSubmissionResponse\",\"success\":true,"
          + "\"submissionId\":\"driver-20261113-0001\"}";
  private volatile String statusAnswer =
      "{\"action\":\"SubmissionStatusResponse\",\"driverState\":\"FAILED\","
          + "\"workerHostPort\":\"172.20.0.3:40000\",\"success\":true}";

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/v1/submissions/create", exchange -> answer(exchange, "create", createAnswer, 200));
    server.createContext(
        "/v1/submissions/status/", exchange -> answer(exchange, "status", statusAnswer, 200));
    server.createContext(
        "/v1/submissions/kill/",
        exchange ->
            answer(
                exchange,
                "kill",
                "{\"action\":\"KillSubmissionResponse\",\"message\":\"Kill request submitted\","
                    + "\"success\":true}",
                200));
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void derivesTheRestPortFromTheMaster() throws Exception {
    assertEquals(
        "http://spark:6066", SparkStandaloneRestSubmitter.deriveSubmitUrl("spark://spark:7077"));
    assertEquals(
        "http://a:6066", SparkStandaloneRestSubmitter.deriveSubmitUrl("spark://a:7077,b:7077"));
    assertThrows(HopException.class, () -> SparkStandaloneRestSubmitter.deriveSubmitUrl("yarn"));
    assertThrows(HopException.class, () -> SparkStandaloneRestSubmitter.deriveSubmitUrl(""));
  }

  @Test
  void submitsTheSparkRestProtocol() throws Exception {
    String id =
        submitter()
            .submit(
                "http://minio:9000/hop-staging/hop-native-abc.jar",
                "org.apache.hop.spark.run.MainSpark",
                List.of("--HopPipelinePath=a.hpl"),
                Map.of("spark.master", "spark://spark:7077", "spark.app.name", "test"),
                Map.of());
    assertEquals("driver-20261113-0001", id);

    JsonNode request = new ObjectMapper().readTree(requests.get("create"));
    assertEquals("CreateSubmissionRequest", request.get("action").asText());
    assertEquals("org.apache.hop.spark.run.MainSpark", request.get("mainClass").asText());
    assertEquals("4.1.3", request.get("clientSparkVersion").asText());
    assertEquals("--HopPipelinePath=a.hpl", request.get("appArgs").get(0).asText());
    assertEquals("test", request.get("sparkProperties").get("spark.app.name").asText());
  }

  @Test
  void aRefusedSubmissionFails() {
    createAnswer =
        "{\"action\":\"ErrorResponse\",\"success\":false,\"message\":\"Malformed request\"}";
    HopException e =
        assertThrows(
            HopException.class,
            () -> submitter().submit("x.jar", "Main", List.of(), Map.of(), Map.of()));
    assertTrue(e.getMessage().contains("Malformed request"), e.getMessage());
  }

  @Test
  void readsAndKillsADriver() throws Exception {
    SparkDriverStatus status = submitter().status("driver-1");
    assertEquals("FAILED", status.state());
    assertEquals("172.20.0.3:40000", status.location());
    assertTrue(status.isFinal());
    assertFalse(status.isSuccess());

    assertEquals("Kill request submitted", submitter().kill("driver-1"));
    assertEquals("", requests.get("kill"));
  }

  /**
   * A master that no longer knows the driver (restarted without recovery, or it dropped the driver
   * from its retained drivers) answers success false without a state: a failed status call, which
   * the run retries only for a while, not a driver that is still running.
   */
  @Test
  void aDriverTheMasterDoesNotKnowIsAFailedStatusCall() {
    statusAnswer =
        "{\"action\":\"SubmissionStatusResponse\",\"success\":false,"
            + "\"submissionId\":\"driver-gone\",\"message\":\"Driver driver-gone not found\"}";
    HopException e = assertThrows(HopException.class, () -> submitter().status("driver-gone"));
    assertTrue(e.getMessage().contains("does not know driver driver-gone"), e.getMessage());
    assertTrue(e.getMessage().contains("Driver driver-gone not found"), e.getMessage());
  }

  @Test
  void anUnreachableMasterSaysWhatToCheck() {
    server.stop(0);
    HopException e = assertThrows(HopException.class, () -> submitter().status("driver-1"));
    assertTrue(e.getMessage().contains("spark.master.rest.enabled"), e.getMessage());
  }

  private SparkStandaloneRestSubmitter submitter() {
    return new SparkStandaloneRestSubmitter(
        "http://127.0.0.1:" + server.getAddress().getPort() + "/", "4.1.3");
  }

  private void answer(HttpExchange exchange, String key, String body, int code) throws IOException {
    requests.put(key, new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(code, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
