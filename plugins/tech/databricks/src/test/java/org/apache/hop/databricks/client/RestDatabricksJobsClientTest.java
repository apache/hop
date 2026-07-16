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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.net.http.HttpClient;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestDatabricksJobsClientTest {

  private WireMockServer server;
  private RestDatabricksJobsClient client;

  @BeforeEach
  void setUp() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    WireMock.configureFor("localhost", server.port());
    client =
        RestDatabricksJobsClient.createForTest(
            "http://localhost:" + server.port(),
            "/api/2.1",
            "test-token",
            HttpClient.newHttpClient());
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void normalizeHostAddsHttps() {
    assertEquals(
        "https://my.databricks.net", RestDatabricksJobsClient.normalizeHost("my.databricks.net"));
    assertEquals(
        "https://my.databricks.net",
        RestDatabricksJobsClient.normalizeHost("https://my.databricks.net/"));
  }

  @Test
  void sanitizeErrorRedactsBearer() {
    String s = RestDatabricksJobsClient.sanitizeError("error bearer abc.def.ghi more");
    assertTrue(s.contains("***"));
    assertFalse(s.contains("abc.def.ghi"));
  }

  @Test
  void testConnectionUsesScimMe() throws Exception {
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/preview/scim/v2/Me"))
            .withHeader("Authorization", WireMock.equalTo("Bearer test-token"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"userName\":\"alice@example.com\"}")));

    assertEquals("alice@example.com", client.testConnection());
  }

  @Test
  void runNowReturnsRunId() throws Exception {
    server.stubFor(
        WireMock.post(WireMock.urlEqualTo("/api/2.1/jobs/run-now"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"run_id\": 99, \"number_in_job\": 1}")));

    long runId = client.runNow(42L, Map.of("k", "v"));
    assertEquals(99L, runId);
  }

  @Test
  void getRunParsesState() throws Exception {
    server.stubFor(
        WireMock.get(WireMock.urlPathEqualTo("/api/2.1/jobs/runs/get"))
            .withQueryParam("run_id", WireMock.equalTo("7"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {
                          "run_id": 7,
                          "job_id": 3,
                          "run_page_url": "https://example/runs/7",
                          "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "SUCCESS",
                            "state_message": "ok"
                          }
                        }
                        """)));

    DatabricksRunStatus status = client.getRun(7L);
    assertEquals(7L, status.getRunId());
    assertEquals(3L, status.getJobId());
    assertTrue(status.isSuccess());
    assertEquals("SUCCESS", status.toStatusVariable());
    assertEquals("https://example/runs/7", status.getRunPageUrl());
  }

  @Test
  void httpErrorSurfacesStatus() {
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/preview/scim/v2/Me"))
            .willReturn(
                WireMock.aResponse().withStatus(401).withBody("{\"error\":\"unauthorized\"}")));
    server.stubFor(
        WireMock.get(WireMock.urlPathEqualTo("/api/2.1/jobs/list"))
            .willReturn(
                WireMock.aResponse().withStatus(401).withBody("{\"error\":\"unauthorized\"}")));

    HopException ex = assertThrows(HopException.class, () -> client.testConnection());
    assertTrue(ex.getMessage().contains("401"));
  }

  @Test
  void createJobReturnsJobId() throws Exception {
    server.stubFor(
        WireMock.post(WireMock.urlEqualTo("/api/2.1/jobs/create"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"job_id\": 55}")));
    assertEquals(55L, client.createJob("{\"name\":\"n\",\"tasks\":[]}"));
  }

  @Test
  void lifeCycleTerminalHelpers() {
    assertTrue(DatabricksRunLifeCycleState.TERMINATED.isTerminal());
    assertFalse(DatabricksRunLifeCycleState.RUNNING.isTerminal());
    assertEquals(
        DatabricksRunLifeCycleState.PENDING, DatabricksRunLifeCycleState.fromApi("pending"));
  }
}
