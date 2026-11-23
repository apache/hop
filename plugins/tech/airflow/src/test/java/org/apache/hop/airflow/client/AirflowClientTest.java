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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.airflow.metadata.AirflowAuthenticationType;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AirflowClientTest {

  private WireMockServer server;

  @BeforeEach
  void setUp() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterEach
  void tearDown() {
    server.stop();
  }

  private AirflowClient client(AirflowAuthenticationType type, String token) {
    return new AirflowClient(
        HttpClient.newHttpClient(),
        "http://localhost:" + server.port() + "/",
        type,
        "admin",
        "secret",
        token,
        Duration.ofSeconds(5));
  }

  private void stubToken(String token) {
    server.stubFor(
        post(urlEqualTo("/auth/token"))
            .withRequestBody(equalToJson("{\"username\":\"admin\",\"password\":\"secret\"}"))
            .willReturn(
                aResponse().withStatus(201).withBody("{\"access_token\":\"" + token + "\"}")));
  }

  @Test
  void normalizesTheBaseUrl() {
    assertEquals("http://host:8080", AirflowClient.normalizeBaseUrl(" http://host:8080/ "));
    assertEquals(
        "https://host/airflow", AirflowClient.normalizeBaseUrl("https://host/airflow/api/v2/"));
    assertThrows(IllegalArgumentException.class, () -> AirflowClient.normalizeBaseUrl("host:8080"));
  }

  @Test
  void signsInOnceAndSendsTheToken() throws Exception {
    stubToken("t1");
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .withHeader("Authorization", equalTo("Bearer t1"))
            .willReturn(aResponse().withBody("{\"dag_id\":\"my_dag\",\"is_paused\":true}")));

    AirflowClient client = client(AirflowAuthenticationType.USERNAME_PASSWORD, null);
    assertTrue(client.isDagPaused("my_dag"));
    assertTrue(client.isDagPaused("my_dag"));

    server.verify(1, postRequestedFor(urlEqualTo("/auth/token")));
    server.verify(2, getRequestedFor(urlEqualTo("/api/v2/dags/my_dag")));
  }

  @Test
  void signsInAgainWhenTheTokenExpired() throws Exception {
    server.stubFor(
        post(urlEqualTo("/auth/token"))
            .inScenario("token")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(201).withBody("{\"access_token\":\"old\"}"))
            .willSetStateTo("expired"));
    server.stubFor(
        post(urlEqualTo("/auth/token"))
            .inScenario("token")
            .whenScenarioStateIs("expired")
            .willReturn(aResponse().withStatus(201).withBody("{\"access_token\":\"new\"}")));
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .withHeader("Authorization", equalTo("Bearer old"))
            .willReturn(aResponse().withStatus(401).withBody("{\"detail\":\"Token expired\"}")));
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .withHeader("Authorization", equalTo("Bearer new"))
            .willReturn(aResponse().withBody("{\"is_paused\":false}")));

    assertFalse(client(AirflowAuthenticationType.USERNAME_PASSWORD, null).isDagPaused("my_dag"));
    server.verify(2, postRequestedFor(urlEqualTo("/auth/token")));
  }

  @Test
  void reportsRefusedCredentials() {
    server.stubFor(
        post(urlEqualTo("/auth/token"))
            .willReturn(
                aResponse().withStatus(401).withBody("{\"detail\":\"Invalid credentials\"}")));

    HopException e =
        assertThrows(
            HopException.class,
            () -> client(AirflowAuthenticationType.USERNAME_PASSWORD, null).isDagPaused("my_dag"));
    assertTrue(e.getMessage().contains("Invalid credentials"), e.getMessage());
  }

  @Test
  void sendsAGivenTokenOrNone() throws Exception {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .willReturn(aResponse().withBody("{\"is_paused\":false}")));

    client(AirflowAuthenticationType.TOKEN, "api-token").isDagPaused("my_dag");
    client(AirflowAuthenticationType.NONE, "ignored").isDagPaused("my_dag");

    server.verify(
        getRequestedFor(urlEqualTo("/api/v2/dags/my_dag"))
            .withHeader("Authorization", equalTo("Bearer api-token")));
    server.verify(
        getRequestedFor(urlEqualTo("/api/v2/dags/my_dag")).withHeader("Authorization", absent()));
    server.verify(0, postRequestedFor(urlEqualTo("/auth/token")));
  }

  @Test
  void triggersARunWithConfAndAnEmptyLogicalDate() throws Exception {
    server.stubFor(
        post(urlEqualTo("/api/v2/dags/my_dag/dagRuns"))
            .withRequestBody(
                equalToJson(
                    "{\"logical_date\":null,\"conf\":{\"greeting\":\"hello\",\"count\":3}}"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"manual__1\",\"state\":\"queued\"}")));

    Map<String, Object> conf = new LinkedHashMap<>();
    conf.put("greeting", "hello");
    conf.put("count", 3L);
    AirflowDagRun run =
        client(AirflowAuthenticationType.NONE, null).triggerDagRun("my_dag", "", null, conf);

    assertEquals("manual__1", run.dagRunId());
    assertEquals("queued", run.state());
    assertFalse(run.isFinished());
  }

  @Test
  void triggersARunWithAnIdAndALogicalDate() throws Exception {
    server.stubFor(
        post(urlEqualTo("/api/v2/dags/my_dag/dagRuns"))
            .withRequestBody(
                equalToJson(
                    "{\"dag_run_id\":\"hop-1\",\"logical_date\":\"2026-11-21T10:00:00+01:00\",\"conf\":{}}"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"hop-1\",\"state\":\"queued\"}")));

    AirflowDagRun run =
        client(AirflowAuthenticationType.NONE, null)
            .triggerDagRun("my_dag", "hop-1", "2026-11-21T10:00:00+01:00", null);

    assertEquals("hop-1", run.dagRunId());
  }

  @Test
  void encodesTheRunIdInThePath() throws Exception {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag/dagRuns/manual__2026-11-21T19%3A12%3A01%2B00%3A00"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"manual__2026-11-21T19:12:01+00:00\","
                            + "\"state\":\"success\"}")));

    AirflowDagRun run =
        client(AirflowAuthenticationType.NONE, null)
            .getDagRun("my_dag", "manual__2026-11-21T19:12:01+00:00");

    assertTrue(run.isSuccess());
  }

  @Test
  void listsTheTasksInAState() throws Exception {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag/dagRuns/r1/taskInstances"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"task_instances\":[{\"task_id\":\"extract\",\"state\":\"success\"},"
                            + "{\"task_id\":\"load\",\"state\":\"failed\"},"
                            + "{\"task_id\":\"report\",\"state\":\"upstream_failed\"}]}")));

    List<String> failed =
        client(AirflowAuthenticationType.NONE, null)
            .getTaskIds("my_dag", "r1", Set.of("failed", "upstream_failed"));

    assertEquals(List.of("load", "report"), failed);
  }

  @Test
  void testConnectionChecksTheCredentialsToo() throws Exception {
    server.stubFor(
        get(urlEqualTo("/api/v2/version"))
            .willReturn(aResponse().withBody("{\"version\":\"3.4.0\"}")));
    server.stubFor(
        get(urlEqualTo("/api/v2/dags?limit=1"))
            .willReturn(
                aResponse().withStatus(401).withBody("{\"detail\":\"Not authenticated\"}")));

    AirflowClient client = client(AirflowAuthenticationType.NONE, null);
    HopException e = assertThrows(HopException.class, client::testConnection);
    assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());

    server.stubFor(
        get(urlEqualTo("/api/v2/dags?limit=1"))
            .willReturn(aResponse().withBody("{\"dags\":[],\"total_entries\":0}")));
    assertEquals("3.4.0", client.testConnection());
  }

  @Test
  void rejectsAnAnswerWithoutARunIdAndState() {
    stubToken("t1");
    server.stubFor(
        post(urlEqualTo("/api/v2/dags/my_dag/dagRuns")).willReturn(aResponse().withBody("{}")));
    AirflowClient client = client(AirflowAuthenticationType.USERNAME_PASSWORD, null);

    HopException e =
        assertThrows(HopException.class, () -> client.triggerDagRun("my_dag", null, null, null));
    assertTrue(e.getMessage().contains("without a DAG run id and state"), e.getMessage());
  }

  @Test
  void reportsWhereARedirectPointsTo() {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .willReturn(
                aResponse()
                    .withStatus(308)
                    .withHeader("Location", "https://airflow.example.com/api/v2/dags/my_dag")));
    AirflowClient client = client(AirflowAuthenticationType.NONE, null);

    HopException e = assertThrows(HopException.class, () -> client.isDagPaused("my_dag"));
    assertTrue(
        e.getMessage().contains("redirects to https://airflow.example.com/api/v2/dags/my_dag"),
        e.getMessage());
  }

  @Test
  void reportsTheMissingDag() {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/nope"))
            .willReturn(
                aResponse()
                    .withStatus(404)
                    .withBody("{\"detail\":\"The Dag with ID: `nope` was not found\"}")));

    HopException e =
        assertThrows(
            HopException.class,
            () -> client(AirflowAuthenticationType.NONE, null).isDagPaused("nope"));
    assertTrue(e.getMessage().contains("HTTP 404"), e.getMessage());
    assertTrue(e.getMessage().contains("was not found"), e.getMessage());
  }

  @Test
  void extractsTheErrorDetail() {
    assertEquals(
        "Not authenticated", AirflowClient.errorDetail("{\"detail\":\"Not authenticated\"}"));
    assertEquals(
        "Unique constraint violation",
        AirflowClient.errorDetail(
            "{\"detail\":{\"reason\":\"Unique constraint violation\",\"statement\":\"hidden\"}}"));
    assertEquals(
        "[\"body\",\"logical_date\"]: Field required",
        AirflowClient.errorDetail(
            "{\"detail\":[{\"type\":\"missing\",\"loc\":[\"body\",\"logical_date\"],"
                + "\"msg\":\"Field required\"}]}"));
    assertEquals("Bad Gateway", AirflowClient.errorDetail("Bad Gateway"));
    assertEquals("(no details)", AirflowClient.errorDetail(""));
  }
}
