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
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        get(urlPathEqualTo("/api/v2/dags/my_dag/dagRuns/r1/taskInstances"))
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

  @Test
  void findsADagOrNothingWhenAirflowDoesNotKnowIt() throws Exception {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/known/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"known\",\"is_paused\":true,\"doc_md\":\"doc\","
                            + "\"timetable_summary\":null}")));
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/daily/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"daily\",\"is_paused\":false,\"doc_md\":null,"
                            + "\"timetable_summary\":\"0 6 * * *\"}")));
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/unknown/details"))
            .willReturn(
                aResponse()
                    .withStatus(404)
                    .withBody("{\"detail\":\"The Dag with ID: `unknown` was not found\"}")));

    AirflowClient client = client(AirflowAuthenticationType.NONE, null);
    assertEquals(new AirflowDag("known", true, "doc", false), client.findDag("known"));
    assertEquals(new AirflowDag("daily", false, null, true), client.findDag("daily"));
    assertNull(client.findDag("unknown"));
  }

  @Test
  void unpausesOnlyThePausedFlag() throws Exception {
    server.stubFor(
        patch(urlEqualTo("/api/v2/dags/my_dag?update_mask=is_paused"))
            .withRequestBody(equalToJson("{\"is_paused\":false}"))
            .willReturn(aResponse().withBody("{\"dag_id\":\"my_dag\",\"is_paused\":false}")));

    client(AirflowAuthenticationType.NONE, null).setDagPaused("my_dag", false);

    server.verify(1, patchRequestedFor(urlEqualTo("/api/v2/dags/my_dag?update_mask=is_paused")));
  }

  @Test
  void setsTheStateOfADagRun() throws Exception {
    String path = "/api/v2/dags/my_dag/dagRuns/manual__2026-11-22T13%3A57%3A20%2B00%3A00";
    server.stubFor(
        patch(urlEqualTo(path))
            .withRequestBody(equalToJson("{\"state\":\"failed\"}"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"manual__2026-11-22T13:57:20+00:00\","
                            + "\"state\":\"failed\"}")));

    AirflowDagRun run =
        client(AirflowAuthenticationType.NONE, null)
            .setDagRunState("my_dag", "manual__2026-11-22T13:57:20+00:00", "failed");

    assertTrue(run.isFailed());
  }

  @Test
  void findsATaskInstanceOrNothingBeforeTheSchedulerCreatesIt() throws Exception {
    server.stubFor(
        get(urlPathEqualTo("/api/v2/dags/my_dag/dagRuns/r1/taskInstances"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"task_instances\":[{\"task_id\":\"run_workflow\",\"state\":\"running\","
                            + "\"try_number\":2,\"map_index\":-1}],\"total_entries\":1}")));
    server.stubFor(
        get(urlPathEqualTo("/api/v2/dags/my_dag/dagRuns/r2/taskInstances"))
            .willReturn(aResponse().withBody("{\"task_instances\":[],\"total_entries\":0}")));

    AirflowClient client = client(AirflowAuthenticationType.NONE, null);
    assertEquals(
        new AirflowTaskInstance("run_workflow", "running", 2, null, null),
        client.findTaskInstance("my_dag", "r1", "run_workflow"));
    assertNull(client.findTaskInstance("my_dag", "r2", "run_workflow"));
  }

  /** Airflow returns at most 100 task instances per page by default. */
  @Test
  void readsEveryPageOfTaskInstances() throws Exception {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag/dagRuns/r1/taskInstances?limit=100&offset=0"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"task_instances\":[{\"task_id\":\"b\",\"state\":\"success\","
                            + "\"try_number\":1,\"start_date\":\"2026-11-23T10:00:05Z\"}],"
                            + "\"total_entries\":2}")));
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag/dagRuns/r1/taskInstances?limit=100&offset=1"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"task_instances\":[{\"task_id\":\"a\",\"state\":\"success\","
                            + "\"try_number\":1,\"start_date\":\"2026-11-23T10:00:01Z\"}],"
                            + "\"total_entries\":2}")));

    List<AirflowTaskInstance> instances =
        client(AirflowAuthenticationType.NONE, null).getTaskInstances("my_dag", "r1");

    assertEquals(List.of("a", "b"), instances.stream().map(AirflowTaskInstance::taskId).toList());
  }

  @Test
  void readsTheTaskLogWithoutTheFileGroupAndContinuesWithTheToken() throws Exception {
    String path = "/api/v2/dags/my_dag/dagRuns/r1/taskInstances/run_workflow/logs/1";
    server.stubFor(
        get(urlEqualTo(path))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"content\":["
                            + "{\"event\":\"::group::Log message source details\"},"
                            + "{\"event\":\"/opt/airflow/logs/dag_id=my_dag/attempt=1.log\"},"
                            + "{\"event\":\"::endgroup::\"},"
                            + "{\"timestamp\":\"2026-11-22T13:56:57Z\",\"event\":\"2026/11/22 13:56:57 - hello"
                            + " - Starting action [Greet]\",\"level\":\"info\",\"logger\":\"task.stdout\"},"
                            + "{\"timestamp\":\"2026-11-22T13:56:58Z\",\"event\":\"Task failed\","
                            + "\"level\":\"error\",\"logger\":\"airflow.sdk\"},"
                            + "{\"event\":\"Error executing task\",\"level\":\"error\","
                            + "\"logger\":\"org.apache.airflow.sdk.execution.TaskRunner\","
                            + "\"error\":\"java.lang.IllegalStateException: [Mail] does not run\"}],"
                            + "\"continuation_token\":\"tok/1+\"}")));
    server.stubFor(
        get(urlEqualTo(path + "?token=tok%2F1%2B"))
            .willReturn(aResponse().withBody("{\"content\":[],\"continuation_token\":null}")));

    AirflowClient client = client(AirflowAuthenticationType.NONE, null);
    AirflowTaskLog log = client.getTaskLog("my_dag", "r1", "run_workflow", 1, null);
    assertEquals(
        List.of(
            "2026/11/22 13:56:57 - hello - Starting action [Greet]",
            "[error] airflow.sdk: Task failed",
            "[error] org.apache.airflow.sdk.execution.TaskRunner: Error executing task:"
                + " java.lang.IllegalStateException: [Mail] does not run"),
        log.lines());
    assertEquals("tok/1+", log.continuationToken());

    AirflowTaskLog next = client.getTaskLog("my_dag", "r1", "run_workflow", 1, "tok/1+");
    assertTrue(next.lines().isEmpty());
    assertNull(next.continuationToken());
  }

  @Test
  void readsAnXComValueOrNothingWhenTheTaskPushedNone() throws Exception {
    String path = "/api/v2/dags/my_dag/dagRuns/r1/taskInstances/run_workflow/xcomEntries/";
    server.stubFor(
        get(urlEqualTo(path + "return_value"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"key\":\"return_value\",\"value\":{\"success\":true,"
                            + "\"counters\":{\"errors\":0}}}")));
    server.stubFor(
        get(urlEqualTo(path + "other"))
            .willReturn(aResponse().withStatus(404).withBody("{\"detail\":\"not found\"}")));

    AirflowClient client = client(AirflowAuthenticationType.NONE, null);
    Object value = client.findXComValue("my_dag", "r1", "run_workflow", "return_value");
    assertTrue(value instanceof Map<?, ?> map && Boolean.TRUE.equals(map.get("success")));
    assertNull(client.findXComValue("my_dag", "r1", "run_workflow", "other"));
  }

  @Test
  void stillFailsOnOtherErrorsWhereNotFoundIsAllowed() {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/broken/details"))
            .willReturn(aResponse().withStatus(500).withBody("{\"detail\":\"boom\"}")));

    HopException error =
        assertThrows(
            HopException.class,
            () -> client(AirflowAuthenticationType.NONE, null).findDag("broken"));
    assertTrue(error.getMessage().contains("HTTP 500"));
  }
}
