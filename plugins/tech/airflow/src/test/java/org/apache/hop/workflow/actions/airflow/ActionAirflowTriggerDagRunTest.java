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

package org.apache.hop.workflow.actions.airflow;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.airflow.metadata.AirflowAuthenticationType;
import org.apache.hop.airflow.metadata.AirflowConnection;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ActionAirflowTriggerDagRunTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String RUNS = "/api/v2/dags/my_dag/dagRuns";

  private WireMockServer server;
  private MemoryMetadataProvider metadataProvider;

  @BeforeEach
  void setUp() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    metadataProvider = new MemoryMetadataProvider();
    AirflowConnection connection = new AirflowConnection();
    connection.setName("airflow");
    connection.setUrl("http://localhost:" + server.port());
    connection.setAuthenticationType(AirflowAuthenticationType.USERNAME_PASSWORD);
    connection.setUsername("admin");
    connection.setPassword("secret");
    metadataProvider.getSerializer(AirflowConnection.class).save(connection);

    server.stubFor(
        post(urlEqualTo("/auth/token"))
            .willReturn(aResponse().withStatus(201).withBody("{\"access_token\":\"t\"}")));
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .willReturn(aResponse().withBody("{\"dag_id\":\"my_dag\",\"is_paused\":false}")));
    server.stubFor(
        post(urlEqualTo(RUNS))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"r1\",\"state\":\"queued\"}")));
  }

  @AfterEach
  void tearDown() {
    server.stop();
  }

  private ActionAirflowTriggerDagRun action() {
    ActionAirflowTriggerDagRun action = new ActionAirflowTriggerDagRun("trigger");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("airflow");
    action.setDagId("my_dag");
    action.setPollIntervalSeconds("1");
    return action;
  }

  private void stubRunStates(String... states) {
    String state = Scenario.STARTED;
    for (int i = 0; i < states.length; i++) {
      String next = i == states.length - 1 ? state : "poll-" + (i + 1);
      server.stubFor(
          get(urlEqualTo(RUNS + "/r1"))
              .inScenario("run")
              .whenScenarioStateIs(state)
              .willReturn(
                  aResponse()
                      .withBody(
                          "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"r1\",\"state\":\""
                              + states[i]
                              + "\"}"))
              .willSetStateTo(next));
      state = next;
    }
  }

  @Test
  void succeedsWhenTheRunSucceeds() {
    stubRunStates("running", "success");
    ActionAirflowTriggerDagRun action = action();

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals("r1", action.getVariable("AIRFLOW_DAG_RUN_ID"));
    assertEquals("success", action.getVariable("AIRFLOW_DAG_RUN_STATE"));
    server.verify(2, getRequestedFor(urlEqualTo(RUNS + "/r1")));
  }

  /**
   * Airflow's API server (uvicorn) answers HTTP 400 to a request that asks to upgrade to HTTP/2,
   * which is what the Java HTTP client does by default on a plain http:// URL.
   */
  @Test
  void doesNotAskToUpgradeToHttp2() {
    ActionAirflowTriggerDagRun action = action();
    action.setWaitingForCompletion(false);

    action.execute(new Result(), 0);

    server.verify(postRequestedFor(urlEqualTo("/auth/token")).withHeader("Upgrade", absent()));
    server.verify(postRequestedFor(urlEqualTo(RUNS)).withHeader("Upgrade", absent()));
  }

  @Test
  void failsAndNamesTheFailedTasksWhenTheRunFails() {
    stubRunStates("failed");
    server.stubFor(
        get(urlEqualTo(RUNS + "/r1/taskInstances"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"task_instances\":[{\"task_id\":\"load\",\"state\":\"failed\"}]}")));
    ActionAirflowTriggerDagRun action = action();

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    assertEquals("failed", action.getVariable("AIRFLOW_DAG_RUN_STATE"));
    server.verify(getRequestedFor(urlEqualTo(RUNS + "/r1/taskInstances")));
  }

  @Test
  void succeedsOnceTheRunIsAcceptedWithoutWaiting() {
    ActionAirflowTriggerDagRun action = action();
    action.setWaitingForCompletion(false);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("queued", action.getVariable("AIRFLOW_DAG_RUN_STATE"));
    server.verify(0, getRequestedFor(urlEqualTo(RUNS + "/r1")));
  }

  @Test
  void failsWhenTheWaitTimesOut() {
    stubRunStates("running");
    ActionAirflowTriggerDagRun action = action();
    action.setTimeoutSeconds("1");

    long start = System.currentTimeMillis();
    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals("running", action.getVariable("AIRFLOW_DAG_RUN_STATE"));
    assertTrue(System.currentTimeMillis() - start < 10_000L);
  }

  @Test
  void sendsTheResolvedRunConf() {
    server.stubFor(
        post(urlEqualTo(RUNS))
            .withRequestBody(
                equalToJson(
                    "{\"dag_run_id\":\"hop-7\",\"logical_date\":null,"
                        + "\"conf\":{\"count\":3,\"greeting\":\"hi\",\"source\":\"crm\"}}"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"hop-7\",\"state\":\"queued\"}")));
    ActionAirflowTriggerDagRun action = action();
    action.setWaitingForCompletion(false);
    action.setVariable("RUN", "7");
    action.setVariable("SOURCE", "crm");
    action.setDagRunId("hop-${RUN}");
    action.setConfJson("{\"count\": 3, \"greeting\": \"replaced\"}");
    action.getConfEntries().add(new AirflowConfEntry("greeting", "hi"));
    action.getConfEntries().add(new AirflowConfEntry("source", "${SOURCE}"));
    action.getConfEntries().add(new AirflowConfEntry("", "ignored"));

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("hop-7", action.getVariable("AIRFLOW_DAG_RUN_ID"));
  }

  @Test
  void rejectsAConfThatIsNotAJsonObject() {
    ActionAirflowTriggerDagRun action = action();
    action.setConfJson("[1, 2]");
    assertThrows(HopException.class, action::buildConf);
    action.setConfJson("{not json");
    assertThrows(HopException.class, action::buildConf);
    action.setConfJson("  ");
    action.getConfEntries().add(new AirflowConfEntry("k", "v"));
    assertDoesNotThrowAndEquals(Map.of("k", "v"), action);
  }

  private static void assertDoesNotThrowAndEquals(
      Map<String, Object> expected, ActionAirflowTriggerDagRun action) {
    try {
      assertEquals(expected, action.buildConf());
    } catch (HopException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void failsWithoutTriggeringWhenTheDagDoesNotExist() {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .willReturn(aResponse().withStatus(404).withBody("{\"detail\":\"not found\"}")));
    ActionAirflowTriggerDagRun action = action();

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    server.verify(0, postRequestedFor(urlEqualTo(RUNS)));
  }

  @Test
  void failsWithoutTriggeringWhenTheDagIsPausedAndTheActionWaits() {
    stubPausedDag();

    Result result = action().execute(new Result(), 0);

    assertFalse(result.isResult());
    server.verify(0, postRequestedFor(urlEqualTo(RUNS)));
  }

  @Test
  void triggersAPausedDagWhenTheActionDoesNotWait() {
    stubPausedDag();
    ActionAirflowTriggerDagRun action = action();
    action.setWaitingForCompletion(false);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    server.verify(1, postRequestedFor(urlEqualTo(RUNS)));
  }

  private void stubPausedDag() {
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/my_dag"))
            .willReturn(aResponse().withBody("{\"dag_id\":\"my_dag\",\"is_paused\":true}")));
  }

  /**
   * Java turns a POST that gets a 301 or 302 into a GET. Following the redirect would list the DAG
   * runs, answer 200 and report a run that was never started.
   */
  @Test
  void failsWhenAirflowRedirectsTheTrigger() {
    server.stubFor(
        post(urlEqualTo(RUNS))
            .willReturn(
                aResponse()
                    .withStatus(301)
                    .withHeader("Location", "http://localhost:" + server.port() + RUNS)));
    server.stubFor(
        get(urlEqualTo(RUNS))
            .willReturn(aResponse().withBody("{\"dag_runs\":[],\"total_entries\":0}")));
    ActionAirflowTriggerDagRun action = action();
    action.setWaitingForCompletion(false);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals("", action.getVariable("AIRFLOW_DAG_RUN_ID"));
    server.verify(0, getRequestedFor(urlEqualTo(RUNS)));
  }

  @Test
  void keepsWaitingWhenAStatusCheckFails() {
    server.stubFor(
        get(urlEqualTo(RUNS + "/r1"))
            .inScenario("flaky")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(502).withBody("Bad Gateway"))
            .willSetStateTo("up"));
    server.stubFor(
        get(urlEqualTo(RUNS + "/r1"))
            .inScenario("flaky")
            .whenScenarioStateIs("up")
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"my_dag\",\"dag_run_id\":\"r1\",\"state\":\"success\"}")));
    ActionAirflowTriggerDagRun action = action();

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("success", action.getVariable("AIRFLOW_DAG_RUN_STATE"));
    server.verify(2, getRequestedFor(urlEqualTo(RUNS + "/r1")));
  }

  @Test
  void failsWhenTooManyStatusChecksInARowFail() {
    server.stubFor(
        get(urlEqualTo(RUNS + "/r1")).willReturn(aResponse().withStatus(502).withBody("down")));
    ActionAirflowTriggerDagRun action = action();

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    server.verify(
        ActionAirflowTriggerDagRun.MAX_FAILED_POLLS, getRequestedFor(urlEqualTo(RUNS + "/r1")));
  }

  @Test
  void failsWithoutAConnection() {
    ActionAirflowTriggerDagRun action = action();
    action.setConnectionName("missing");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void checkReportsMissingFields() {
    ActionAirflowTriggerDagRun action = new ActionAirflowTriggerDagRun("trigger");
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, action, metadataProvider);
    assertEquals(2, remarks.size());

    remarks.clear();
    action.setConnectionName("${CONNECTION}");
    action.setDagId("${DAG}");
    action.check(remarks, null, action, metadataProvider);
    assertTrue(remarks.isEmpty());
  }

  @Test
  void cloneCopiesTheConfEntries() {
    ActionAirflowTriggerDagRun action = action();
    action.getConfEntries().add(new AirflowConfEntry("k", "v"));

    ActionAirflowTriggerDagRun copy = action.clone();

    assertEquals("my_dag", copy.getDagId());
    assertEquals(1, copy.getConfEntries().size());
    assertNotSame(action.getConfEntries().get(0), copy.getConfEntries().get(0));
  }

  @Test
  void serializesToXmlAndBack() throws Exception {
    ActionAirflowTriggerDagRun action =
        ActionSerializationTestUtil.testSerialization(
            "/airflow-trigger-dag-run-action.xml", ActionAirflowTriggerDagRun.class);

    assertEquals("airflow", action.getConnectionName());
    assertEquals("daily_load", action.getDagId());
    assertEquals("hop-${RUN_ID}", action.getDagRunId());
    assertEquals("2026-11-21T10:00:00+01:00", action.getLogicalDate());
    assertEquals("{\"count\": 3}", action.getConfJson());
    assertEquals(2, action.getConfEntries().size());
    assertEquals("source", action.getConfEntries().get(1).getName());
    assertEquals("${SOURCE}", action.getConfEntries().get(1).getValue());
    assertFalse(action.isWaitingForCompletion());
    assertEquals("5", action.getPollIntervalSeconds());
    assertEquals("600", action.getTimeoutSeconds());
    assertEquals("RUN", action.getDagRunIdVariable());
    assertEquals("STATE", action.getStateVariable());
  }
}
