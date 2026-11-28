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
package org.apache.hop.airflow.engine;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hop.airflow.bundle.AirflowBundleWriter;
import org.apache.hop.airflow.launcher.BundleManifest;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.airflow.metadata.AirflowAuthenticationType;
import org.apache.hop.airflow.metadata.AirflowConnection;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class AirflowWorkflowEngineTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String DAG = "/api/v2/dags/demo__flows__hello";
  private static final String RUN = DAG + "/dagRuns/r1";
  private static final String TASK = RUN + "/taskInstances/run_workflow";

  /** The documentation of the DAG of the published flows/hello.hwf, which holds "<workflow/>". */
  private static final String DOC =
      "Hop workflow `flows/hello.hwf`\\n\\nWorkflow version: "
          + sha("<workflow/>")
          + "\\n\\nTasks: PER_ACTION";

  private static String sha(String content) {
    try {
      return java.util.HexFormat.of()
          .formatHex(
              java.security.MessageDigest.getInstance("SHA-256")
                  .digest(content.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
          .substring(0, 12);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @TempDir Path project;
  @TempDir Path bundle;

  private WireMockServer server;
  private MemoryMetadataProvider metadataProvider;
  private AirflowWorkflowRunConfiguration config;

  @BeforeEach
  void setUp() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    metadataProvider = new MemoryMetadataProvider();
    AirflowConnection connection = new AirflowConnection();
    connection.setName("airflow");
    connection.setUrl("http://localhost:" + server.port());
    connection.setAuthenticationType(AirflowAuthenticationType.NONE);
    metadataProvider.getSerializer(AirflowConnection.class).save(connection);

    Files.createDirectories(project.resolve("flows"));
    Files.writeString(project.resolve("flows/hello.hwf"), "<workflow/>");

    config = new AirflowWorkflowRunConfiguration();
    config.setConnectionName("airflow");
    config.setBundleFolder(bundle.toString());
    config.setPollSeconds("1");
    config.setDagWaitSeconds("10");
  }

  @AfterEach
  void tearDown() {
    server.stop();
  }

  /** Runs while the engine publishes the bundle, for example a Stop from Hop GUI. */
  private Runnable whilePublishing = () -> {};

  private AirflowWorkflowEngine engine() throws Exception {
    AirflowWorkflowEngine engine =
        new AirflowWorkflowEngine() {
          @Override
          protected AirflowBundleWriter newBundleWriter() {
            whilePublishing.run();
            AirflowBundleWriter writer = new AirflowBundleWriter();
            writer.setSchemaVersion("2026-10-30");
            return writer;
          }
        };
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("hello");
    meta.setFilename(project.resolve("flows/hello.hwf").toString());
    meta.addAction(new ActionMeta(new ActionStart("Start")));
    meta.addAction(new ActionMeta(new ActionStart("Greet")));
    meta.setChanged(false);
    engine.setWorkflowMeta(meta);
    engine.setMetadataProvider(metadataProvider);
    engine.setWorkflowRunConfiguration(
        new WorkflowRunConfiguration("airflow", "", null, config, false));
    engine.setVariable("HOP_PROJECT_NAME", "demo");
    engine.setVariable("PROJECT_HOME", project.toString());
    engine.addParameterDefinition("PRM_DAY", "", "");
    engine.setVariable("PRM_DAY", "2026-11-22");
    return engine;
  }

  /** Airflow shows the DAG only after the first check, and paused. */
  private void stubNewPausedDag() {
    stubNewPausedDag(DOC);
  }

  private void stubNewPausedDag(String doc) {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("dag")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(404).withBody("{\"detail\":\"not found\"}"))
            .willSetStateTo("parsed"));
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("dag")
            .whenScenarioStateIs("parsed")
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":true,\"doc_md\":\""
                            + doc
                            + "\"}")));
    server.stubFor(
        patch(urlEqualTo(DAG + "?update_mask=is_paused"))
            .willReturn(aResponse().withBody("{\"is_paused\":false}")));
  }

  private void stubTrigger() {
    server.stubFor(
        post(urlEqualTo(DAG + "/dagRuns"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\"queued\"}")));
  }

  private void stubRunStates(String... states) {
    String state = Scenario.STARTED;
    for (int i = 0; i < states.length; i++) {
      String next = i == states.length - 1 ? state : "poll-" + (i + 1);
      server.stubFor(
          get(urlEqualTo(RUN))
              .inScenario("run")
              .whenScenarioStateIs(state)
              .willReturn(
                  aResponse()
                      .withBody(
                          "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\""
                              + states[i]
                              + "\"}"))
              .willSetStateTo(next));
      state = next;
    }
  }

  private void stubTask(String state, String... lines) {
    server.stubFor(
        get(urlPathEqualTo(RUN + "/taskInstances"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"task_instances\":[{\"task_id\":\"run_workflow\",\"state\":\""
                            + state
                            + "\",\"try_number\":1}]}")));
    String content =
        List.of(lines).stream()
            .map(
                line ->
                    "{\"event\":\"2026/11/22 13:56:57 - hello - "
                        + line
                        + "\",\"logger\":\"task.stdout\"}")
            .collect(Collectors.joining(","));
    server.stubFor(
        get(urlEqualTo(TASK + "/logs/1"))
            .willReturn(
                aResponse()
                    .withBody("{\"content\":[" + content + "],\"continuation_token\":null}")));
  }

  private void stubOutcome(String json) {
    server.stubFor(
        get(urlEqualTo(TASK + "/xcomEntries/return_value"))
            .willReturn(aResponse().withBody("{\"key\":\"return_value\",\"value\":" + json + "}")));
  }

  @Test
  void publishesWaitsForTheNewDagUnpausesItAndFollowsTheRun() throws Exception {
    stubNewPausedDag();
    stubTrigger();
    stubRunStates("queued", "running", "success");
    stubTask(
        "success",
        "Starting action [Start]",
        "Finished action [Start] (result=[true])",
        "Starting action [Greet]",
        "Finished action [Greet] (result=[true])");
    stubOutcome("{\"success\":true,\"counters\":{\"errors\":0,\"read\":3,\"written\":2}}");

    AirflowWorkflowEngine engine = engine();
    Result result = engine.startExecution();

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(3, result.getNrLinesRead());
    assertEquals(2, result.getNrLinesWritten());
    assertTrue(Files.isRegularFile(bundle.resolve("hop-airflow.jar")));
    assertTrue(Files.isRegularFile(bundle.resolve("flows/hello.hwf")));
    server.verify(1, patchRequestedFor(urlEqualTo(DAG + "?update_mask=is_paused")));
    server.verify(
        1,
        postRequestedFor(urlEqualTo(DAG + "/dagRuns"))
            .withRequestBody(
                equalToJson(
                    "{\"logical_date\":null,\"conf\":{\"hop_workflow_version\":\""
                        + sha("<workflow/>")
                        + "\",\"PRM_DAY\":\"2026-11-22\"}}")));
    Map<String, Boolean> actions =
        engine.getActionResults().stream()
            .collect(Collectors.toMap(ActionResult::getActionName, r -> r.getResult().isResult()));
    assertEquals(Map.of("Start", true, "Greet", true), actions);
    assertTrue(engine.isFinished());
    assertFalse(engine.isActive());
    assertEquals(
        HopAirflowBundle.TASKS_PER_ACTION,
        BundleManifest.read(bundle.resolve("hop-airflow.jar")).tasks());
  }

  /** The stub asks for one task per workflow; the canvas still shows the actions. */
  @Test
  void publishesTheTasksOfTheRunConfiguration() throws Exception {
    config.setTasks(AirflowDagTasks.WHOLE_WORKFLOW);
    stubNewPausedDag(DOC.replace("PER_ACTION", "WHOLE_WORKFLOW"));
    stubTrigger();
    stubRunStates("running", "success");
    stubTask(
        "success",
        "Starting action [Start]",
        "Finished action [Start] (result=[true])",
        "Starting action [Greet]",
        "Finished action [Greet] (result=[false])");
    stubOutcome("{\"success\":true,\"counters\":{\"errors\":0}}");

    AirflowWorkflowEngine engine = engine();
    Result result = engine.startExecution();

    assertTrue(result.isResult());
    assertTrue(BundleManifest.read(bundle.resolve("hop-airflow.jar")).wholeWorkflowTasks());
    Map<String, Boolean> actions =
        engine.getActionResults().stream()
            .collect(Collectors.toMap(ActionResult::getActionName, r -> r.getResult().isResult()));
    assertEquals(Map.of("Start", true, "Greet", false), actions);
  }

  private void stubTaskLog(String taskId, String... lines) {
    String content =
        List.of(lines).stream()
            .map(
                line ->
                    "{\"event\":\"2026/11/23 13:56:57 - hello - "
                        + line
                        + "\",\"logger\":\"task.stdout\"}")
            .collect(Collectors.joining(","));
    server.stubFor(
        get(urlEqualTo(RUN + "/taskInstances/" + taskId + "/logs/1"))
            .willReturn(
                aResponse()
                    .withBody("{\"content\":[" + content + "],\"continuation_token\":null}")));
  }

  @Test
  void followsEveryTaskOfTheRunAndTakesTheCountersOfTheLastOne() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    stubRunStates("running", "success");
    // Listed out of order: the engine follows them in the order they started
    server.stubFor(
        get(urlPathEqualTo(RUN + "/taskInstances"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"task_instances\":["
                            + "{\"task_id\":\"Check\",\"state\":\"success\",\"try_number\":1,"
                            + "\"start_date\":\"2026-11-23T10:00:09Z\","
                            + "\"end_date\":\"2026-11-23T10:00:12Z\"},"
                            + "{\"task_id\":\"Not_expected\",\"state\":\"skipped\",\"try_number\":0,"
                            + "\"start_date\":null},"
                            + "{\"task_id\":\"Greet\",\"state\":\"success\",\"try_number\":1,"
                            + "\"start_date\":\"2026-11-23T10:00:02Z\","
                            + "\"end_date\":\"2026-11-23T10:00:05Z\"}]}")));
    stubTaskLog("Greet", "Starting action [Greet]", "Finished action [Greet] (result=[true])");
    stubTaskLog("Check", "Starting action [Start]", "Finished action [Start] (result=[true])");
    server.stubFor(
        get(urlEqualTo(RUN + "/taskInstances/Greet/xcomEntries/hop_outcome"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"key\":\"hop_outcome\",\"value\":{\"success\":true,"
                            + "\"counters\":{\"read\":3,\"written\":1}}}")));
    server.stubFor(
        get(urlEqualTo(RUN + "/taskInstances/Check/xcomEntries/hop_outcome"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"key\":\"hop_outcome\",\"value\":{\"success\":true,"
                            + "\"counters\":{\"read\":2,\"written\":4}}}")));

    AirflowWorkflowEngine engine = engine();
    Result result = engine.startExecution();

    assertTrue(result.isResult());
    // Each action passes the counters on, as in Hop: the last task has them all
    assertEquals(2, result.getNrLinesRead());
    assertEquals(4, result.getNrLinesWritten());
    assertEquals(
        List.of("Greet", "Start"),
        engine.getActionResults().stream().map(ActionResult::getActionName).toList());
    server.verify(0, getRequestedFor(urlPathMatching(RUN + "/taskInstances/Not_expected/.*")));
  }

  /** After a change, Airflow runs the DAG of the earlier version until it parses the file again. */
  @Test
  void waitsForTheDagOfThePublishedVersion() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("version")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,"
                            + "\"doc_md\":\"Workflow version: 0123456789ab\"}"))
            .willSetStateTo("parsed"));
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("version")
            .whenScenarioStateIs("parsed")
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    stubRunStates("success");
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");

    engine().startExecution();

    server.verify(2, getRequestedFor(urlEqualTo(DAG + "/details")));
    server.verify(1, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  /**
   * Changing only the Tasks of the run configuration leaves the workflow as it was: Airflow still
   * shows the DAG with the earlier tasks until it parses the new stub. A run of that DAG would fail
   * on the workers, which already read the new stub.
   */
  @Test
  void waitsForTheDagWithTheTasksOfTheRunConfiguration() throws Exception {
    config.setTasks(AirflowDagTasks.WHOLE_WORKFLOW);
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("tasks")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}"))
            .willSetStateTo("parsed"));
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("tasks")
            .whenScenarioStateIs("parsed")
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC.replace("PER_ACTION", "WHOLE_WORKFLOW")
                            + "\"}")));
    stubTrigger();
    stubRunStates("success");
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");

    engine().startExecution();

    server.verify(2, getRequestedFor(urlEqualTo(DAG + "/details")));
    server.verify(1, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  /** Writes an Airflow DAG object of flows/hello.hwf, and returns its version. */
  private String airflowDag(String json) throws Exception {
    Path folder = Files.createDirectories(project.resolve("metadata/airflow-dag"));
    Files.writeString(folder.resolve("daily.json"), json);
    return sha(json);
  }

  private static String withSettings(String doc, String version) {
    return doc + "\\n\\nSettings: " + version + " (`metadata/airflow-dag/daily.json`)";
  }

  /**
   * The object's parameter value wins over the workflow default: the run conf only has the version,
   * and the launcher adds the value of the object.
   */
  @Test
  void runsTheDagOfItsAirflowDagObject() throws Exception {
    String version =
        airflowDag(
            "{\"name\":\"daily\",\"workflow\":\"${PROJECT_HOME}/flows/hello.hwf\","
                + "\"tasks\":\"PER_ACTION\","
                + "\"parameters\":[{\"name\":\"PRM_DAY\",\"value\":\"today\"}]}");
    stubNewPausedDag(withSettings(DOC, version));
    stubTrigger();
    stubRunStates("success");
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");

    Result result = engine().startExecution();

    assertTrue(result.isResult());
    assertTrue(Files.isRegularFile(bundle.resolve("metadata/airflow-dag/daily.json")));
    server.verify(
        1,
        postRequestedFor(urlEqualTo(DAG + "/dagRuns"))
            .withRequestBody(
                equalToJson(
                    "{\"logical_date\":null,\"conf\":{\"hop_workflow_version\":\""
                        + sha("<workflow/>")
                        + "\"}}")));
  }

  /** A value set for the run wins over the value of the object. */
  @Test
  void passesOnTheParameterValueOfTheRun() throws Exception {
    String version =
        airflowDag(
            "{\"name\":\"daily\",\"workflow\":\"flows/hello.hwf\",\"tasks\":\"PER_ACTION\","
                + "\"parameters\":[{\"name\":\"PRM_DAY\",\"value\":\"today\"}]}");
    stubNewPausedDag(withSettings(DOC, version));
    stubTrigger();
    stubRunStates("success");
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");
    AirflowWorkflowEngine engine = engine();
    engine.setParameterValue("PRM_DAY", "2026-11-22");

    engine.startExecution();

    server.verify(
        1,
        postRequestedFor(urlEqualTo(DAG + "/dagRuns"))
            .withRequestBody(
                equalToJson(
                    "{\"logical_date\":null,\"conf\":{\"hop_workflow_version\":\""
                        + sha("<workflow/>")
                        + "\",\"PRM_DAY\":\"2026-11-22\"}}")));
  }

  /**
   * A change of the object alone leaves the workflow version as it was: Airflow still shows the DAG
   * with the earlier settings until it parses the bundle again.
   */
  @Test
  void waitsForTheDagWithTheSettingsOfItsObject() throws Exception {
    String version =
        airflowDag(
            "{\"name\":\"daily\",\"workflow\":\"flows/hello.hwf\",\"tasks\":\"PER_ACTION\","
                + "\"schedule\":\"@daily\"}");
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("settings")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + withSettings(DOC, "0123456789ab")
                            + "\"}"))
            .willSetStateTo("parsed"));
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .inScenario("settings")
            .whenScenarioStateIs("parsed")
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + withSettings(DOC, version)
                            + "\"}")));
    stubTrigger();
    stubRunStates("success");
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");

    engine().startExecution();

    server.verify(2, getRequestedFor(urlEqualTo(DAG + "/details")));
    server.verify(1, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  /** A DAG listed before the object was removed still has its settings. */
  @Test
  void waitsForTheDagWithoutSettingsOnceTheObjectIsGone() throws Exception {
    config.setDagWaitSeconds("1");
    stubNewPausedDag(withSettings(DOC, "0123456789ab"));

    HopException e = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(e.getMessage().contains("earlier version"), e.getMessage());
    server.verify(0, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  @Test
  void namesTheOlderHopOnTheWorkersWhenItIgnoresTheObject() throws Exception {
    airflowDag("{\"name\":\"daily\",\"workflow\":\"flows/hello.hwf\"}");
    config.setDagWaitSeconds("1");
    stubNewPausedDag(DOC.replace("\\n\\nTasks: PER_ACTION", ""));

    HopException e = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(e.getMessage().contains("ignores Airflow DAG objects"), e.getMessage());
  }

  @Test
  void waitsForTheDagIdOfItsObject() throws Exception {
    airflowDag("{\"name\":\"daily\",\"workflow\":\"flows/hello.hwf\",\"dag_id\":\"hello daily\"}");
    config.setDagWaitSeconds("0");
    server.stubFor(
        get(urlEqualTo("/api/v2/dags/demo__hello_daily/details"))
            .willReturn(aResponse().withStatus(404).withBody("{\"detail\":\"not found\"}")));

    HopException e = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(e.getMessage().contains("demo__hello_daily"), e.getMessage());
    server.verify(1, getRequestedFor(urlEqualTo("/api/v2/dags/demo__hello_daily/details")));
  }

  @Test
  void failsWhenAirflowKeepsTheEarlierVersion() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,"
                            + "\"doc_md\":\"Workflow version: 0123456789ab\"}")));
    config.setDagWaitSeconds("2");

    HopException e = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(e.getMessage().contains("earlier version"), e.getMessage());
    server.verify(0, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  /**
   * Unpausing a DAG with a schedule would also start the schedule, and catch up on the runs it
   * missed: Hop leaves such a DAG paused and starts no run.
   */
  @Test
  void neverUnpausesADagWithASchedule() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":true,\"doc_md\":\""
                            + DOC
                            + "\",\"timetable_summary\":\"0 6 * * *\"}")));
    assertTrue(config.isUnpausingDag());

    HopException e = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(e.getMessage().contains("has a schedule"), e.getMessage());
    server.verify(0, patchRequestedFor(urlEqualTo(DAG + "?update_mask=is_paused")));
    server.verify(0, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  /** The launcher of an older Hop on the workers writes no Tasks line, and one task per action. */
  @Test
  void runsTheDagOfAnOlderHopOnTheWorkers() throws Exception {
    stubNewPausedDag(DOC.replace("\\n\\nTasks: PER_ACTION", ""));
    stubTrigger();
    stubRunStates("success");
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");

    Result result = engine().startExecution();

    assertTrue(result.isResult());
    server.verify(1, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  @Test
  void namesTheOlderHopOnTheWorkersWhenItIgnoresTheTasks() throws Exception {
    config.setTasks(AirflowDagTasks.WHOLE_WORKFLOW);
    config.setDagWaitSeconds("1");
    stubNewPausedDag(DOC.replace("\\n\\nTasks: PER_ACTION", ""));

    HopException e = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(e.getMessage().contains("older than 2.21"), e.getMessage());
    server.verify(0, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  @Test
  void readsTheTasksOfTheDagDocumentation() {
    assertEquals(
        "WHOLE_WORKFLOW",
        AirflowWorkflowEngine.docTasks("Workflow version: abc\n\nTasks: WHOLE_WORKFLOW\n"));
    assertEquals("PER_ACTION", AirflowWorkflowEngine.docTasks("Tasks: PER_ACTION"));
    assertEquals("PER_ACTION", AirflowWorkflowEngine.docTasks("Workflow version: abc"));
    // The documentation of an Airflow DAG object comes after the lines Hop reads
    assertEquals(
        "PER_ACTION",
        AirflowWorkflowEngine.docTasks(
            "Hop workflow `flows/load.hwf`\n\nWorkflow version: abc\n\nTasks: PER_ACTION\n\n"
                + "Settings: def (`metadata/airflow-dag/daily.json`)\n\n"
                + "Daily load. Tasks: extract, transform"));
  }

  @Test
  void startsNoRunWhenStoppedWhilePublishing() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    AirflowWorkflowEngine engine = engine();
    whilePublishing = engine::stopExecution;

    HopException e = assertThrows(HopException.class, engine::submitToRemoteServer);

    assertTrue(e.getMessage().contains("Stopped"), e.getMessage());
    server.verify(0, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  /**
   * A Stop while Airflow starts the run found no run to fail: the run is failed once it is known,
   * instead of going on in Airflow while Hop GUI shows the workflow as stopped.
   */
  @Test
  void failsTheRunWhenStoppedWhileItStarts() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    server.stubFor(
        post(urlEqualTo(DAG + "/dagRuns"))
            .willReturn(
                aResponse()
                    .withFixedDelay(1500)
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\"queued\"}")));
    server.stubFor(
        patch(urlEqualTo(RUN))
            .withRequestBody(equalToJson("{\"state\":\"failed\"}"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\"failed\"}")));
    AirflowWorkflowEngine engine = engine();
    CountDownLatch triggering = new CountDownLatch(1);
    server.addMockServiceRequestListener(
        (request, response) -> {
          if ("POST".equals(request.getMethod().getName())) {
            triggering.countDown();
          }
        });
    CompletableFuture<Void> submit =
        CompletableFuture.runAsync(
            () -> {
              try {
                engine.submitToRemoteServer();
              } catch (HopException e) {
                throw new IllegalStateException(e);
              }
            });
    // The listener runs once the response is ready, before its delay: the trigger is in flight
    assertTrue(triggering.await(10, TimeUnit.SECONDS));
    engine.stopExecution();
    submit.get(10, TimeUnit.SECONDS);

    assertEquals("r1", engine.getContainerId());
    server.verify(1, patchRequestedFor(urlEqualTo(RUN)));
  }

  /** The launcher reads the plain file name: the DAG id must not hold VFS's '%25'. */
  @Test
  void namesTheDagAfterTheDecodedFileName() throws Exception {
    Files.writeString(project.resolve("flows/100%.hwf"), "<workflow/>");
    String dag = "/api/v2/dags/demo__flows__100_";
    server.stubFor(
        get(urlEqualTo(dag + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__100_\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    server.stubFor(
        post(urlEqualTo(dag + "/dagRuns"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__100_\",\"dag_run_id\":\"r1\",\"state\":\"queued\"}")));
    AirflowWorkflowEngine engine = engine();
    // As Hop GUI keeps a file it opened: a URL, with the '%' encoded
    engine.getWorkflowMeta().setFilename(project.resolve("flows").toUri() + "100%25.hwf");

    engine.submitToRemoteServer();

    assertEquals("r1", engine.getContainerId());
    server.verify(1, postRequestedFor(urlEqualTo(dag + "/dagRuns")));
  }

  @Test
  void usesTheFileNameOfTheWorkflowInTheDagId() throws Exception {
    assertEquals(
        "flows/load #1.hwf",
        AirflowWorkflowEngine.relativeWorkflowPath(
            project.toString(), project.resolve("flows/load #1.hwf").toString()));
    assertEquals(
        "flows/load #1.hwf",
        AirflowWorkflowEngine.relativeWorkflowPath(
            project.toString(), project.resolve("flows").toUri() + "load%20%231.hwf"));
    assertEquals(
        "flows/100%.hwf",
        AirflowWorkflowEngine.relativeWorkflowPath(
            project.toString(), project.resolve("flows").toUri() + "100%25.hwf"));
  }

  /** A proxy that answers 503 once must not end the follow-up of a run that goes on. */
  @Test
  void keepsFollowingTheRunAfterAFailedPoll() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    server.stubFor(
        get(urlEqualTo(RUN))
            .inScenario("poll")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(503).withBody("Service Unavailable"))
            .willSetStateTo("up"));
    server.stubFor(
        get(urlEqualTo(RUN))
            .inScenario("poll")
            .whenScenarioStateIs("up")
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\"success\"}")));
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");

    Result result = engine().startExecution();

    assertTrue(result.isResult());
    server.verify(2, getRequestedFor(urlEqualTo(RUN)));
  }

  @Test
  void readsAnOutcomeThatArrivesAsJsonText() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    stubRunStates("success");
    stubTask("success", "Starting action [Start]", "Finished action [Start] (result=[true])");
    server.stubFor(
        get(urlEqualTo(TASK + "/xcomEntries/return_value"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"key\":\"return_value\",\"value\":"
                            + "\"{\\\"success\\\":true,\\\"counters\\\":{\\\"read\\\":7}}\"}")));

    Result result = engine().startExecution();

    assertEquals(7, result.getNrLinesRead());
  }

  @Test
  void showsEachLogLineOnceWhenAirflowGivesNoToken() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    stubRunStates("running", "running", "running", "success");
    stubTask("running", "Starting action [Start]", "Finished action [Start] (result=[true])");
    stubOutcome("{\"success\":true}");

    AirflowWorkflowEngine engine = engine();
    engine.startExecution();

    assertEquals(1, engine.getActionResults().size());
    assertTrue(engine.getActiveActions().isEmpty());
  }

  @Test
  void failsWhenTheRunFails() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    stubRunStates("running", "failed");
    stubTask("failed", "Starting action [Greet]", "Finished action [Greet] (result=[false])");
    stubOutcome("{\"success\":false,\"error\":\"Greet failed\",\"counters\":{\"errors\":1}}");

    AirflowWorkflowEngine engine = engine();
    Result result = engine.startExecution();

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    assertFalse(engine.getActionResults().get(0).getResult().isResult());
    server.verify(0, patchRequestedFor(urlEqualTo(DAG + "?update_mask=is_paused")));
  }

  @Test
  void failsWhenTheRunFailsWithoutAnOutcome() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    stubRunStates("failed");
    stubTask("failed");
    server.stubFor(
        get(urlEqualTo(TASK + "/xcomEntries/return_value"))
            .willReturn(aResponse().withStatus(404).withBody("{\"detail\":\"not found\"}")));

    Result result = engine().startExecution();

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void doesNotStartAPausedDagItMayNotUnpause() throws Exception {
    config.setUnpausingDag(false);
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":true,\"doc_md\":\""
                            + DOC
                            + "\"}")));

    HopException error = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(error.getMessage().contains("paused"), error.getMessage());
    server.verify(0, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  @Test
  void failsWhenAirflowDoesNotShowTheDagInTime() throws Exception {
    config.setDagWaitSeconds("0");
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(aResponse().withStatus(404).withBody("{\"detail\":\"x\"}")));

    HopException error = assertThrows(HopException.class, () -> engine().submitToRemoteServer());

    assertTrue(error.getMessage().contains("demo__flows__hello"), error.getMessage());
    server.verify(0, postRequestedFor(urlEqualTo(DAG + "/dagRuns")));
  }

  @Test
  void refusesAWorkflowWithUnsavedChanges() throws Exception {
    AirflowWorkflowEngine engine = engine();
    engine.getWorkflowMeta().setChanged(true);

    HopException error = assertThrows(HopException.class, engine::submitToRemoteServer);

    assertTrue(error.getMessage().contains("Save the workflow"), error.getMessage());
    assertFalse(Files.exists(bundle.resolve("hop-airflow.jar")));
  }

  @Test
  void refusesAWorkflowOutsideTheProject(@TempDir Path elsewhere) {
    assertThrows(
        HopException.class,
        () ->
            AirflowWorkflowEngine.relativeWorkflowPath(
                project.toString(), elsewhere.resolve("other.hwf").toString()));
  }

  @Test
  void stopsTheRunByFailingIt() throws Exception {
    server.stubFor(
        get(urlEqualTo(DAG + "/details"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"is_paused\":false,\"doc_md\":\""
                            + DOC
                            + "\"}")));
    stubTrigger();
    stubTask("running", "Starting action [Greet]");
    server.stubFor(
        get(urlEqualTo(RUN))
            .inScenario("stop")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\"running\"}")));
    server.stubFor(
        patch(urlEqualTo(RUN))
            .inScenario("stop")
            .withRequestBody(equalToJson("{\"state\":\"failed\"}"))
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\"failed\"}"))
            .willSetStateTo("stopped"));
    server.stubFor(
        get(urlEqualTo(RUN))
            .inScenario("stop")
            .whenScenarioStateIs("stopped")
            .willReturn(
                aResponse()
                    .withBody(
                        "{\"dag_id\":\"demo__flows__hello\",\"dag_run_id\":\"r1\",\"state\":\"failed\"}")));
    stubOutcome("{\"success\":false,\"stopped\":true}");

    AirflowWorkflowEngine engine = engine();
    CompletableFuture<Result> run = CompletableFuture.supplyAsync(engine::startExecution);
    long deadline = System.currentTimeMillis() + 10_000;
    while (engine.getActiveActions().isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    assertFalse(engine.getActiveActions().isEmpty(), "Greet runs");
    engine.stopExecution();
    Result result = run.get(20, TimeUnit.SECONDS);

    assertNotNull(result);
    assertFalse(result.isResult());
    assertTrue(result.isStopped());
    assertTrue(engine.getActiveActions().isEmpty());
    server.verify(1, patchRequestedFor(urlEqualTo(RUN)));
  }
}
