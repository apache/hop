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

package org.apache.hop.airflow.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopDagSettingsTest {

  @TempDir Path project;

  private static final String TWO_ACTIONS =
      "<workflow><name>w</name><description>Load</description><actions>"
          + "<action><name>Start</name><type>SPECIAL</type></action>"
          + "<action><name>Log</name><type>WRITE_TO_LOG</type></action>"
          + "</actions><hops>"
          + "<hop><from>Start</from><to>Log</to><enabled>Y</enabled><evaluation>Y</evaluation>"
          + "<unconditional>Y</unconditional></hop>"
          + "</hops></workflow>";

  /** Log, and Mail when Log fails: Mail waits with trigger rule one_failed. */
  private static final String FAILURE_HOP =
      "<workflow><name>w</name><actions>"
          + "<action><name>Start</name><type>SPECIAL</type></action>"
          + "<action><name>Log</name><type>WRITE_TO_LOG</type></action>"
          + "<action><name>Mail</name><type>WRITE_TO_LOG</type></action>"
          + "</actions><hops>"
          + "<hop><from>Start</from><to>Log</to><enabled>Y</enabled><evaluation>Y</evaluation>"
          + "<unconditional>Y</unconditional></hop>"
          + "<hop><from>Log</from><to>Mail</to><enabled>Y</enabled><evaluation>N</evaluation>"
          + "<unconditional>N</unconditional></hop>"
          + "</hops></workflow>";

  private Path dagFile(String name, String json) throws Exception {
    Path folder = project.resolve("metadata/airflow-dag");
    Files.createDirectories(folder);
    Path file = folder.resolve(name + ".json");
    Files.writeString(file, json);
    return file;
  }

  private HopWorkflowFile workflow(String relative, String xml) throws Exception {
    Path file = project.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, xml);
    return HopWorkflowFile.read(file, relative, "p__");
  }

  private BundleManifest manifest(String tasks) {
    return new BundleManifest(
        project,
        "2026-10-30",
        "",
        "p",
        "local",
        "Basic",
        List.of(),
        List.of(),
        "p__",
        ActionTasks.NOT_EVALUATING,
        tasks);
  }

  @Test
  void readsEverySettingInTheTypeTheSdkTakes() throws Exception {
    Path file =
        dagFile(
            "daily",
            """
            {"name":"daily","workflow":"${PROJECT_HOME}/flows/load.hwf","dag_id":"load_daily",
             "schedule":"0 6 * * *","start_date":"2026-01-31","end_date":"2026-12-31 18:30",
             "catchup":"NO","paused_on_creation":"YES","max_active_runs":"1",
             "max_active_tasks":"4","max_consecutive_failed_runs":"3",
             "run_timeout_seconds":"3600","fail_fast":true,"display_name":"Daily load",
             "tags":"sales, daily,sales","documentation":"Loads the *orders*.",
             "tasks":"WHOLE_WORKFLOW",
             "parameters":[{"name":"PRM_DAY","value":"today"},{"name":"PRM_EMPTY"}]}
            """);

    List<HopDagSettings> all = HopDagSettings.read(project);

    assertEquals(1, all.size());
    HopDagSettings settings = all.get(0);
    assertNull(settings.error());
    assertEquals("daily", settings.name());
    assertEquals("metadata/airflow-dag/daily.json", settings.file());
    assertEquals("flows/load.hwf", settings.workflow());
    assertEquals("load_daily", settings.dagId());
    assertEquals(HopWorkflowFile.version(Files.readAllBytes(file)), settings.version());
    Map<String, Object> config = settings.config();
    assertEquals("0 6 * * *", config.get("schedule"));
    assertEquals(
        OffsetDateTime.of(2026, 1, 31, 0, 0, 0, 0, ZoneOffset.UTC), config.get("start_date"));
    assertEquals(
        OffsetDateTime.of(2026, 12, 31, 18, 30, 0, 0, ZoneOffset.UTC), config.get("end_date"));
    assertEquals(false, config.get("catchup"));
    assertEquals(true, config.get("is_paused_upon_creation"));
    assertEquals(1, config.get("max_active_runs"));
    assertEquals(4, config.get("max_active_tasks"));
    assertEquals(3, config.get("max_consecutive_failed_dag_runs"));
    assertEquals(Duration.ofHours(1), config.get("dagrun_timeout"));
    assertEquals(true, config.get("fail_fast"));
    assertEquals("Daily load", config.get("dag_display_name"));
    assertEquals(List.of("sales", "daily"), settings.tags());
    assertEquals("Loads the *orders*.", settings.documentation());
    assertEquals(HopAirflowBundle.TASKS_WHOLE_WORKFLOW, settings.tasks());
    assertEquals(Map.of("PRM_DAY", "today", "PRM_EMPTY", ""), settings.parameters());
  }

  @Test
  void leavesWhatIsNotSetToAirflow() throws Exception {
    dagFile(
        "plain",
        """
        {"name":"plain","workflow":"flows/load.hwf","catchup":"AIRFLOW_DEFAULT",
         "paused_on_creation":"AIRFLOW_DEFAULT","fail_fast":false,"schedule":"",
         "parameters":[]}
        """);

    HopDagSettings settings = HopDagSettings.read(project).get(0);

    assertNull(settings.error());
    assertEquals(Map.of(), settings.config());
    assertNull(settings.dagId());
    assertNull(settings.tasks());
  }

  @Test
  void namesEverySettingItCannotUse() throws Exception {
    dagFile(
        "bad",
        """
        {"name":"bad","workflow":"/opt/flows/load.hwf","start_date":"31/01/2026",
         "max_active_runs":"0","run_timeout_seconds":"1h","catchup":"MAYBE",
         "tasks":"PER_TASK_GROUP"}
        """);

    HopDagSettings settings = HopDagSettings.read(project).get(0);

    String error = settings.error();
    assertTrue(error.startsWith("Airflow DAG bad (metadata/airflow-dag/bad.json): "), error);
    for (String part :
        List.of(
            "workflow /opt/flows/load.hwf",
            "start date 31/01/2026",
            "max active runs 0",
            "run timeout 1h",
            "catchup MAYBE",
            "tasks PER_TASK_GROUP")) {
      assertTrue(error.contains(part), part + " in " + error);
    }
    assertEquals(Map.of(), settings.config());
    assertEquals(Map.of(), settings.parameters());
  }

  @Test
  void readsAFileThatIsNoJsonAsAnError() throws Exception {
    dagFile("broken", "{\"name\":");

    HopDagSettings settings = HopDagSettings.read(project).get(0);

    assertEquals("broken", settings.name());
    assertTrue(settings.error().startsWith("Hop could not read Airflow DAG"), settings.error());
  }

  @Test
  void findsTheMetadataFolderOfTheProject() throws Exception {
    Files.writeString(
        project.resolve("project-config.json"),
        "{\"metadataBaseFolder\":\"${PROJECT_HOME}/meta\"}");
    assertEquals(project.resolve("meta"), HopDagSettings.metadataFolder(project));

    Files.writeString(
        project.resolve("project-config.json"), "{\"metadataBaseFolder\":\"${SHARED}/meta\"}");
    assertNull(HopDagSettings.metadataFolder(project));

    Files.delete(project.resolve("project-config.json"));
    assertEquals(project.resolve("metadata"), HopDagSettings.metadataFolder(project));
    assertEquals(List.of(), HopDagSettings.read(project));
  }

  @Test
  void keepsPathsInTheProject() {
    assertEquals("flows/a.hwf", HopDagSettings.projectRelative("${PROJECT_HOME}/flows/a.hwf"));
    assertEquals("flows/a.hwf", HopDagSettings.projectRelative("${PROJECT_HOME}\\flows\\a.hwf"));
    assertEquals("flows/a.hwf", HopDagSettings.projectRelative("./flows/x/../a.hwf"));
    assertEquals("", HopDagSettings.projectRelative("${PROJECT_HOME}"));
    assertNull(HopDagSettings.projectRelative("${PROJECT_HOME}flows/a.hwf"));
    assertNull(HopDagSettings.projectRelative("${PROJECT_HOME}/../other/a.hwf"));
    assertNull(HopDagSettings.projectRelative("${FLOWS}/a.hwf"));
    assertNull(HopDagSettings.projectRelative("/opt/flows/a.hwf"));
    assertNull(HopDagSettings.projectRelative("file:///opt/flows/a.hwf"));
  }

  @Test
  void readsDatesInUtc() {
    assertEquals(
        OffsetDateTime.of(2026, 3, 29, 1, 30, 0, 0, ZoneOffset.UTC),
        HopDagSettings.parseUtc("2026-03-29T01:30"));
    assertEquals(
        OffsetDateTime.of(2026, 3, 29, 1, 30, 15, 0, ZoneOffset.UTC),
        HopDagSettings.parseUtc(" 2026-03-29 01:30:15 "));
    // An offset would move the runs at each change to or from summer time
    assertNull(HopDagSettings.parseUtc("2026-03-29T01:30+01:00"));
    assertNull(HopDagSettings.parseUtc("2026-02-30"));
  }

  /** Each object is a DAG; a workflow without one keeps its DAG, as before. */
  @Test
  void makesADagOfEachObjectInsteadOfTheDagOfTheWorkflow() throws Exception {
    HopWorkflowFile load = workflow("flows/load.hwf", TWO_ACTIONS);
    HopWorkflowFile other = workflow("flows/other.hwf", TWO_ACTIONS);
    dagFile(
        "a-daily", "{\"name\":\"daily\",\"workflow\":\"flows/load.hwf\",\"schedule\":\"@daily\"}");
    dagFile(
        "b-hourly",
        "{\"name\":\"hourly\",\"workflow\":\"${PROJECT_HOME}/flows/load.hwf\","
            + "\"dag_id\":\"load hourly\",\"tasks\":\"WHOLE_WORKFLOW\","
            + "\"parameters\":[{\"name\":\"PRM\",\"value\":\"h\"}]}");

    List<HopWorkflowFile> dags =
        HopBundleMain.dags(
            List.of(load, other), HopDagSettings.read(project), manifest("PER_ACTION"));

    assertEquals(
        List.of("p__flows__load", "p__load_hourly", "p__flows__other"),
        dags.stream().map(HopWorkflowFile::dagId).toList());
    HopWorkflowFile daily = dags.get(0);
    assertEquals("daily", daily.settings().name());
    assertEquals(1, daily.tasks().size());
    HopWorkflowFile hourly = dags.get(1);
    assertTrue(hourly.tasks().isEmpty());
    assertEquals("Airflow DAG hourly runs the workflow as one task", hourly.singleTaskReason());
    assertEquals(load.version(), hourly.version());
    assertNull(dags.get(2).settings());

    String doc = HopBundleMain.dagDoc(hourly, HopAirflowBundle.TASKS_WHOLE_WORKFLOW);
    assertTrue(
        doc.endsWith(
            HopAirflowBundle.DAG_DOC_TASKS
                + "WHOLE_WORKFLOW\n\n"
                + HopAirflowBundle.DAG_DOC_SETTINGS
                + hourly.settings().version()
                + " (`metadata/airflow-dag/b-hourly.json`)"),
        doc);
    assertTrue(doc.contains(HopAirflowBundle.DAG_DOC_VERSION + load.version()), doc);
    // The SDK takes the settings
    HopBundleMain.dagDef(daily, manifest("PER_ACTION"));

    // The run conf wins over the parameter values of the object
    assertEquals(
        Map.of("PRM", "conf", "OTHER", 1),
        RunWorkflowTask.conf(hourly, Map.of("PRM", "conf", "OTHER", 1)));
    assertEquals(Map.of("PRM", "h"), RunWorkflowTask.conf(hourly, null));
    assertEquals(Map.of(), RunWorkflowTask.conf(dags.get(2), null));
  }

  @Test
  void runsTheTasksOfTheBundleWhenTheObjectHasNone() throws Exception {
    HopWorkflowFile load = workflow("flows/load.hwf", TWO_ACTIONS);
    dagFile("daily", "{\"name\":\"daily\",\"workflow\":\"flows/load.hwf\"}");

    List<HopWorkflowFile> dags =
        HopBundleMain.dags(List.of(load), HopDagSettings.read(project), manifest("WHOLE_WORKFLOW"));

    assertTrue(dags.get(0).tasks().isEmpty());
    assertTrue(
        dags.get(0).singleTaskReason().contains("Hop-Tasks"), dags.get(0).singleTaskReason());
  }

  @Test
  void failsTheDagOfAnObjectItCannotUse() throws Exception {
    HopWorkflowFile load = workflow("flows/load.hwf", TWO_ACTIONS);
    dagFile("bad", "{\"name\":\"bad\",\"workflow\":\"flows/load.hwf\",\"start_date\":\"soon\"}");
    dagFile("gone", "{\"name\":\"gone\",\"workflow\":\"flows/gone.hwf\"}");
    dagFile("nowhere", "{\"name\":\"no where\",\"workflow\":\"${OTHER}/x.hwf\"}");

    List<HopWorkflowFile> dags =
        HopBundleMain.dags(List.of(load), HopDagSettings.read(project), manifest("PER_ACTION"));

    assertEquals(
        List.of("p__flows__load", "p__flows__gone", "p__no_where"),
        dags.stream().map(HopWorkflowFile::dagId).toList());
    for (HopWorkflowFile dag : dags) {
      assertTrue(dag.tasks().isEmpty(), dag.dagId());
      assertTrue(dag.settings().error() != null, dag.dagId());
      assertEquals(Map.of(), dag.settings().config(), dag.dagId());
    }
    assertTrue(dags.get(0).settings().error().contains("start date soon"));
    assertEquals(dags.get(0).settings().error(), dags.get(0).description());
    assertEquals(load.version(), dags.get(0).version());
    assertTrue(
        dags.get(1).settings().error().endsWith("workflow flows/gone.hwf is not in the project"));
    assertEquals(dags.get(1).settings().error(), dags.get(1).description());
  }

  /** A second bundle of the same project can list other workflows. */
  @Test
  void skipsAnObjectOfAWorkflowTheBundleDoesNotList() throws Exception {
    workflow("flows/load.hwf", TWO_ACTIONS);
    dagFile("daily", "{\"name\":\"daily\",\"workflow\":\"flows/load.hwf\"}");
    dagFile("wrong", "{\"name\":\"wrong\",\"workflow\":\"flows/load.hwf\",\"start_date\":\"x\"}");

    assertEquals(
        List.of(),
        HopBundleMain.dags(List.of(), HopDagSettings.read(project), manifest("PER_ACTION")));
  }

  /** Airflow allows fail fast only with the default trigger rule. */
  @Test
  void failsAFailFastDagWithTasksThatWaitForAFailure() throws Exception {
    HopWorkflowFile workflow = workflow("flows/mail.hwf", FAILURE_HOP);
    dagFile("a-fast", "{\"name\":\"fast\",\"workflow\":\"flows/mail.hwf\",\"fail_fast\":true}");
    dagFile(
        "b-fast-one",
        "{\"name\":\"fast one\",\"workflow\":\"flows/mail.hwf\",\"fail_fast\":true,"
            + "\"dag_id\":\"mail_one\",\"tasks\":\"WHOLE_WORKFLOW\"}");

    List<HopWorkflowFile> dags =
        HopBundleMain.dags(List.of(workflow), HopDagSettings.read(project), manifest("PER_ACTION"));

    String error = dags.get(0).settings().error();
    assertTrue(error.contains("one_failed"), error);
    assertTrue(dags.get(0).tasks().isEmpty());
    // One task has the default trigger rule
    assertNull(dags.get(1).settings().error());
    assertEquals(true, dags.get(1).settings().config().get("fail_fast"));
  }
}
