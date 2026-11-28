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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.airflow.launcher.HopDagSettings;
import org.apache.hop.airflow.launcher.HopWorkflowFile;
import org.apache.hop.airflow.metadata.AirflowDagDefinition;
import org.apache.hop.airflow.metadata.AirflowDagObjectTasks;
import org.apache.hop.airflow.metadata.AirflowDagParameter;
import org.apache.hop.airflow.metadata.AirflowDagSwitch;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AirflowDagFilesTest {

  @TempDir Path project;

  private void save(AirflowDagDefinition... definitions) throws HopException {
    JsonMetadataProvider provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            project.resolve("metadata").toString(),
            Variables.getADefaultVariableSpace());
    for (AirflowDagDefinition definition : definitions) {
      provider.getSerializer(AirflowDagDefinition.class).save(definition);
    }
  }

  /** What Hop GUI saves is what the launcher on the workers reads. */
  @Test
  void writesWhatTheLauncherReads() throws Exception {
    AirflowDagDefinition daily = new AirflowDagDefinition("daily");
    daily.setWorkflow("${PROJECT_HOME}/flows/load.hwf");
    daily.setDagId("load_daily");
    daily.setDisplayName("Daily load");
    daily.setTags("sales, daily");
    daily.setTasks(AirflowDagObjectTasks.WHOLE_WORKFLOW);
    daily.setDocumentation("Loads the orders.");
    daily.setSchedule("0 6 * * *");
    daily.setStartDate("2026-01-31");
    daily.setEndDate("2026-12-31 18:30");
    daily.setCatchup(AirflowDagSwitch.NO);
    daily.setPausedOnCreation(AirflowDagSwitch.YES);
    daily.setMaxActiveRuns("1");
    daily.setMaxActiveTasks("4");
    daily.setRunTimeoutSeconds("3600");
    daily.setMaxConsecutiveFailedRuns("3");
    daily.setFailFast(true);
    daily.getParameters().add(new AirflowDagParameter("PRM_DAY", "today"));
    AirflowDagDefinition plain = new AirflowDagDefinition("plain");
    plain.setWorkflow("flows/load.hwf");
    save(daily, plain);

    List<HopDagSettings> launcher = HopDagSettings.read(project);

    assertEquals(2, launcher.size());
    HopDagSettings settings = launcher.get(0);
    assertNull(settings.error());
    assertEquals("daily", settings.name());
    assertEquals("flows/load.hwf", settings.workflow());
    assertEquals("load_daily", settings.dagId());
    assertEquals("WHOLE_WORKFLOW", settings.tasks());
    assertEquals(List.of("sales", "daily"), settings.tags());
    assertEquals("Loads the orders.", settings.documentation());
    assertEquals(Map.of("PRM_DAY", "today"), settings.parameters());
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

    // An object with only a workflow leaves everything else to Airflow and the bundle
    HopDagSettings plainSettings = launcher.get(1);
    assertNull(plainSettings.error());
    assertEquals(Map.of(), plainSettings.config());
    assertNull(plainSettings.dagId());

    List<AirflowDagFiles.PublishedDag> hop = AirflowDagFiles.read(project.toString());
    assertEquals(launcher.size(), hop.size());
    for (int i = 0; i < hop.size(); i++) {
      assertEquals(launcher.get(i).name(), hop.get(i).name());
      assertEquals(launcher.get(i).workflow(), hop.get(i).workflow());
      assertEquals(launcher.get(i).dagId(), hop.get(i).dagId());
      assertEquals(launcher.get(i).version(), hop.get(i).version());
      assertEquals(launcher.get(i).parameters().keySet(), hop.get(i).parameters());
    }
    // Hop GUI stores BUNDLE_DEFAULT by default: the object follows the tasks of the bundle
    assertNull(launcher.get(1).tasks());
    assertNull(hop.get(1).tasks());
    assertEquals(launcher.get(0).tasks(), hop.get(0).tasks());
  }

  @Test
  void findsTheMetadataFolderAsTheLauncherDoes() throws Exception {
    Files.writeString(
        project.resolve("project-config.json"),
        "{\"metadataBaseFolder\":\"${PROJECT_HOME}/meta\"}");
    Path folder = Files.createDirectories(project.resolve("meta/airflow-dag"));
    Files.writeString(folder.resolve("x.json"), "{\"name\":\"x\",\"workflow\":\"a.hwf\"}");

    assertEquals(1, HopDagSettings.read(project).size());
    assertEquals(1, AirflowDagFiles.read(project.toString()).size());
  }

  @Test
  void readsPathsAsTheLauncherDoes() {
    for (String path :
        List.of(
            "${PROJECT_HOME}/flows/a.hwf",
            "${PROJECT_HOME}\\flows\\a.hwf",
            "./flows/x/../a.hwf",
            "${PROJECT_HOME}",
            "${PROJECT_HOME}flows/a.hwf",
            "${PROJECT_HOME}/../other/a.hwf",
            "${FLOWS}/a.hwf",
            "/opt/flows/a.hwf",
            "file:///opt/flows/a.hwf",
            "~/a.hwf",
            "flows/a.hwf")) {
      assertEquals(
          HopDagSettings.projectRelative(path), AirflowDagFiles.projectRelative(path), path);
    }
  }

  @Test
  void namesTheDagOfAnObjectAsTheLauncherDoes() {
    for (String id : List.of("load_daily", "load daily", "x.y-z", "a/b")) {
      assertEquals(
          HopWorkflowFile.objectDagId("p__", id),
          AirflowDagIds.objectDagId("p__", "project", id),
          id);
    }
    assertEquals("My_Project__daily", AirflowDagIds.objectDagId("", "My Project", "daily"));
  }

  @Test
  void choosesTheDagOfTheWorkflow() throws Exception {
    AirflowDagFiles.PublishedDag daily =
        new AirflowDagFiles.PublishedDag("daily", "flows/load.hwf", null, null, "v1", Set.of());
    AirflowDagFiles.PublishedDag hourly =
        new AirflowDagFiles.PublishedDag("hourly", "flows/load.hwf", "h", null, "v2", Set.of());
    AirflowDagFiles.PublishedDag other =
        new AirflowDagFiles.PublishedDag("other", "flows/other.hwf", null, null, "v3", Set.of());

    assertNull(AirflowWorkflowEngine.chooseDag(List.of(other), "flows/load.hwf", null));
    // The only object of the workflow, also when the run configuration names another one
    assertEquals(
        daily, AirflowWorkflowEngine.chooseDag(List.of(daily, other), "flows/load.hwf", "other"));
    assertEquals(
        hourly,
        AirflowWorkflowEngine.chooseDag(List.of(daily, hourly, other), "flows/load.hwf", "hourly"));
    HopException error =
        assertThrows(
            HopException.class,
            () ->
                AirflowWorkflowEngine.chooseDag(
                    List.of(daily, hourly, other), "flows/load.hwf", ""));
    assertTrue(error.getMessage().contains("daily, hourly"), error.getMessage());
  }
}
