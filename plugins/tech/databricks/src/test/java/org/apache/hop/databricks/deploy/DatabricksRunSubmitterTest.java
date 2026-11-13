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

package org.apache.hop.databricks.deploy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.DatabricksRunLifeCycleState;
import org.apache.hop.databricks.client.DatabricksRunStatus;
import org.apache.hop.databricks.deploy.DatabricksJobSpecFactory.ClusterTarget;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DatabricksRunSubmitterTest {

  private static final List<String> ARGS =
      List.of(
          "--HopProjectPackage=/Volumes/main/default/hop/staging/hop-spark-package-1a2b.zip",
          "--HopPipelinePath=pipelines/group-by.hpl",
          "--HopRunConfigurationName=databricks");

  private static ClusterTarget existingCluster() throws HopException {
    return DatabricksJobSpecFactory.resolveClusterTarget(
        "1126-082508-x0e8qaoz", null, null, null, null, null, null);
  }

  @Test
  void aRunIsOneJarTaskOfMainSparkWithTheStagedJarAndPackage() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.submitRun(anyString())).thenReturn(4711L);
    DatabricksRunSubmitter submitter =
        new DatabricksRunSubmitter(
            LogChannel.GENERAL, client, existingCluster(), "Databricks test");

    String id =
        submitter.submit(
            "/Volumes/main/default/hop/staging/hop-native-627c3a023abb.jar",
            DatabricksJobSpecFactory.MAIN_CLASS,
            ARGS,
            Map.of("spark.app.name", "Apache Hop - group-by"),
            Map.of());

    assertEquals("4711", id);
    ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(client).submitRun(body.capture());
    JSONObject run = (JSONObject) new JSONParser().parse(body.getValue());
    assertEquals("Apache Hop - group-by", run.get("run_name"));
    JSONObject task = (JSONObject) ((JSONArray) run.get("tasks")).get(0);
    assertEquals("1126-082508-x0e8qaoz", task.get("existing_cluster_id"));
    JSONObject jarTask = (JSONObject) task.get("spark_jar_task");
    assertEquals(DatabricksJobSpecFactory.MAIN_CLASS, jarTask.get("main_class_name"));
    assertEquals(
        List.of(
            "--HopProjectPackage=/Volumes/main/default/hop/staging/hop-spark-package-1a2b.zip",
            "--HopPipelinePath=pipelines/group-by.hpl",
            "--HopRunConfigurationName=databricks"),
        jarTask.get("parameters"));
    JSONObject library = (JSONObject) ((JSONArray) task.get("libraries")).get(0);
    assertEquals(
        "/Volumes/main/default/hop/staging/hop-native-627c3a023abb.jar", library.get("jar"));
  }

  @Test
  void aNewClusterRunCarriesTheJobCluster() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.submitRun(anyString())).thenReturn(1L);
    ClusterTarget newCluster =
        DatabricksJobSpecFactory.resolveClusterTarget(
            "new_cluster", "18.x-scala2.13", "Standard_D4ds_v5", "2", null, null, null);
    new DatabricksRunSubmitter(LogChannel.GENERAL, client, newCluster, "Databricks test")
        .submit(
            "/Volumes/v/hop/hop-native-1.jar",
            DatabricksJobSpecFactory.MAIN_CLASS,
            ARGS,
            Map.of(),
            Map.of());

    ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(client).submitRun(body.capture());
    JSONObject run = (JSONObject) new JSONParser().parse(body.getValue());
    JSONObject cluster =
        (JSONObject) ((JSONObject) ((JSONArray) run.get("tasks")).get(0)).get("new_cluster");
    assertEquals("18.x-scala2.13", cluster.get("spark_version"));
    assertEquals("Standard_D4ds_v5", cluster.get("node_type_id"));
    assertEquals(2L, ((Number) cluster.get("num_workers")).longValue());
  }

  @Test
  void anotherMainClassOrAMissingArgumentIsRefused() throws Exception {
    DatabricksRunSubmitter submitter =
        new DatabricksRunSubmitter(
            LogChannel.GENERAL,
            mock(DatabricksJobsClient.class),
            existingCluster(),
            "Databricks test");
    assertThrows(
        HopException.class,
        () -> submitter.submit("/Volumes/v/a.jar", "org.example.Main", ARGS, Map.of(), Map.of()));
    assertThrows(
        HopException.class,
        () ->
            submitter.submit(
                "/Volumes/v/a.jar",
                DatabricksJobSpecFactory.MAIN_CLASS,
                List.of("--HopPipelinePath=a.hpl"),
                Map.of(),
                Map.of()));
  }

  @Test
  void theRunStateBecomesTheDriverStatus() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    DatabricksRunSubmitter submitter =
        new DatabricksRunSubmitter(
            LogChannel.GENERAL, client, existingCluster(), "Databricks test");

    when(client.getRun(42L))
        .thenReturn(
            new DatabricksRunStatus(
                42L, null, DatabricksRunLifeCycleState.RUNNING, null, "", "https://run/42"));
    SparkDriverStatus running = submitter.status("42");
    assertEquals("RUNNING", running.state());
    assertEquals("https://run/42", running.location());
    assertFalse(running.isFinal());

    when(client.getRun(42L))
        .thenReturn(
            new DatabricksRunStatus(
                42L,
                null,
                DatabricksRunLifeCycleState.TERMINATED,
                "SUCCESS",
                "",
                "https://run/42"));
    SparkDriverStatus done = submitter.status("42");
    assertTrue(done.isFinal());
    assertTrue(done.isSuccess());
    assertEquals("SUCCESS", done.state());

    when(client.getRun(42L))
        .thenReturn(
            new DatabricksRunStatus(
                42L,
                null,
                DatabricksRunLifeCycleState.TERMINATED,
                "FAILED",
                "Pipeline failed",
                "https://run/42"));
    SparkDriverStatus failed = submitter.status("42");
    assertTrue(failed.isFinal());
    assertFalse(failed.isSuccess());
    assertEquals("Pipeline failed", failed.message());
  }

  @Test
  void stopCancelsTheRun() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    new DatabricksRunSubmitter(LogChannel.GENERAL, client, existingCluster(), "Databricks test")
        .kill("42");
    verify(client).cancelRun(42L);
  }

  @Test
  void olderHopFatJarsOnTheClusterAreRemovedAtItsNextRestart() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.submitRun(anyString())).thenReturn(1L);
    when(client.getClusterJarLibraries("1126-082508-x0e8qaoz"))
        .thenReturn(
            List.of(
                "/Volumes/main/default/hop/staging/hop-native-627c3a023abb.jar",
                "/Volumes/main/default/hop/staging/hop-native-28fc416f5741.jar",
                "/Volumes/main/default/libs/postgresql-42.7.4.jar"));
    ILogChannel log = mock(ILogChannel.class);
    new DatabricksRunSubmitter(log, client, existingCluster(), "Databricks test")
        .submit(
            "/Volumes/main/default/hop/staging/hop-native-28fc416f5741.jar",
            DatabricksJobSpecFactory.MAIN_CLASS,
            ARGS,
            Map.of(),
            Map.of());
    verify(client)
        .uninstallClusterJarLibraries(
            "1126-082508-x0e8qaoz",
            List.of("/Volumes/main/default/hop/staging/hop-native-627c3a023abb.jar"));
    verify(log)
        .logBasic(
            argThat(
                (String message) ->
                    message.contains("hop-native-627c3a023abb.jar")
                        && message.contains("restarts")));
  }

  @Test
  void aClusterWithOnlyThisFatJarOrAJobClusterIsLeftAlone() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.submitRun(anyString())).thenReturn(1L);
    when(client.getClusterJarLibraries("1126-082508-x0e8qaoz"))
        .thenReturn(List.of("/Volumes/v/hop/hop-native-28fc416f5741.jar"));
    new DatabricksRunSubmitter(LogChannel.GENERAL, client, existingCluster(), "Databricks test")
        .submit(
            "/Volumes/v/hop/hop-native-28fc416f5741.jar",
            DatabricksJobSpecFactory.MAIN_CLASS,
            ARGS,
            Map.of(),
            Map.of());
    verify(client, never()).uninstallClusterJarLibraries(anyString(), any());
  }
}
