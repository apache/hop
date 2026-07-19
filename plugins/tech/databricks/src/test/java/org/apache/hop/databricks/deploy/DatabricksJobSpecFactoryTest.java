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

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class DatabricksJobSpecFactoryTest {

  @Test
  void createJobJsonContainsMainSparkAndPaths() throws Exception {
    String json =
        DatabricksJobSpecFactory.buildCreateJobJson(
            "hop-job",
            "cluster-1",
            "/FileStore/hop/j.jar",
            "/FileStore/hop/p.hpl",
            "/FileStore/hop/m.json",
            "spark-local");
    assertTrue(json.contains("hop-job"), json);
    assertTrue(json.contains("cluster-1"), json);
    assertTrue(json.contains("existing_cluster_id"), json);
    assertTrue(json.contains("MainSpark"), json);
    assertTrue(json.contains("j.jar"), json);
    assertTrue(json.contains("p.hpl"), json);
    assertTrue(json.contains("spark-local"), json);
    assertFalse(json.contains("environments"), json);
  }

  @Test
  void classicExistingClusterIdIsNotTreatedAsServerlessOrNewCluster() throws Exception {
    var target =
        DatabricksJobSpecFactory.resolveClusterTarget(
            "0718-abcdef-classic1", null, null, null, null, "default", "4");
    assertEquals(DatabricksJobSpecFactory.ComputeMode.EXISTING_CLUSTER, target.mode());
    assertEquals("0718-abcdef-classic1", target.existingClusterId());
    String json =
        DatabricksJobSpecFactory.buildCreateJobJson(
            "hop-job",
            target,
            "/Volumes/c/s/v/j.jar",
            "/Volumes/c/s/v/p.hpl",
            "/Volumes/c/s/v/m.json",
            "rc");
    assertTrue(json.contains("existing_cluster_id"), json);
    assertTrue(json.contains("\"libraries\""), json);
    assertFalse(json.contains("environment_key"), json);
    assertFalse(json.contains("new_cluster"), json);
  }

  @Test
  void resetJobJsonIncludesJobId() throws Exception {
    String json =
        DatabricksJobSpecFactory.buildResetJobJson(
            42L, "n", "c", "/a.jar", "/b.hpl", "/c.json", "rc");
    assertTrue(json.contains("\"job_id\":42") || json.contains("\"job_id\": 42"));
    assertTrue(json.contains("new_settings"));
  }

  @Test
  void toDbfsUriNormalizes() throws Exception {
    assertEquals("dbfs:/x", DatabricksJobSpecFactory.toDbfsUri("/x"));
    assertEquals("dbfs:/x", DatabricksJobSpecFactory.toDbfsUri("dbfs:/x"));
  }

  @Test
  void toDbfsUriKeepsVolumePathsWithoutDbfsScheme() throws Exception {
    assertEquals(
        "/Volumes/apache-hop/default/jars/hop-native.jar",
        DatabricksJobSpecFactory.toDbfsUri("/Volumes/apache-hop/default/jars/hop-native.jar"));
    assertEquals(
        "/Volumes/apache-hop/default/jars/hop-native.jar",
        DatabricksJobSpecFactory.toDbfsUri("dbfs:/Volumes/apache-hop/default/jars/hop-native.jar"));
  }

  @Test
  void createJobJsonVolumePathsDoNotGetDbfsScheme() throws Exception {
    String json =
        DatabricksJobSpecFactory.buildCreateJobJson(
            "hop-job",
            "cluster-1",
            "/Volumes/c/s/v/j.jar",
            "/Volumes/c/s/v/p.hpl",
            "/Volumes/c/s/v/m.json",
            "spark-local");
    // json-simple may escape slashes as \/
    assertTrue(json.contains("Volumes") && json.contains("j.jar"), json);
    assertFalse(json.contains("dbfs:"), json);
  }

  @Test
  void missingClusterFails() {
    assertThrows(
        HopException.class,
        () -> DatabricksJobSpecFactory.buildCreateJobJson("n", "", "/j", "/p", "/m", "rc"));
  }

  @Test
  void namedParametersForProjectPackageAndEnv() throws Exception {
    MainSparkLaunchSpec launch =
        new MainSparkLaunchSpec(
            "pipelines/hello.hpl",
            null,
            "databricks-native",
            "/Volumes/c/s/v/hop-spark-package.zip",
            "/Volumes/c/s/v/env-config.json");
    var params = DatabricksJobSpecFactory.buildMainSparkParameters(launch);
    assertEquals(4, params.size());
    assertEquals("--HopProjectPackage=/Volumes/c/s/v/hop-spark-package.zip", params.get(0));
    assertEquals("--HopPipelinePath=pipelines/hello.hpl", params.get(1));
    assertEquals("--HopRunConfigurationName=databricks-native", params.get(2));
    assertEquals("--HopConfigFile=/Volumes/c/s/v/env-config.json", params.get(3));
  }

  @Test
  void envOnlyUsesNamedParametersWithMetadata() throws Exception {
    MainSparkLaunchSpec launch =
        new MainSparkLaunchSpec(
            "/Volumes/c/s/v/pipeline.hpl",
            "/Volumes/c/s/v/metadata.json",
            "rc",
            null,
            "/Volumes/c/s/v/env-config.json");
    var params = DatabricksJobSpecFactory.buildMainSparkParameters(launch);
    String joined = params.toString();
    assertTrue(joined.contains("--HopPipelinePath="), joined);
    assertTrue(joined.contains("--HopMetadataPath="), joined);
    assertTrue(joined.contains("--HopConfigFile="), joined);
    assertFalse(joined.contains("--HopProjectPackage="), joined);
  }

  @Test
  void classicPositionalWhenNoPackageOrEnv() throws Exception {
    MainSparkLaunchSpec launch =
        MainSparkLaunchSpec.positional("/Volumes/c/s/v/p.hpl", "/Volumes/c/s/v/m.json", "rc");
    var params = DatabricksJobSpecFactory.buildMainSparkParameters(launch);
    assertEquals(3, params.size());
    assertEquals("/Volumes/c/s/v/p.hpl", params.get(0));
    assertEquals("/Volumes/c/s/v/m.json", params.get(1));
    assertEquals("rc", params.get(2));
  }

  @Test
  void newClusterTokenEmitsNestedObjectNotExistingClusterId() throws Exception {
    var cluster =
        DatabricksJobSpecFactory.buildNewClusterObject("18.2.x-scala2.13", "i3.xlarge", "1", null);
    String json =
        DatabricksJobSpecFactory.buildCreateJobJson(
            "hop-job",
            DatabricksJobSpecFactory.NEW_CLUSTER_TOKEN,
            cluster,
            "/Volumes/c/s/v/j.jar",
            "/Volumes/c/s/v/p.hpl",
            "/Volumes/c/s/v/m.json",
            "spark-local");
    assertTrue(json.contains("new_cluster"), json);
    assertTrue(json.contains("18.2.x-scala2.13"), json);
    assertTrue(json.contains("i3.xlarge"), json);
    assertFalse(json.contains("existing_cluster_id"), json);
    // Must not send the sentinel as a fake cluster id
    assertFalse(json.contains("\"existing_cluster_id\":\"new_cluster\""), json);
  }

  @Test
  void resolveClusterTargetBuildsJobCluster() throws Exception {
    var target =
        DatabricksJobSpecFactory.resolveClusterTarget(
            "new_cluster", "18.2.x-scala2.13", "i3.xlarge", "2", null, "default", "4");
    assertEquals(DatabricksJobSpecFactory.ComputeMode.NEW_CLUSTER, target.mode());
    assertEquals("18.2.x-scala2.13", target.newCluster().get("spark_version"));
    assertEquals("i3.xlarge", target.newCluster().get("node_type_id"));
    assertEquals(2L, ((Number) target.newCluster().get("num_workers")).longValue());
  }

  @Test
  void newClusterJsonOverrideWins() throws Exception {
    var cluster =
        DatabricksJobSpecFactory.buildNewClusterObject(
            "ignored",
            "ignored",
            "9",
            "{\"spark_version\":\"18.2.x-photon-scala2.13\",\"node_type_id\":\"i3.xlarge\",\"num_workers\":0}");
    assertEquals("18.2.x-photon-scala2.13", cluster.get("spark_version"));
    assertEquals("i3.xlarge", cluster.get("node_type_id"));
  }

  @Test
  void serverlessEmitsEnvironmentsAndEnvironmentKeyWithoutClusterFields() throws Exception {
    var target =
        DatabricksJobSpecFactory.resolveClusterTarget(
            "serverless", null, null, null, null, "default", "4");
    assertEquals(DatabricksJobSpecFactory.ComputeMode.SERVERLESS, target.mode());
    String json =
        DatabricksJobSpecFactory.buildCreateJobJson(
            "My Hop Job",
            target,
            "/Volumes/apache-hop/default/jars/hop-native.jar",
            "/Volumes/apache-hop/default/jars/pipeline.hpl",
            "/Volumes/apache-hop/default/jars/metadata.json",
            "databricks-native");
    assertTrue(json.contains("environments"), json);
    assertTrue(json.contains("environment_key"), json);
    assertTrue(json.contains("\"client\":\"4\"") || json.contains("\"client\": \"4\""), json);
    assertTrue(json.contains("java_dependencies"), json);
    assertTrue(json.contains("hop-native.jar"), json);
    // Serverless: jar in environment java_dependencies, not task libraries
    assertFalse(json.contains("\"libraries\""), json);
    assertFalse(json.contains("existing_cluster_id"), json);
    assertFalse(json.contains("new_cluster"), json);
    assertTrue(json.contains("MainSpark"), json);
  }
}
