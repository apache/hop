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

package org.apache.hop.spark.engines.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkDeployMode;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.engines.SparkSetting;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The checks of a native Spark run configuration, and what they say for each problem. */
class SparkConfigurationCheckerTest {

  @TempDir Path folder;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  /** Check name to result, in the order the checks ran. */
  private static Map<String, ICheckResult> check(SparkPipelineRunConfiguration config) {
    Map<String, ICheckResult> byName = new LinkedHashMap<>();
    for (ICheckResult result :
        SparkConfigurationChecker.standard().check(config, new Variables(), null)) {
      byName.put(result.getSourceInfo().getName(), result);
    }
    return byName;
  }

  private static SparkPipelineRunConfiguration cluster(SparkClusterType type, String master) {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(type);
    config.setSparkMaster(master);
    return config;
  }

  @Test
  void aLocalRunOnlyChecksWhatALocalRunUses() {
    Map<String, ICheckResult> results = check(cluster(SparkClusterType.LOCAL, "local[*]"));
    assertEquals(
        List.of("Settings", "Cluster type", "Spark and Java versions"),
        List.copyOf(results.keySet()));
    assertEquals(ICheckResult.TYPE_RESULT_OK, results.get("Settings").getType());
    assertTrue(results.get("Spark and Java versions").getText().contains("Hop runs Spark 4."));
  }

  @Test
  void settingsSparkRefusesFailWithTheValue() {
    SparkPipelineRunConfiguration config = cluster(SparkClusterType.LOCAL, "local[*]");
    config.setDriverMemory("2 gigs");
    config.setExecutorCores("0");
    config.setSourcePartitions("four");
    config.setSparkSettings(
        new java.util.ArrayList<>(
            List.of(
                new SparkSetting("spark.ui.enabled", "false"), new SparkSetting("", "orphan"))));
    ICheckResult settings = check(config).get("Settings");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, settings.getType());
    assertTrue(settings.getText().contains("'2 gigs'"), settings.getText());
    assertTrue(settings.getText().contains("1 or more, not 0"), settings.getText());
    assertTrue(settings.getText().contains("source partitions"), settings.getText());
    assertTrue(settings.getText().contains("row 2 has a value but no key"), settings.getText());

    config = cluster(SparkClusterType.LOCAL, "local[*]");
    config.setSparkConfigs("spark.sql.shuffle.partitions=8\nspark.sql.shuffle.partitions=16");
    settings = check(config).get("Settings");
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, settings.getType());
    assertTrue(settings.getText().contains("set twice"), settings.getText());

    config = cluster(SparkClusterType.LOCAL, "local[*]");
    config.setSparkConfigs("fs.s3a.endpoint=http://minio:9000");
    settings = check(config).get("Settings");
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, settings.getType());
    assertTrue(settings.getText().contains("spark.hadoop."), settings.getText());
  }

  @Test
  void aTypeAndAMasterThatDisagreeFailWithTheFix() {
    ICheckResult type =
        check(cluster(SparkClusterType.YARN, "spark://spark:7077")).get("Cluster type");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, type.getType());
    assertTrue(type.getText().contains("Fix:"), type.getText());
    assertTrue(type.getText().contains("'yarn'"), type.getText());
  }

  @Test
  void aFatJarWithSparkInsideClashesWithTheCluster() throws Exception {
    SparkPipelineRunConfiguration config = cluster(SparkClusterType.YARN, "yarn");
    assertEquals(
        ICheckResult.TYPE_RESULT_OK, check(config).get("Fat jar").getType(), "built by Hop");

    config.setFatJar(folder.resolve("missing.jar").toString());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, check(config).get("Fat jar").getType());

    config.setFatJar(jar("with-spark.jar", FatJarCheck.SPARK_CLASS).getPath());
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, check(config).get("Fat jar").getType());

    config.setFatJar(jar("provided.jar", "org/apache/hop/Hop.class").getPath());
    assertEquals(ICheckResult.TYPE_RESULT_OK, check(config).get("Fat jar").getType());
  }

  @Test
  void clusterModeNeedsAStagingFolderTheClusterCanRead() {
    SparkPipelineRunConfiguration config = cluster(SparkClusterType.STANDALONE, "spark://s:7077");
    config.setDeployMode(SparkDeployMode.CLUSTER);
    Map<String, ICheckResult> results = check(config);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, results.get("Staging folder").getType());
    assertFalse(results.containsKey("Staging folder for the cluster"), "nothing to read yet");

    config.setStagingFolder(folder.toString());
    results = check(config);
    assertEquals(ICheckResult.TYPE_RESULT_OK, results.get("Staging folder").getType());
    assertEquals(
        ICheckResult.TYPE_RESULT_WARNING,
        results.get("Staging folder for the cluster").getType(),
        "a local path only works when the cluster mounts it");

    config.setStagingClusterUrl("minio://hop-staging");
    assertEquals(
        ICheckResult.TYPE_RESULT_ERROR,
        check(config).get("Staging folder for the cluster").getType(),
        "Spark can't read a Hop connection");

    config.setStagingClusterUrl("s3a://hop-staging");
    assertEquals(
        ICheckResult.TYPE_RESULT_OK, check(config).get("Staging folder for the cluster").getType());

    config.setDeployMode(SparkDeployMode.CLIENT);
    assertFalse(check(config).containsKey("Staging folder"), "client mode stages nothing");
  }

  @Test
  void aCheckWhosePrerequisiteFailedIsSkipped() {
    SparkCheckContext context =
        new SparkCheckContext(cluster(SparkClusterType.LOCAL, "local[*]"), new Variables());
    ISparkConfigurationCheck failing =
        new BaseSparkConfigurationCheck("Settings") {
          @Override
          public boolean appliesTo(SparkCheckContext c) {
            return true;
          }

          @Override
          public ICheckResult run(SparkCheckContext c) {
            return error("broken", "mend it");
          }
        };
    ISparkConfigurationCheck dependent =
        new BaseSparkConfigurationCheck("ClusterType") {
          @Override
          public boolean appliesTo(SparkCheckContext c) {
            return true;
          }

          @Override
          public List<String> getRequiredChecks() {
            return List.of("Settings");
          }

          @Override
          public ICheckResult run(SparkCheckContext c) {
            throw new AssertionError("must not run");
          }
        };
    context.putResult("Settings", SparkConfigurationChecker.run(failing, context));
    ICheckResult skipped = SparkConfigurationChecker.run(dependent, context);
    assertEquals(ICheckResult.TYPE_RESULT_COMMENT, skipped.getType());
    assertTrue(skipped.getText().startsWith("Skipped"), skipped.getText());
  }

  /** A class conflict fails its own check, on one line, instead of the whole test. */
  @Test
  void aCheckThatBreaksFailsWithTheReasonOnOneLine() {
    SparkCheckContext context =
        new SparkCheckContext(cluster(SparkClusterType.LOCAL, "local[*]"), new Variables());
    ICheckResult broken =
        SparkConfigurationChecker.run(
            new BaseSparkConfigurationCheck("Settings") {
              @Override
              public boolean appliesTo(SparkCheckContext c) {
                return true;
              }

              @Override
              public ICheckResult run(SparkCheckContext c) {
                throw new NoSuchMethodError(
                    "\nUnexpected error starting Spark pipeline\n'Buffer.empty()'");
              }
            },
            context);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, broken.getType());
    assertEquals(
        "The check failed: Unexpected error starting Spark pipeline: 'Buffer.empty()'",
        broken.getText());
  }

  private File jar(String name, String entry) throws Exception {
    File file = folder.resolve(name).toFile();
    try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file))) {
      zip.putNextEntry(new ZipEntry(entry));
      zip.write(new byte[] {1, 2, 3});
      zip.closeEntry();
    }
    return file;
  }
}
