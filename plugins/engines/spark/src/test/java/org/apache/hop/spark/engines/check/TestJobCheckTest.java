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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transforms.constant.ConstantMeta;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkDeployMode;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The test job runs its pipeline with the configuration, here on local Spark. */
class TestJobCheckTest {

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    for (Class<?> meta : List.of(RowGeneratorMeta.class, ConstantMeta.class)) {
      PluginRegistry.getInstance()
          .registerPluginClass(meta.getName(), TransformPluginType.class, Transform.class);
    }
  }

  private static SparkPipelineRunConfiguration local() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(SparkClusterType.LOCAL);
    config.setSparkMaster("local[2]");
    config.setSparkConfigs("spark.ui.enabled=false\nspark.driver.host=localhost");
    return config;
  }

  private static ICheckResult testJob(SparkPipelineRunConfiguration config) {
    List<ICheckResult> results =
        SparkConfigurationChecker.standard()
            .check(config, new Variables(), new MemoryMetadataProvider(), null);
    ICheckResult last = results.get(results.size() - 1);
    assertEquals("Test job", last.getSourceInfo().getName());
    return last;
  }

  @Test
  void theRowsGoThroughAndTheirCountsComeBack() {
    ICheckResult result = testJob(local());
    assertEquals(ICheckResult.TYPE_RESULT_OK, result.getType(), result.getText());
    assertTrue(result.getText().contains("1000 rows"), result.getText());
  }

  /** While the job runs the progress says what it does, with the seconds going up. */
  @Test
  void theProgressSaysWhatTheJobDoes() {
    List<String> subTasks = new java.util.concurrent.CopyOnWriteArrayList<>();
    IProgressMonitor monitor =
        new IProgressMonitor() {
          @Override
          public void beginTask(String message, int nrWorks) {}

          @Override
          public void subTask(String message) {
            subTasks.add(message);
          }

          @Override
          public boolean isCanceled() {
            return false;
          }

          @Override
          public void worked(int nrWorks) {}

          @Override
          public void done() {}

          @Override
          public void setTaskName(String taskName) {}
        };
    List<ICheckResult> results =
        SparkConfigurationChecker.standard()
            .check(local(), new Variables(), new MemoryMetadataProvider(), monitor);
    assertEquals(ICheckResult.TYPE_RESULT_OK, results.get(results.size() - 1).getType());
    assertTrue(
        subTasks.stream().anyMatch(line -> line.startsWith("Test job, ")),
        "the job reports its progress: " + subTasks);

    // Before rows come back the line is the engine's last message, shortened to fit
    String line =
        TestJobCheck.progress(
            new org.apache.hop.spark.engines.SparkPipelineEngine(),
            "Fat jar: 240 of 480 jar files added (50%)",
            42_000);
    assertEquals("Test job, 42 s: Fat jar: 240 of 480 jar files added (50%)", line);
    assertEquals(100, TestJobCheck.shorten("x".repeat(300)).length());
    assertEquals("Building it now", TestJobCheck.shorten("\nBuilding it\n  now"));
  }

  @Test
  void aConfigurationWithFailuresDoesNotRunTheJob() {
    SparkPipelineRunConfiguration config = local();
    config.setExecutorCores("none");
    ICheckResult result = testJob(config);
    assertEquals(ICheckResult.TYPE_RESULT_COMMENT, result.getType());
    assertTrue(result.getText().startsWith("Skipped"), result.getText());
  }

  /**
   * In cluster mode the driver loads the run configuration by name from the project package. The
   * tested one may not be saved: it goes into the metadata of the run, over one of the same name.
   */
  @Test
  void inClusterModeTheTestedConfigurationGoesWithTheRun() throws Exception {
    MemoryMetadataProvider project = new MemoryMetadataProvider();
    PipelineRunConfiguration saved = new PipelineRunConfiguration();
    saved.setName(TestJobCheck.RUN_CONFIGURATION);
    saved.setEngineRunConfiguration(local());
    project.getSerializer(PipelineRunConfiguration.class).save(saved);

    SparkPipelineRunConfiguration edited = local();
    edited.setClusterType(SparkClusterType.STANDALONE);
    edited.setSparkMaster("spark://spark:7077");
    edited.setDeployMode(SparkDeployMode.CLUSTER);
    PipelineRunConfiguration tested = new PipelineRunConfiguration();
    tested.setName(TestJobCheck.RUN_CONFIGURATION);
    tested.setEngineRunConfiguration(edited);

    IHopMetadataProvider metadata =
        TestJobCheck.withRunConfiguration(project, tested, new Variables());
    PipelineRunConfiguration loaded =
        metadata.getSerializer(PipelineRunConfiguration.class).load(TestJobCheck.RUN_CONFIGURATION);
    assertEquals(
        "spark://spark:7077",
        ((SparkPipelineRunConfiguration) loaded.getEngineRunConfiguration()).getSparkMaster());
    // What the package's metadata.json gets, as the driver reads it back
    SerializableMetadataProvider shipped =
        new SerializableMetadataProvider(new SerializableMetadataProvider(metadata).toJson());
    PipelineRunConfiguration onTheDriver =
        shipped.getSerializer(PipelineRunConfiguration.class).load(TestJobCheck.RUN_CONFIGURATION);
    assertEquals(
        "spark://spark:7077",
        ((SparkPipelineRunConfiguration) onTheDriver.getEngineRunConfiguration()).getSparkMaster());
  }

  @Test
  void thePlainTestLeavesTheJobOut() {
    for (ICheckResult result :
        SparkConfigurationChecker.standard().check(local(), new Variables(), null)) {
      assertTrue(!"Test job".equals(result.getSourceInfo().getName()));
    }
  }
}
