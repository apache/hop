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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.local.FileExecutionInfoLocation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies driver-side execution information location wiring for {@link SparkPipelineEngine}
 * without running a full Spark job.
 */
class SparkPipelineEngineExecutionInfoTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void registerAndUpdateExecutionInfoToFileLocation() throws Exception {
    Path root = tempDir.resolve("exec-info");
    Files.createDirectories(root);

    FileExecutionInfoLocation fileLocation = new FileExecutionInfoLocation(root.toString());
    ExecutionInfoLocation locationMeta =
        new ExecutionInfoLocation(
            "spark-exec", "test location", "100", "200", null, null, fileLocation);

    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    metadata.getSerializer(ExecutionInfoLocation.class).save(locationMeta);

    SparkPipelineRunConfiguration sparkEngine = new SparkPipelineRunConfiguration();
    sparkEngine.setSparkMaster("local[1]");
    sparkEngine.setSparkAppName("exec-info-test");

    PipelineRunConfiguration runConfig = new PipelineRunConfiguration();
    runConfig.setName("spark-local");
    runConfig.setEngineRunConfiguration(sparkEngine);
    runConfig.setExecutionInfoLocationName("spark-exec");
    metadata.getSerializer(PipelineRunConfiguration.class).save(runConfig);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("spark-exec-pipe");
    TransformMeta dummy = new TransformMeta("Dummy", new DummyMeta());
    dummy.setTransformPluginId("Dummy");
    pipelineMeta.addTransform(dummy);

    SparkPipelineEngine engine = new SparkPipelineEngine();
    engine.setPipelineMeta(pipelineMeta);
    engine.setMetadataProvider(metadata);
    engine.setPipelineRunConfiguration(runConfig);
    engine.setExecutionStartDate(new Date());

    // Mimic prepareExecution log channel setup
    var logField = SparkPipelineEngine.class.getDeclaredField("logChannel");
    logField.setAccessible(true);
    logField.set(engine, new LogChannel(engine));

    engine.lookupExecutionInformationLocation();
    engine.registerPipelineExecutionInformation();

    // lookupExecutionInformationLocation clones the metadata location and initialize()s it.
    // Use that instance (not the raw pre-init fileLocation) for update/register paths.
    var locationField = SparkPipelineEngine.class.getDeclaredField("executionInfoLocation");
    locationField.setAccessible(true);
    ExecutionInfoLocation engineLocation = (ExecutionInfoLocation) locationField.get(engine);
    assertNotNull(engineLocation, "expected execution info location after lookup");
    IExecutionInfoLocation iLocation = engineLocation.getExecutionInfoLocation();
    assertNotNull(iLocation);

    // Seed components with stable log channel ids
    EngineComponent component = new EngineComponent("Dummy", 0);
    component.setStatus(ComponentExecutionStatus.STATUS_RUNNING);
    component.setLinesRead(10);
    component.setLinesWritten(10);
    // Use populate path via seed
    var seed = SparkPipelineEngine.class.getDeclaredMethod("seedEngineMetricsComponents");
    seed.setAccessible(true);
    seed.invoke(engine);

    engine.updatePipelineState(iLocation);

    // Final update + close
    engine.stopExecutionInfoTimer();

    String logId = engine.getLogChannelId();
    assertNotNull(logId);
    Path executionFolder = root.resolve(logId);
    assertTrue(Files.isDirectory(executionFolder), "expected folder " + executionFolder);
    assertTrue(Files.exists(executionFolder.resolve("execution.json")));
    assertTrue(Files.exists(executionFolder.resolve("state.json")));

    // Second stop is idempotent
    engine.stopExecutionInfoTimer();
    assertFalse(Files.list(root).findAny().isEmpty());
  }
}
