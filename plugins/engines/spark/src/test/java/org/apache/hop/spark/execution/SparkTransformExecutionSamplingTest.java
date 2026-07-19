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

package org.apache.hop.spark.execution;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.local.FileExecutionInfoLocation;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.plugins.first.FirstRowsExecutionDataSampler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.spark.core.SparkExecutionDataAccumulator;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkTransformExecutionSamplingTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void lookupInactiveWithoutProfile() throws Exception {
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    FileExecutionInfoLocation fileLocation =
        new FileExecutionInfoLocation(tempDir.resolve("loc").toString());
    ExecutionInfoLocation locationMeta =
        new ExecutionInfoLocation("loc", "", "100", "200", null, null, fileLocation);
    metadata.getSerializer(ExecutionInfoLocation.class).save(locationMeta);

    SparkPipelineRunConfiguration spark = new SparkPipelineRunConfiguration();
    PipelineRunConfiguration runConfig = new PipelineRunConfiguration();
    runConfig.setName("rc");
    runConfig.setEngineRunConfiguration(spark);
    runConfig.setExecutionInfoLocationName("loc");
    // no data profile
    metadata.getSerializer(PipelineRunConfiguration.class).save(runConfig);

    SparkTransformExecutionSampling sampling =
        new SparkTransformExecutionSampling("Dummy", "parent-id", 0);
    sampling.lookup(new Variables(), metadata, "rc", "[]");
    assertFalse(sampling.isActive());
  }

  @Test
  void sampleAndRegisterData() throws Exception {
    Path root = tempDir.resolve("samples");
    Files.createDirectories(root);

    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    FileExecutionInfoLocation fileLocation = new FileExecutionInfoLocation(root.toString());
    ExecutionInfoLocation locationMeta =
        new ExecutionInfoLocation("loc", "", "50", "100", null, null, fileLocation);
    metadata.getSerializer(ExecutionInfoLocation.class).save(locationMeta);

    FirstRowsExecutionDataSampler first = new FirstRowsExecutionDataSampler();
    first.setSampleSize("5");
    first.setPluginId("FirstRowsExecutionDataSampler");
    ExecutionDataProfile profile = new ExecutionDataProfile("profile");
    profile.getSamplers().add(first);
    metadata.getSerializer(ExecutionDataProfile.class).save(profile);

    SparkPipelineRunConfiguration spark = new SparkPipelineRunConfiguration();
    PipelineRunConfiguration runConfig = new PipelineRunConfiguration();
    runConfig.setName("rc");
    runConfig.setEngineRunConfiguration(spark);
    runConfig.setExecutionInfoLocationName("loc");
    runConfig.setExecutionDataProfileName("profile");
    metadata.getSerializer(PipelineRunConfiguration.class).save(runConfig);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("sample-pipe");
    TransformMeta dummyMeta = new TransformMeta("Dummy", new DummyMeta());
    dummyMeta.setTransformPluginId("Dummy");
    pipelineMeta.addTransform(dummyMeta);

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    pipeline.setMetadataProvider(metadata);
    pipeline.prepareExecution();
    pipeline.startThreads();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    // Parent pipeline execution folder must exist before registerData (file location layout)
    Execution parentExecution = new Execution();
    parentExecution.setId("parent-log");
    parentExecution.setName("sample-pipe");
    parentExecution.setExecutionType(ExecutionType.Pipeline);
    parentExecution.setExecutionStartDate(new java.util.Date());
    fileLocation.initialize(new Variables(), metadata);
    fileLocation.registerExecution(parentExecution);

    SparkTransformExecutionSampling sampling =
        new SparkTransformExecutionSampling("Dummy", "parent-log", 0);
    sampling.lookup(new Variables(), metadata, "rc", "[]");
    assertTrue(sampling.isActive());

    ITransform transform = pipeline.getTransform("Dummy", 0);
    assertNotNull(transform);
    sampling.registerExecutingTransform(pipeline);
    sampling.attach(new Variables(), pipeline, transform, rowMeta, rowMeta);

    for (int i = 0; i < 3; i++) {
      for (var listener : transform.getRowListeners()) {
        listener.rowWrittenEvent(rowMeta, new Object[] {"row-" + i});
      }
    }

    sampling.sendSamplesToLocation(true);
    sampling.close();

    boolean anyData =
        Files.walk(root)
            .anyMatch(
                p ->
                    p.getFileName().toString().endsWith("-data.json")
                        || p.getFileName().toString().equals("execution.json"));
    assertTrue(anyData, "expected execution/data artifacts under " + root);
  }

  @Test
  void samplesAreShippedViaDriverAccumulator() throws Exception {
    Path root = tempDir.resolve("acc");
    Files.createDirectories(root);

    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    FileExecutionInfoLocation fileLocation = new FileExecutionInfoLocation(root.toString());
    ExecutionInfoLocation locationMeta =
        new ExecutionInfoLocation("loc", "", "50", "100", null, null, fileLocation);
    metadata.getSerializer(ExecutionInfoLocation.class).save(locationMeta);

    FirstRowsExecutionDataSampler first = new FirstRowsExecutionDataSampler();
    first.setSampleSize("5");
    first.setPluginId("FirstRowsExecutionDataSampler");
    ExecutionDataProfile profile = new ExecutionDataProfile("profile");
    profile.getSamplers().add(first);
    metadata.getSerializer(ExecutionDataProfile.class).save(profile);

    SparkPipelineRunConfiguration spark = new SparkPipelineRunConfiguration();
    PipelineRunConfiguration runConfig = new PipelineRunConfiguration();
    runConfig.setName("rc");
    runConfig.setEngineRunConfiguration(spark);
    runConfig.setExecutionInfoLocationName("loc");
    runConfig.setExecutionDataProfileName("profile");
    metadata.getSerializer(PipelineRunConfiguration.class).save(runConfig);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("sample-pipe");
    TransformMeta dummyMeta = new TransformMeta("Dummy", new DummyMeta());
    dummyMeta.setTransformPluginId("Dummy");
    pipelineMeta.addTransform(dummyMeta);

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(pipelineMeta, new Variables(), new LoggingObject("test"));
    pipeline.setMetadataProvider(metadata);
    pipeline.prepareExecution();
    pipeline.startThreads();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    SparkExecutionDataAccumulator accumulator = new SparkExecutionDataAccumulator();
    SparkTransformExecutionSampling sampling =
        new SparkTransformExecutionSampling("Dummy", "parent-log", 0, accumulator);
    sampling.lookup(new Variables(), metadata, "rc", "[]");
    assertTrue(sampling.isActive());

    ITransform transform = pipeline.getTransform("Dummy", 0);
    assertNotNull(transform);
    sampling.attach(new Variables(), pipeline, transform, rowMeta, rowMeta);

    for (int i = 0; i < 3; i++) {
      for (var listener : transform.getRowListeners()) {
        listener.rowWrittenEvent(rowMeta, new Object[] {"row-" + i});
      }
    }

    sampling.sendSamplesToLocation(true);
    sampling.close();

    Map<String, String> packed = accumulator.value();
    assertFalse(packed.isEmpty(), "expected sample JSON in driver accumulator");
    ExecutionData data =
        HopJson.newMapper().readValue(packed.values().iterator().next(), ExecutionData.class);
    assertNotNull(data);
    assertTrue(data.isFinished());
    assertNotNull(data.getDataSets());
    assertFalse(data.getDataSets().isEmpty(), "expected sample row buffers after JSON round-trip");
  }
}
