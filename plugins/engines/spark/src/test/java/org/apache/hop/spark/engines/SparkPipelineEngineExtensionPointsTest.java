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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The native Spark engine fires the pipeline extension points of the local engine, once. Only the
 * calls of the Spark engine itself count: the local pipelines that run Hop transforms on the
 * partitions fire their own.
 */
class SparkPipelineEngineExtensionPointsTest {

  static final List<String> FIRED = new CopyOnWriteArrayList<>();

  @ExtensionPoint(
      id = "SparkTestPrepareExecutionXp",
      extensionPointId = "PipelinePrepareExecution",
      description = "")
  public static class PrepareExecutionXp implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> engine) {
      if (engine instanceof SparkPipelineEngine) {
        FIRED.add("PipelinePrepareExecution");
      }
    }
  }

  @ExtensionPoint(
      id = "SparkTestStartThreadsXp",
      extensionPointId = "PipelineStartThreads",
      description = "")
  public static class StartThreadsXp implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> engine) {
      if (engine instanceof SparkPipelineEngine) {
        FIRED.add("PipelineStartThreads");
      }
    }
  }

  @ExtensionPoint(id = "SparkTestStartXp", extensionPointId = "PipelineStart", description = "")
  public static class StartXp implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> engine) {
      if (engine instanceof SparkPipelineEngine) {
        FIRED.add("PipelineStart");
      }
    }
  }

  @ExtensionPoint(id = "SparkTestFinishXp", extensionPointId = "PipelineFinish", description = "")
  public static class FinishXp implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> engine) {
      if (engine instanceof SparkPipelineEngine) {
        FIRED.add("PipelineFinish");
      }
    }
  }

  @ExtensionPoint(
      id = "SparkTestCompletedXp",
      extensionPointId = "PipelineCompleted",
      description = "")
  public static class CompletedXp implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> engine) {
      if (engine instanceof SparkPipelineEngine) {
        FIRED.add("PipelineCompleted");
      }
    }
  }

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (Class<?> xp :
        List.of(PrepareExecutionXp.class, StartThreadsXp.class, StartXp.class, FinishXp.class)) {
      registry.registerPluginClass(
          xp.getName(), ExtensionPointPluginType.class, ExtensionPoint.class);
    }
    registry.registerPluginClass(
        CompletedXp.class.getName(), ExtensionPointPluginType.class, ExtensionPoint.class);
  }

  @Test
  void finishThenCompletedOncePerRun() throws Exception {
    SparkPipelineEngine engine = new SparkPipelineEngine();
    List<String> order = new CopyOnWriteArrayList<>();
    engine.addExecutionFinishedListener(e -> order.add("listener"));
    FIRED.clear();

    engine.fireExecutionFinishedListeners();
    // Fired again from outside (firePipelineExecutionFinishedListeners)
    engine.firePipelineExecutionFinishedListeners();

    assertEquals(List.of("PipelineFinish", "PipelineCompleted"), FIRED);
    assertEquals(List.of("listener", "listener"), order);
  }

  /** A run on a local Spark session: every extension point once, in the local engine's order. */
  @Test
  void aRunFiresTheExtensionPointsOfTheLocalEngineInOrder(@TempDir Path tempDir) throws Exception {
    Path input = tempDir.resolve("input.csv");
    Files.writeString(input, "1\n2\n3\n", StandardCharsets.UTF_8);

    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    SparkPipelineRunConfiguration spark = new SparkPipelineRunConfiguration();
    spark.setSparkMaster("local[1]");
    spark.setSparkAppName("hop-extension-points-test");
    spark.setSparkConfigs("spark.ui.enabled=false\nspark.driver.host=localhost");
    PipelineRunConfiguration runConfiguration = new PipelineRunConfiguration();
    runConfiguration.setName("spark-local");
    runConfiguration.setEngineRunConfiguration(spark);
    metadata.getSerializer(PipelineRunConfiguration.class).save(runConfiguration);

    PipelineMeta pipelineMeta =
        new PipelineMeta(
            new ByteArrayInputStream(
                PIPELINE_XML.formatted(input.toUri().toString()).getBytes(StandardCharsets.UTF_8)),
            metadata,
            new Variables());
    SparkPipelineEngine engine =
        new SparkPipelineEngine(pipelineMeta, new LoggingObject("test"), new Variables());
    engine.setMetadataProvider(metadata);
    engine.setPipelineRunConfiguration(runConfiguration);
    FIRED.clear();

    engine.prepareExecution();
    engine.startThreads();
    engine.waitUntilFinished();

    assertEquals(0, engine.getErrors());
    assertEquals(
        List.of(
            "PipelinePrepareExecution",
            "PipelineStartThreads",
            "PipelineStart",
            "PipelineFinish",
            "PipelineCompleted"),
        FIRED);
  }

  /**
   * A run in cluster deploy mode fires the same extension points: plugins that follow a run, such
   * as execution logging, see it start. This one fails before it submits (no project), as any
   * cluster run that can't be submitted.
   */
  @Test
  void aClusterModeRunFiresTheExtensionPointsToo(@TempDir Path tempDir) throws Exception {
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    SparkPipelineRunConfiguration spark = new SparkPipelineRunConfiguration();
    spark.setClusterType(SparkClusterType.STANDALONE);
    spark.setSparkMaster("spark://127.0.0.1:1");
    spark.setDeployMode(SparkDeployMode.CLUSTER);
    PipelineRunConfiguration runConfiguration = new PipelineRunConfiguration();
    runConfiguration.setName("spark-cluster");
    runConfiguration.setEngineRunConfiguration(spark);
    metadata.getSerializer(PipelineRunConfiguration.class).save(runConfiguration);

    PipelineMeta pipelineMeta =
        new PipelineMeta(
            new ByteArrayInputStream(
                PIPELINE_XML
                    .formatted(tempDir.resolve("input.csv").toUri().toString())
                    .getBytes(StandardCharsets.UTF_8)),
            metadata,
            new Variables());
    SparkPipelineEngine engine =
        new SparkPipelineEngine(pipelineMeta, new LoggingObject("test"), new Variables());
    engine.setMetadataProvider(metadata);
    engine.setPipelineRunConfiguration(runConfiguration);
    FIRED.clear();

    engine.prepareExecution();
    engine.startThreads();
    engine.waitUntilFinished();

    assertEquals(1, engine.getErrors(), "no project to package");
    assertEquals(
        List.of(
            "PipelinePrepareExecution",
            "PipelineStartThreads",
            "PipelineStart",
            "PipelineFinish",
            "PipelineCompleted"),
        FIRED);
  }

  /** Text File Input reads a local file into Dummy. */
  private static final String PIPELINE_XML =
      """
      <pipeline>
        <info>
          <name>extension-points</name>
          <pipeline_type>Normal</pipeline_type>
        </info>
        <order>
          <hop><from>read</from><to>done</to><enabled>Y</enabled></hop>
        </order>
        <transform>
          <name>read</name>
          <type>TextFileInput2</type>
          <file_type>CSV</file_type>
          <compression>None</compression>
          <separator>,</separator>
          <header>N</header>
          <format>mixed</format>
          <encoding>UTF-8</encoding>
          <date_format_lenient>Y</date_format_lenient>
          <date_format_locale>en_US</date_format_locale>
          <files>
            <file>
              <name>%s</name>
              <file_required>Y</file_required>
              <include_subfolders>N</include_subfolders>
            </file>
          </files>
          <fields>
            <field>
              <name>n</name>
              <type>String</type>
              <position>-1</position>
              <length>-1</length>
              <precision>-1</precision>
              <trim_type>none</trim_type>
              <repeat>N</repeat>
            </field>
          </fields>
          <copies>1</copies>
        </transform>
        <transform>
          <name>done</name>
          <type>Dummy</type>
          <copies>1</copies>
        </transform>
      </pipeline>
      """;
}
