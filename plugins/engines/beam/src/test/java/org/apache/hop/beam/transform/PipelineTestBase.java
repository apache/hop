/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.transform;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineRunConfiguration;
import org.apache.hop.beam.engines.flink.BeamFlinkPipelineEngine;
import org.apache.hop.beam.engines.spark.BeamSparkPipelineEngine;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.transforms.bq.BeamBQInputMeta;
import org.apache.hop.beam.transforms.bq.BeamBQOutputMeta;
import org.apache.hop.beam.transforms.io.BeamInputMeta;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.beam.transforms.kafka.BeamConsumeMeta;
import org.apache.hop.beam.transforms.kafka.BeamProduceMeta;
import org.apache.hop.beam.transforms.pubsub.BeamPublishMeta;
import org.apache.hop.beam.transforms.pubsub.BeamSubscribeMeta;
import org.apache.hop.beam.transforms.window.BeamTimestampMeta;
import org.apache.hop.beam.transforms.window.BeamWindowMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.local.FileExecutionInfoLocation;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPluginType;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerPlugin;
import org.apache.hop.execution.sampler.ExecutionDataSamplerPluginType;
import org.apache.hop.execution.sampler.plugins.first.FirstRowsExecutionDataSampler;
import org.apache.hop.execution.sampler.plugins.last.LastRowsExecutionDataSampler;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.pipeline.transforms.constant.ConstantMeta;
import org.apache.hop.pipeline.transforms.filterrows.FilterRowsMeta;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta;
import org.apache.hop.pipeline.transforms.mergejoin.MergeJoinMeta;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta;
import org.apache.hop.pipeline.transforms.switchcase.SwitchCaseMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

public class PipelineTestBase {

  public static final String INPUT_CUSTOMERS_FILE =
      System.getProperty("java.io.tmpdir") + "/customers/io/customers-100.txt";
  public static final String INPUT_STATES_FILE =
      System.getProperty("java.io.tmpdir") + "/customers/io/state-data.txt";
  public static final String NAME_LOCATION = "location";
  public static final String NAME_DATA_PROFILE = "first-last-20";
  public static final String NAME_RUN_CONFIG = "direct";

  protected IVariables variables;
  protected IHopMetadataProvider metadataProvider;

  @BeforeEach
  void setUp() throws Exception {

    variables = Variables.getADefaultVariableSpace();

    List<String> beamTransforms =
        Arrays.asList(
            BeamBQInputMeta.class.getName(),
            BeamBQOutputMeta.class.getName(),
            BeamInputMeta.class.getName(),
            BeamOutputMeta.class.getName(),
            BeamConsumeMeta.class.getName(),
            BeamProduceMeta.class.getName(),
            BeamSubscribeMeta.class.getName(),
            BeamPublishMeta.class.getName(),
            BeamTimestampMeta.class.getName(),
            BeamWindowMeta.class.getName(),
            ConstantMeta.class.getName(),
            FilterRowsMeta.class.getName(),
            MemoryGroupByMeta.class.getName(),
            MergeJoinMeta.class.getName(),
            StreamLookupMeta.class.getName(),
            SwitchCaseMeta.class.getName());

    BeamHop.init();

    PluginRegistry.getInstance()
        .registerPluginClass(
            BeamDirectPipelineEngine.class.getName(),
            PipelineEnginePluginType.class,
            PipelineEnginePlugin.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            BeamFlinkPipelineEngine.class.getName(),
            PipelineEnginePluginType.class,
            PipelineEnginePlugin.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            BeamSparkPipelineEngine.class.getName(),
            PipelineEnginePluginType.class,
            PipelineEnginePlugin.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            FirstRowsExecutionDataSampler.class.getName(),
            ExecutionDataSamplerPluginType.class,
            ExecutionDataSamplerPlugin.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            LastRowsExecutionDataSampler.class.getName(),
            ExecutionDataSamplerPluginType.class,
            ExecutionDataSamplerPlugin.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            FileExecutionInfoLocation.class.getName(),
            ExecutionInfoLocationPluginType.class,
            ExecutionInfoLocationPlugin.class);

    PluginRegistry.getInstance()
        .registerPluginClass(
            FileDefinition.class.getName(), MetadataPluginType.class, HopMetadata.class);

    // Create a few items in the metadata...
    //
    metadataProvider = new MemoryMetadataProvider();

    // Data profile
    //
    ExecutionDataProfile dataProfile =
        new ExecutionDataProfile(
            NAME_DATA_PROFILE,
            null,
            Arrays.asList(
                new FirstRowsExecutionDataSampler("20"), new LastRowsExecutionDataSampler("20")));
    metadataProvider.getSerializer(ExecutionDataProfile.class).save(dataProfile);

    // Execution information location
    //
    FileExecutionInfoLocation fileLocation = new FileExecutionInfoLocation("/tmp/exec-info/");
    ExecutionInfoLocation location =
        new ExecutionInfoLocation(
            NAME_LOCATION, null, "5000", "10000", "100", dataProfile, fileLocation);
    metadataProvider.getSerializer(ExecutionInfoLocation.class).save(location);

    // Run config
    //
    BeamDirectPipelineRunConfiguration directRunConfiguration =
        new BeamDirectPipelineRunConfiguration("1");
    directRunConfiguration.setTempLocation(System.getProperty("java.io.tmpdir"));

    PipelineRunConfiguration runConfiguration =
        new PipelineRunConfiguration(
            NAME_RUN_CONFIG,
            "",
            NAME_LOCATION,
            Collections.emptyList(),
            directRunConfiguration,
            NAME_DATA_PROFILE,
            true);
    metadataProvider.getSerializer(PipelineRunConfiguration.class).save(runConfiguration);

    File inputFolder = new File("/tmp/customers/io");
    inputFolder.mkdirs();
    File outputFolder = new File("/tmp/customers/output");
    outputFolder.mkdirs();
    File tmpFolder = new File("/tmp/customers/tmp");
    tmpFolder.mkdirs();

    FileUtils.copyFile(
        new File("src/test/resources/customers/customers-100.txt"), new File(INPUT_CUSTOMERS_FILE));
    FileUtils.copyFile(
        new File("src/test/resources/customers/state-data.txt"), new File(INPUT_STATES_FILE));
  }

  @Disabled("This test needs to be reviewed")
  public void createRunPipeline(IVariables variables, PipelineMeta pipelineMeta) throws Exception {

    IPipelineEngine<PipelineMeta> hopPipeline =
        PipelineEngineFactory.createPipelineEngine(
            variables, NAME_RUN_CONFIG, metadataProvider, pipelineMeta);
    hopPipeline.execute();
    hopPipeline.waitUntilFinished();

    EngineMetrics engineMetrics = hopPipeline.getEngineMetrics();
    for (IEngineComponent component : engineMetrics.getComponents()) {
      System.out.println("Component: " + component.getName());
      writeMetric(engineMetrics, component, "read", Pipeline.METRIC_READ);
      writeMetric(engineMetrics, component, "written", Pipeline.METRIC_WRITTEN);
      writeMetric(engineMetrics, component, "input", Pipeline.METRIC_INPUT);
      writeMetric(engineMetrics, component, "output", Pipeline.METRIC_OUTPUT);
      writeMetric(engineMetrics, component, "errors", Pipeline.METRIC_ERROR);
    }
  }

  private void writeMetric(
      EngineMetrics engineMetrics, IEngineComponent component, String label, IEngineMetric metric) {
    Long value = engineMetrics.getComponentMetric(component, metric);
    if (value != null) {
      System.out.println("  - " + Const.rightPad(label, 8) + " : " + value);
    }
  }
}
