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

package org.apache.hop.beam.transform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineRunConfiguration;
import org.apache.hop.beam.engines.flink.BeamFlinkPipelineEngine;
import org.apache.hop.beam.engines.spark.BeamSparkPipelineEngine;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
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
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.pipeline.transforms.constant.ConstantMeta;
import org.apache.hop.pipeline.transforms.filterrows.FilterRowsMeta;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta;
import org.apache.hop.pipeline.transforms.mergejoin.MergeJoinMeta;
import org.apache.hop.pipeline.transforms.streamlookup.StreamLookupMeta;
import org.apache.hop.pipeline.transforms.switchcase.SwitchCaseMeta;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PipelineTestBase {

  public static final String INPUT_CUSTOMERS_FILE = System.getProperty( "java.io.tmpdir" ) + "/customers/io/customers-100.txt";
  public static final String INPUT_STATES_FILE = System.getProperty( "java.io.tmpdir" ) + "/customers/io/state-data.txt";

  protected IVariables variables;
  protected IHopMetadataProvider metadataProvider;

  @Before
  public void setUp() throws Exception {

    variables = Variables.getADefaultVariableSpace();

    List<String> beamTransforms = Arrays.asList(
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
      SwitchCaseMeta.class.getName()
    );

    BeamHop.init( beamTransforms, new ArrayList<>() );

    PluginRegistry.getInstance().registerPluginClass( BeamDirectPipelineEngine.class.getName(), PipelineEnginePluginType.class, PipelineEnginePlugin.class );
    PluginRegistry.getInstance().registerPluginClass( BeamFlinkPipelineEngine.class.getName(), PipelineEnginePluginType.class, PipelineEnginePlugin.class );
    PluginRegistry.getInstance().registerPluginClass( BeamSparkPipelineEngine.class.getName(), PipelineEnginePluginType.class, PipelineEnginePlugin.class );

    PluginRegistry.getInstance().registerPluginClass( FileDefinition.class.getName(), MetadataPluginType.class, HopMetadata.class );

    metadataProvider = new MemoryMetadataProvider();

    File inputFolder = new File( "/tmp/customers/io" );
    inputFolder.mkdirs();
    File outputFolder = new File( "/tmp/customers/output" );
    outputFolder.mkdirs();
    File tmpFolder = new File( "/tmp/customers/tmp" );
    tmpFolder.mkdirs();

    FileUtils.copyFile( new File( "src/test/resources/customers/customers-100.txt" ), new File( INPUT_CUSTOMERS_FILE ));
    FileUtils.copyFile( new File( "src/test/resources/customers/state-data.txt" ), new File( INPUT_STATES_FILE ));
  }


  @Ignore
  public void createRunPipeline( IVariables variables, PipelineMeta pipelineMeta ) throws Exception {

    /*
    FileOutputStream fos = new FileOutputStream( "/tmp/"+pipelineMeta.getName()+".hpl" );
    fos.write( pipelineMeta.getXml().getBytes() );
    fos.close();
    */

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    pipelineOptions.setJobName( pipelineMeta.getName() );
    pipelineOptions.setUserAgent( BeamConst.STRING_HOP_BEAM );

    BeamDirectPipelineRunConfiguration beamRunConfig = new BeamDirectPipelineRunConfiguration();
    beamRunConfig.setTempLocation( System.getProperty( "java.io.tmpdir" ) );

    // No extra plugins to load : null option
    HopPipelineMetaToBeamPipelineConverter converter = new HopPipelineMetaToBeamPipelineConverter( variables, pipelineMeta, metadataProvider, beamRunConfig );
    Pipeline pipeline = converter.createPipeline();

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();

    MetricResults metricResults = pipelineResult.metrics();

    MetricQueryResults allResults = metricResults.queryMetrics( MetricsFilter.builder().build() );
    for ( MetricResult<Long> result : allResults.getCounters() ) {
      System.out.println( "Name: " + result.getName() + " Attempted: " + result.getAttempted() );
    }
  }
}
