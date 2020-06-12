package org.apache.hop.beam.engines.spark;

import org.apache.hop.beam.engines.BeamBasePipelineEngineTest;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.variables.VariableValueDescription;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BeamSparkPipelineEngineTest extends BeamBasePipelineEngineTest {

  @Test
  @Ignore
  public void testSparkPipelineEngine() throws Exception {

    BeamSparkPipelineRunConfiguration configuration = new BeamSparkPipelineRunConfiguration();
    configuration.setSparkMaster( "local" );
    configuration.setEnginePluginId( "BeamSparkPipelineEngine" );
    PipelineRunConfiguration pipelineRunConfiguration = new PipelineRunConfiguration( "spark", "description",
      Arrays.asList( new VariableValueDescription( "VAR1", "spark1", "description1" ) ),
      configuration
    );

    metadataProvider.getSerializer( PipelineRunConfiguration.class ).save( pipelineRunConfiguration );

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta( "input-process-output", "INPUT", "OUTPUT", metadataProvider );

    IPipelineEngine<PipelineMeta> engine = createAndExecutePipeline( pipelineRunConfiguration.getName(), metadataProvider, pipelineMeta );
    validateInputOutputEngineMetrics( engine );

    assertEquals( "spark1", engine.getVariable( "VAR1" ) );
  }

}