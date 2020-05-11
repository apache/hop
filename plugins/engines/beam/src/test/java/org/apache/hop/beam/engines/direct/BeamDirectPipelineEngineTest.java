package org.apache.hop.beam.engines.direct;

import org.apache.hop.beam.engines.BeamBasePipelineEngineTest;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.variables.VariableValueDescription;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.Test;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;

public class BeamDirectPipelineEngineTest extends BeamBasePipelineEngineTest {

  @Test
  public void testDirectPipelineEngine() throws Exception {

    IPipelineEngineRunConfiguration configuration = new BeamDirectPipelineRunConfiguration();
    configuration.setEnginePluginId( "BeamDirectPipelineEngine" );
    PipelineRunConfiguration pipelineRunConfiguration = new PipelineRunConfiguration("direct", "description",
      Arrays.asList(new VariableValueDescription("VAR1", "value1", "description1")),
      configuration
    );
    PipelineRunConfiguration.createFactory( metaStore ).saveElement( pipelineRunConfiguration );

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta( "input-process-output", "INPUT", "OUTPUT", metaStore );

    IPipelineEngine<PipelineMeta> engine = createAndExecutePipeline(pipelineRunConfiguration.getName(), metaStore, pipelineMeta);
    validateInputOutputEngineMetrics( engine );

    assertEquals("value1", engine.getVariable( "VAR1" ));
  }

}