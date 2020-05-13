package org.apache.hop.beam.engines.dataflow;

import org.apache.hop.beam.engines.BeamPipelineEngine;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

@PipelineEnginePlugin(
  id = "BeamDataFlowPipelineEngine",
  name = "Beam DataFlow pipeline engine",
  description = "This allows you to run your pipeline on Google Cloud Platform DataFlow, provided by the Apache Beam community"
)
public class BeamDataFlowPipelineEngine extends BeamPipelineEngine implements IPipelineEngine<PipelineMeta> {
  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    BeamDataFlowPipelineRunConfiguration runConfiguration = new BeamDataFlowPipelineRunConfiguration();
    runConfiguration.setUserAgent( "Hop" );
    return runConfiguration;
  }

  @Override public void validatePipelineRunConfigurationClass( IPipelineEngineRunConfiguration engineRunConfiguration ) throws HopException {
    if (!(engineRunConfiguration instanceof BeamDataFlowPipelineRunConfiguration )) {
      throw new HopException("A Beam Direct pipeline engine needs a direct run configuration, not of class "+engineRunConfiguration.getClass().getName());
    }
  }

}
