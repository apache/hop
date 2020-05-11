package org.apache.hop.beam.engines.direct;

import org.apache.hop.beam.engines.BeamPipelineEngine;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

@PipelineEnginePlugin(
  id = "BeamDirectPipelineEngine",
  name = "Beam Direct pipeline engine",
  description = "This is a local pipeline engine provided by the Apache Beam community as a way of testing pipelines"
)
public class BeamDirectPipelineEngine extends BeamPipelineEngine implements IPipelineEngine<PipelineMeta> {

  public BeamDirectPipelineEngine() {
  }

  public BeamDirectPipelineEngine( PipelineMeta pipelineMeta ) {
    super( pipelineMeta );
  }

  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    BeamDirectPipelineRunConfiguration runConfiguration = new BeamDirectPipelineRunConfiguration();
    runConfiguration.setUserAgent( "Hop" );
    return runConfiguration;
  }

  @Override public void validatePipelineRunConfigurationClass( IPipelineEngineRunConfiguration engineRunConfiguration ) throws HopException {
    if (!(engineRunConfiguration instanceof BeamDirectPipelineRunConfiguration)) {
      throw new HopException("A Beam Direct pipeline engine needs a direct run configuration, not of class "+engineRunConfiguration.getClass().getName());
    }
  }

}
