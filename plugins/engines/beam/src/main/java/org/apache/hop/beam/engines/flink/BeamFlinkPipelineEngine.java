package org.apache.hop.beam.engines.flink;

import org.apache.hop.beam.engines.BeamPipelineEngine;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

@PipelineEnginePlugin(
  id = "BeamFlinkPipelineEngine",
  name = "Beam Flink pipeline engine",
  description = "This is a Flink pipeline engine provided by the Apache Beam community"
)
public class BeamFlinkPipelineEngine extends BeamPipelineEngine implements IPipelineEngine<PipelineMeta> {
  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    BeamFlinkPipelineRunConfiguration runConfiguration = new BeamFlinkPipelineRunConfiguration();
    runConfiguration.setUserAgent( "Hop" );
    return runConfiguration;
  }

  @Override public void validatePipelineRunConfigurationClass( IPipelineEngineRunConfiguration engineRunConfiguration ) throws HopException {
    if (!(engineRunConfiguration instanceof BeamFlinkPipelineRunConfiguration )) {
      throw new HopException("A Beam Direct pipeline engine needs a direct run configuration, not of class "+engineRunConfiguration.getClass().getName());
    }
  }

}
