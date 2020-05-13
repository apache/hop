package org.apache.hop.beam.engines.spark;

import org.apache.hop.beam.engines.BeamPipelineEngine;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

@PipelineEnginePlugin(
  id = "BeamSparkPipelineEngine",
  name = "Beam Spark pipeline engine",
  description = "This is an Apache Spark pipeline engine provided by the Apache Beam community"
)
public class BeamSparkPipelineEngine extends BeamPipelineEngine implements IPipelineEngine<PipelineMeta> {
  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    BeamSparkPipelineRunConfiguration runConfiguration = new BeamSparkPipelineRunConfiguration();
    runConfiguration.setUserAgent( "Hop" );
    return runConfiguration;
  }

  @Override public void validatePipelineRunConfigurationClass( IPipelineEngineRunConfiguration engineRunConfiguration ) throws HopException {
    if (!(engineRunConfiguration instanceof BeamSparkPipelineRunConfiguration )) {
      throw new HopException("A Beam Direct pipeline engine needs a direct run configuration, not of class "+engineRunConfiguration.getClass().getName());
    }
  }

}
