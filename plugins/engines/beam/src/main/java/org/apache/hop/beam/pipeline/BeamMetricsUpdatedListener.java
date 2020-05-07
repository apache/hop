package org.apache.hop.beam.pipeline;

import org.apache.beam.sdk.PipelineResult;

public interface BeamMetricsUpdatedListener {

  void beamMetricsUpdated( PipelineResult pipelineResult );
}
