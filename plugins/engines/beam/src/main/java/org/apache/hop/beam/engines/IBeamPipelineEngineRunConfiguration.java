package org.apache.hop.beam.engines;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hop.beam.metadata.RunnerType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;

public interface IBeamPipelineEngineRunConfiguration extends IPipelineEngineRunConfiguration {

  RunnerType getRunnerType();
  PipelineOptions getPipelineOptions() throws HopException;
  boolean isRunningAsynchronous();

  String getUserAgent();
  String getTempLocation();
  String getPluginsToStage();
  String getTransformPluginClasses();
  String getXpPluginClasses();
  String getStreamingHopTransformsFlushInterval();
  String getStreamingHopTransformsBufferSize();
  String getFatJar();

}
