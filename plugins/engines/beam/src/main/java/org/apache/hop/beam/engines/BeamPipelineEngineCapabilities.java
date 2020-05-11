package org.apache.hop.beam.engines;

import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;

public class BeamPipelineEngineCapabilities extends PipelineEngineCapabilities {

  /**
   * This engine doesn't support preview or debug
   * However it does allow you to sniff test transform output remotely
   */
  public BeamPipelineEngineCapabilities() {
    super(false, false, false, false);
  }
}
