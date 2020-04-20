package org.apache.hop.pipeline.engines.remote;

import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;

public class RemotePipelineEngineCapabilities extends PipelineEngineCapabilities {

  /**
   * This engine doesn't support preview or debug
   * However it does allow you to sniff test transform output remotely
   */
  public RemotePipelineEngineCapabilities() {
    super(false, false, true);
  }
}
