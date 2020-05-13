package org.apache.hop.pipeline.engines.local;

import org.apache.hop.pipeline.engine.PipelineEngineCapabilities;

public class LocalPipelineEngineCapabilities extends PipelineEngineCapabilities {

  /**
   * Locally we can do preview, debug, sniff testing, ...
   */
  public LocalPipelineEngineCapabilities() {
    super(true, true, true, true);
  }
}
