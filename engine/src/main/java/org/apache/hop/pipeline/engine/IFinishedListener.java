package org.apache.hop.pipeline.engine;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;

public interface IFinishedListener {

  /**
   * When all processing has completed for the engine.
   *
   * @param pipelineEngine
   * @throws HopException
   */
  void finished( IPipelineEngine<PipelineMeta> pipelineEngine) throws HopException;
}
