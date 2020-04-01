package org.apache.hop.pipeline.engine;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.pipeline.PipelineMeta;

public interface IPipelineComponentRowsReceived {

  /**
   * When all rows are received for a specific task (sniff, preview, debug, ...) this method will get called.
   *
   * @param pipelineEngine
   * @param rowBuffer
   * @throws HopException
   */
  void rowsReceived( IPipelineEngine<PipelineMeta> pipelineEngine, RowBuffer rowBuffer) throws HopException;
}
