package org.apache.hop.trans.engine;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.trans.TransMeta;

public interface IFinishedListener {

  /**
   * When all processing has completed for the engine.
   *
   * @param pipelineEngine
   * @throws HopException
   */
  void finished( IPipelineEngine<TransMeta> pipelineEngine) throws HopException;
}
