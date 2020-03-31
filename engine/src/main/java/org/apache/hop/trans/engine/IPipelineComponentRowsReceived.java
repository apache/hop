package org.apache.hop.trans.engine;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.trans.TransMeta;

public interface IPipelineComponentRowsReceived {

  /**
   * When all rows are received for a specific task (sniff, preview, debug, ...) this method will get called.
   *
   * @param pipelineEngine
   * @param rowBuffer
   * @throws HopException
   */
  void rowsReceived( IPipelineEngine<TransMeta> pipelineEngine, RowBuffer rowBuffer) throws HopException;
}
