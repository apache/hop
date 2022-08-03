package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class ArrowDecode extends BaseTransform<ArrowDecodeMeta, ArrowDecodeData> {
  /**
   * Encode Arrow RecordBatch into Hop Rows.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta
   * @param data          the data object to store temporary data, database connections, caches, result sets,
   *                      hashtables etc.
   * @param copyNr        The copynumber for this transform.
   * @param pipelineMeta  The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline      The (running) pipeline to obtain information shared among the transforms.
   */
  public ArrowDecode(
    TransformMeta transformMeta,
    ArrowDecodeMeta meta,
    ArrowDecodeData data,
    int copyNr,
    PipelineMeta pipelineMeta,
    Pipeline pipeline
  ) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      meta.get

    }

    return true;
  }
}
