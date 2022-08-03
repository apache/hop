package org.apache.hop.arrow.transforms.arrowencode;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;

public class ArrowEncode extends BaseTransform<ArrowEncodeMeta, ArrowEncodeData> {

  private int batchSize = 1_000;

  /**
   * Encode Hop Rows into an Arrow RecordBatch of Arrow Vectors.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta
   * @param data          the data object to store temporary data, database connections, caches, result sets,
   *                      hashtables etc.
   * @param copyNr        The copynumber for this transform.
   * @param pipelineMeta  The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline      The (running) pipeline to obtain information shared among the transforms.
   */
  public ArrowEncode(
      TransformMeta transformMeta,
      ArrowEncodeMeta meta,
      ArrowEncodeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();

    // Either we're operating on our first row or the start of a new batch.
    //
    if (first) {
      if (row == null) {
        setOutputDone();
        return false;
      }

      // Initialize output row.
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.sourceFieldIndexes = new ArrayList<>();

      // Index the selected fields..
      //
      for (SourceField field : meta.getSourceFields()) {
        int index = getInputRowMeta().indexOfValue(field.getSourceFieldName());
        if (index < 0) {
          throw new HopException("Unable to find input field " + field.getSourceFieldName());
        }
        data.sourceFieldIndexes.add(index);
      }

      // Build the Arrow schema.
      //
      data.arrowSchema = meta.createArrowSchema(getInputRowMeta(), meta.getSourceFields());
      if (log.isDetailed()) {
        log.logDetailed("Schema: " + data.arrowSchema);
      }
    }



    return true;
  }
}
