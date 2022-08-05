package org.apache.hop.arrow.transforms.arrowencode;

import org.apache.arrow.vector.*;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.ArrowBufferAllocator;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ArrowEncode extends BaseTransform<ArrowEncodeMeta, ArrowEncodeData> {

  private int batchSize = 10_000;

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
    if (first || data.count == batchSize) {
      first = false;
      // Initialize output row.
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.sourceFieldIndexes = new ArrayList<>();

      // Index the selected fields.
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

      // Initialize batch state tracking.
      //
      data.count = 0;
      data.batches = 0;

      // Initialize Arrow Vectors.
      //
      data.vectors = data.arrowSchema
              .getFields()
              .stream()
              .map(field -> field.createVector(ArrowBufferAllocator.rootAllocator()))
              .collect(Collectors.toList());
      data.vectors.forEach(ValueVector::allocateNew); // XXX is this required?
    }

    // Add Row to the current batch of Vectors
    if (row != null) {
      for (int index : data.sourceFieldIndexes) {
        Object value = row[index];
        ValueVector vector = data.vectors.get(index);

        // XXX The mess...
        // TODO: Arrow List support
        if (vector instanceof IntVector) {
          ((IntVector) vector).set(index, (int) value);
        } else if (vector instanceof BigIntVector) {
          ((BigIntVector) vector).set(index, (long) value);
        } else if (vector instanceof Float4Vector) {
          ((Float4Vector) vector).set(index, (float) value);
        } else if (vector instanceof Float8Vector) {
          ((Float8Vector) vector).set(index, (double) value);
        } else if (vector instanceof VarCharVector && value != null) {
          ((VarCharVector) vector).setSafe(index, ((String) value).getBytes(StandardCharsets.UTF_8));
        } else {
          throw new HopException(this + " - encountered unsupported vector type: " + vector.getClass());
        }
      }
      data.count++;
    }

    // Flush if we're at the limit.
    //
    if ((row == null && data.count > 0) || data.count == batchSize) {
      Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      outputRow[getInputRowMeta().size()] = data.vectors;
      data.vectors = List.of();
      putRow(data.outputRowMeta, outputRow);
    }

    if (row == null) {
      setOutputDone();
      return false;
    }
    return true;
  }
}
