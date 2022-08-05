package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaArrowVector;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

  public static int getStandardHopType(Field field) {
    ArrowType.ArrowTypeID typeId = field.getFieldType().getType().getTypeID();
    switch (typeId) {
      case Int:
        return IValueMeta.TYPE_INTEGER;
      case Utf8:
      case LargeUtf8:
        return IValueMeta.TYPE_STRING;
      case FloatingPoint:
        return IValueMeta.TYPE_NUMBER;
      default:
        // TODO: additional Arrow to Hop mappings
        return IValueMeta.TYPE_NONE;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    // Setup a schema?
    //
    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      String sourceFieldName = resolve(meta.getSourceFieldName());
      data.inputIndex = getInputRowMeta().indexOfValue(sourceFieldName);
      if (data.inputIndex < 0) {
        throw new HopException("Unable to find Arrow source field: " + sourceFieldName);
      }
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.inputIndex);
      if (!(valueMeta instanceof ValueMetaArrowVector)) {
        throw new HopException(
                "We can only decode Arrow data types and field "
                        + sourceFieldName
                        + " is of type "
                        + valueMeta.getTypeDesc());
      }
      data.arrowValueMeta = (ValueMetaArrowVector) valueMeta;
    }

    List<ValueVector> vectors = data.arrowValueMeta.getValueVectors(row[data.inputIndex]);

    if (vectors.isEmpty()) {
      throw new HopException("No vectors provided");
    }

    // Convert vectors to rows.
    //
    // TODO track vector rowcount in metadata?
    int rowCount = vectors.get(0).getValueCount();
    if (rowCount == 0) {
      // XXX bail out?
      return true;
    }

    // Build a mapping between the incoming vectors and the outgoing fields
    // XXX Assumes Schema order is in line with data order
    Schema schema = data.arrowValueMeta.getSchema();
    List<TargetField> targetFields = meta.getTargetFields();
    int[] vectorIndices = new int[targetFields.size()];

    for (int j = 0; j < vectorIndices.length; j++) {
      int index = -1;

      for (int n = 0; n < schema.getFields().size(); n++) {
        String name = schema.getFields().get(n).getName();
        if (name.equals(targetFields.get(j).getTargetFieldName())) {
          index = n;
          break;
        }
      }
      vectorIndices[j] = index;
    }

    for (int i = 0; i < rowCount; i++) {
      Object[] outputRow = convertToRow(i, row, vectors, vectorIndices);
      putRow(data.outputRowMeta, outputRow);
    }

    return true;
  }

  private Object[] convertToRow(int rowNum, Object[] inputRow, List<ValueVector> vectors, int[] indices) {
    Object[] outputRow = RowDataUtil.createResizedCopy(inputRow, data.outputRowMeta.size());

    // We overwrite the original Arrow object...
    //
    outputRow[data.inputIndex] = List.of(); // XXX use null?

    // ...and append new fields.
    //
    int rowIndex = getInputRowMeta().size();
    for (int index : indices) {
      ValueVector vector = vectors.get(index);
      outputRow[rowIndex++] = vector.getObject(rowNum);
    }

    return outputRow;
  }
}
