package org.apache.hop.arrow.transforms.arrowencode;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.ArrayList;
import java.util.List;

public class ArrowEncodeData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;

  public List<Integer> sourceFieldIndexes;

  public Schema arrowSchema;

  public List<FieldVector> fieldVectors = new ArrayList<>();
}
