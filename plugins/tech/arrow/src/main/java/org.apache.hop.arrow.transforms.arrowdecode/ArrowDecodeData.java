package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaArrowVector;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class ArrowDecodeData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;

  public int inputIndex;

  public ValueMetaArrowVector arrowValueMeta;
}
