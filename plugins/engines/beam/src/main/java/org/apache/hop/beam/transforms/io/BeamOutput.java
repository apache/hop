package org.apache.hop.beam.transforms.io;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class BeamOutput extends BaseTransform<BeamOutputMeta, BeamOutputData> implements ITransform<BeamOutputMeta, BeamOutputData> {

  public BeamOutput( TransformMeta transformMeta, BeamOutputMeta meta, BeamOutputData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row==null) {
      setOutputDone();
      return false;
    }
    putRow(getInputRowMeta(), row);
    return true;
  }
}
