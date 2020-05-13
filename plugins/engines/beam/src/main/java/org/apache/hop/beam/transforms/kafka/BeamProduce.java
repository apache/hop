package org.apache.hop.beam.transforms.kafka;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;

public class BeamProduce extends BaseTransform implements ITransform {

  public BeamProduce( TransformMeta transformMeta, BeamProduceMeta meta, DummyData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean processRow() throws HopException {

    // Outside of a Beam Runner this transform doesn't actually do anything, it's just metadata
    // This transform gets converted into Beam API calls in a pipeline
    //
    return false;
  }
}
