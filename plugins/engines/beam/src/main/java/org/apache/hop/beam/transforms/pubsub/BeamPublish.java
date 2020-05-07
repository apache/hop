package org.apache.hop.beam.transforms.pubsub;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class BeamPublish extends BaseTransform<BeamPublishMeta, BeamPublishData> implements ITransform<BeamPublishMeta, BeamPublishData> {

  public BeamPublish( TransformMeta transformMeta, BeamPublishMeta meta, BeamPublishData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean processRow() throws HopException {

    // Outside of a Beam Runner this transform doesn't actually do anything, it's just metadata
    // This transform gets converted into Beam API calls in a pipeline
    //
    throw new HopException("The Beam Publish transform can only run in a Beam pipeline");
  }
}
