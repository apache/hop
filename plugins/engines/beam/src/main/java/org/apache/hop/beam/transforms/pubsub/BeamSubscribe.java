package org.apache.hop.beam.transforms.pubsub;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class BeamSubscribe extends BaseTransform<BeamSubscribeMeta, BeamSubscribeData> implements ITransform<BeamSubscribeMeta, BeamSubscribeData> {

  public BeamSubscribe( TransformMeta transformMeta, BeamSubscribeMeta meta, BeamSubscribeData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean processRow() throws HopException {

    // Outside of a Beam Runner this transform doesn't actually do anything, it's just metadata
    // This transform gets converted into Beam API calls in a pipeline
    //
    throw new HopException("The Beam Subscribe transform can only run in a Beam pipeline");
  }
}
