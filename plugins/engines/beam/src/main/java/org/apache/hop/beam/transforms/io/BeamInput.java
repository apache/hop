package org.apache.hop.beam.transforms.io;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class BeamInput extends BaseTransform<BeamInputMeta, BeamInputData> implements ITransform<BeamInputMeta, BeamInputData> {

  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this class to implement your own
   * transforms.
   *
   * @param transformMeta          The TransformMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this transform.
   * @param pipelineMeta         The TransInfo of which the transform transformMeta is part of.
   * @param pipeline             The (running) transformation to obtain information shared among the transforms.
   */
  public BeamInput( TransformMeta transformMeta, BeamInputMeta meta, BeamInputData data, int copyNr, PipelineMeta pipelineMeta,
                    Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean processRow() throws HopException {


    // Outside of Beam this transform doesn't actually do anything, it's just metadata
    // This transform gets converted into Beam API calls in a pipeline
    //
    return false;
  }
}
