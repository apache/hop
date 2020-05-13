package org.apache.hop.beam.xp;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;

public class BeamDummy extends Dummy implements ITransform<DummyMeta, DummyData> {

  protected boolean init;

  public BeamDummy( TransformMeta transformMeta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, new DummyMeta(), data, copyNr, pipelineMeta, pipeline );
  }

  public void setInit( boolean init ) {
    this.init = init;
  }

  @Override public boolean isInitialising() {
    return init;
  }
}