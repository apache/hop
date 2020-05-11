package org.apache.hop.beam.transform;

import org.junit.Test;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.pipeline.PipelineMeta;

public class BasePipelineTest extends PipelineTestBase {

  @Test
  public void testBasicPipeline() throws Exception {

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta(
      "io-dummy-output",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    createRunPipeline( pipelineMeta );
  }
}