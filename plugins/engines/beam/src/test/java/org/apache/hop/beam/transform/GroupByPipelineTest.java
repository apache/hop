package org.apache.hop.beam.transform;

import org.junit.Test;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.pipeline.PipelineMeta;

public class GroupByPipelineTest extends org.apache.hop.beam.transform.PipelineTestBase {

  @Test
  public void testGroupByPipeline() throws Exception {

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateBeamGroupByPipelineMeta(
      "io-group-output",
      "INPUT",
      "OUTPUT",
      metaStore
    );

    try {
      createRunPipeline( pipelineMeta );
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}