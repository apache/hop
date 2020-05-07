package org.apache.hop.beam.transform;

import org.junit.Test;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.pipeline.PipelineMeta;

public class MergeJoinPipelineTest extends PipelineTestBase {

  @Test
  public void testMergeJoinPipeline() throws Exception {

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateMergeJoinPipelineMeta(
      "inputs-merge-join-output",
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