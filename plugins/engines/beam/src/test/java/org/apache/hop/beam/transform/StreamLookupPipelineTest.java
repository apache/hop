package org.apache.hop.beam.transform;

import org.junit.Test;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.pipeline.PipelineMeta;

public class StreamLookupPipelineTest extends PipelineTestBase {

  @Test
  public void testStreamLookupPipeline() throws Exception {

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateStreamLookupPipelineMeta(
      "io-stream-lookup-output",
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