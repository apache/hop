package org.apache.hop.beam.transform;

import org.junit.Test;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.pipeline.PipelineMeta;

public class FilterPipelineTest extends org.apache.hop.beam.transform.PipelineTestBase {

  @Test
  public void testFilterRowsPipeline() throws Exception {

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateFilterRowsPipelineMeta(
      "io-filter-rows-output",
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