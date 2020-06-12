package org.apache.hop.beam.transform;

import org.junit.Test;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.pipeline.PipelineMeta;

public class SwitchCasePipelineTest extends org.apache.hop.beam.transform.PipelineTestBase {

  @Test
  public void testSwitchCasePipeline() throws Exception {

    PipelineMeta pipelineMeta = BeamPipelineMetaUtil.generateSwitchCasePipelineMeta(
      "io-switch-case-output",
      "INPUT",
      "OUTPUT",
      metadataProvider
    );

    try {
      createRunPipeline( pipelineMeta );
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

}