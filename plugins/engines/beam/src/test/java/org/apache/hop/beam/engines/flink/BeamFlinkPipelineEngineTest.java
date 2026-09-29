/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.engines.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.Arrays;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.hop.beam.engines.BeamBasePipelineEngineTest;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.junit.jupiter.api.Test;

class BeamFlinkPipelineEngineTest extends BeamBasePipelineEngineTest {

  @Test
  void testFlinkPipelineEngine() throws Exception {

    // [collection] avoids MiniCluster startup; BATCH_FORCED is the Flink default for batch jobs.
    BeamFlinkPipelineRunConfiguration configuration =
        new BeamFlinkPipelineRunConfiguration("[collection]", "1");
    configuration.setEnginePluginId("BeamFlinkPipelineEngine");
    configuration.setTempLocation(System.getProperty("java.io.tmpdir"));
    PipelineRunConfiguration pipelineRunConfiguration =
        new PipelineRunConfiguration(
            "flink",
            "description",
            "",
            Arrays.asList(new DescribedVariable("VAR1", "flink1", "description1")),
            configuration,
            null,
            false);
    // Save the metadata
    metadataProvider.getSerializer(PipelineRunConfiguration.class).save(pipelineRunConfiguration);

    PipelineMeta pipelineMeta =
        BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta(
            "input-process-output", "INPUT", "OUTPUT", metadataProvider);

    IPipelineEngine<PipelineMeta> engine =
        createAndExecutePipeline(
            pipelineRunConfiguration.getName(), metadataProvider, pipelineMeta);
    validateInputOutputEngineMetrics(engine);

    assertEquals("flink1", engine.getVariable("VAR1"));
    assertNull(((BeamFlinkPipelineEngine) engine).getFlinkJobId());
  }

  @Test
  void flinkJobIdIsFixedBeforeSubmission() throws Exception {
    BeamFlinkPipelineRunConfiguration configuration =
        new BeamFlinkPipelineRunConfiguration("127.0.0.1:9", "1");
    configuration.setEnginePluginId("BeamFlinkPipelineEngine");
    configuration.setTempLocation(System.getProperty("java.io.tmpdir"));
    PipelineRunConfiguration pipelineRunConfiguration =
        new PipelineRunConfiguration(
            "flink-job-id",
            "description",
            "",
            Arrays.asList(new DescribedVariable("VAR1", "flink1", "description1")),
            configuration,
            null,
            false);
    metadataProvider.getSerializer(PipelineRunConfiguration.class).save(pipelineRunConfiguration);

    PipelineMeta pipelineMeta =
        BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta(
            "flink-job-id", "INPUT", "OUTPUT", metadataProvider);
    IPipelineEngine<PipelineMeta> engine =
        PipelineEngineFactory.createPipelineEngine(
            variables, pipelineRunConfiguration.getName(), metadataProvider, pipelineMeta);
    engine.prepareExecution();

    Path confDir = null;
    try {
      BeamFlinkPipelineEngine flinkEngine = (BeamFlinkPipelineEngine) engine;
      String jobId = flinkEngine.getFlinkJobId();
      assertNotNull(jobId);
      assertEquals(jobId, JobID.fromHexString(jobId).toHexString());

      FlinkPipelineOptions options =
          flinkEngine.getBeamPipeline().getOptions().as(FlinkPipelineOptions.class);
      confDir = Path.of(options.getFlinkConfDir());
      assertEquals(
          jobId,
          GlobalConfiguration.loadConfiguration(confDir.toString())
              .get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));

      ExecutionState state = flinkEngine.capturePipelineExecutionState();
      assertEquals(jobId, state.getDetails().get(BeamFlinkPipelineEngine.DETAIL_FLINK_JOB_ID));
    } finally {
      FlinkJobConfiguration.delete(confDir);
    }
  }
}
