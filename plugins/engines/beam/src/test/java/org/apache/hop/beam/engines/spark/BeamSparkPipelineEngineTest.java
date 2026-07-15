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

package org.apache.hop.beam.engines.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.apache.hop.beam.engines.BeamBasePipelineEngineTest;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.Test;

/**
 * Embedded Spark local-mode smoke test for the Beam Spark pipeline engine.
 *
 * <p>Requires spark-core on the test classpath (see hop-engines-beam pom). Hop runs on Java 21;
 * Spark 3.5.x is used for local mode. Cluster multi-version coverage lives in {@code
 * integration-tests/spark} / {@code run-spark-matrix.sh}.
 */
class BeamSparkPipelineEngineTest extends BeamBasePipelineEngineTest {

  @Test
  void testSparkPipelineEngine() throws Exception {
    // Spark UI pulls in a shaded Jetty + jersey stack that conflicts with Hop's servlet APIs
    // on the unit-test classpath. The pipeline does not need the UI.
    System.setProperty("spark.ui.enabled", "false");
    System.setProperty("spark.driver.host", "127.0.0.1");
    System.setProperty("spark.driver.bindAddress", "127.0.0.1");

    BeamSparkPipelineRunConfiguration configuration = new BeamSparkPipelineRunConfiguration();
    configuration.setSparkMaster("local[2]");
    configuration.setTempLocation(System.getProperty("java.io.tmpdir"));
    configuration.setEnginePluginId("BeamSparkPipelineEngine");
    PipelineRunConfiguration pipelineRunConfiguration =
        new PipelineRunConfiguration(
            "spark",
            "description",
            "",
            Arrays.asList(new DescribedVariable("VAR1", "spark1", "description1")),
            configuration,
            null,
            false);

    metadataProvider.getSerializer(PipelineRunConfiguration.class).save(pipelineRunConfiguration);

    PipelineMeta pipelineMeta =
        BeamPipelineMetaUtil.generateBeamInputOutputPipelineMeta(
            "input-process-output", "INPUT", "OUTPUT", metadataProvider);

    IPipelineEngine<PipelineMeta> engine =
        createAndExecutePipeline(
            pipelineRunConfiguration.getName(), metadataProvider, pipelineMeta);
    validateInputOutputEngineMetrics(engine);

    assertEquals("spark1", engine.getVariable("VAR1"));
  }
}
