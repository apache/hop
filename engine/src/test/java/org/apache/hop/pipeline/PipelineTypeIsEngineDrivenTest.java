/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * How the transforms of a pipeline are driven - every transform in its own thread, or all of them
 * one iteration at a time - is decided by the engine the pipeline is run with. It is not a property
 * of the pipeline and is therefore never written to the .hpl file.
 *
 * <p>It used to be one: engines wrote their choice into the {@link PipelineMeta} they were handed,
 * which in Hop GUI is the very object the editor holds. A single run with the single threaded
 * engine left {@code <pipeline_type>SingleThreaded</pipeline_type>} in the file on the next save,
 * nothing ever set it back, and the local engine then started no threads at all for it - issue
 * #8262, where the pipeline hung after switching the run configuration back.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class PipelineTypeIsEngineDrivenTest {

  /** A pipeline as saved by a Hop version that still wrote the execution model into the file. */
  private static final String PIPELINE_WITH_LEGACY_TYPE =
      """
      <pipeline>
        <info>
          <name>legacy-pipeline-type</name>
          <pipeline_type>SingleThreaded</pipeline_type>
        </info>
      </pipeline>
      """;

  private final IVariables variables = new Variables();
  private final IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

  /** An engine that drives its transforms from a single thread, like the LocalSingle engine. */
  private static class SingleThreadedTestEngine extends LocalPipelineEngine {
    SingleThreadedTestEngine(PipelineMeta pipelineMeta) {
      super(pipelineMeta, new Variables(), new LoggingObject("test"));
    }

    @Override
    public PipelineMeta.PipelineType getPipelineType() {
      return PipelineMeta.PipelineType.SingleThreaded;
    }
  }

  @Test
  void executionModelIsNotWrittenToTheFile() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("no-pipeline-type");

    assertFalse(pipelineMeta.getXml(variables).contains("pipeline_type"));
  }

  @Test
  void legacyExecutionModelInTheFileIsIgnored() throws Exception {
    PipelineMeta pipelineMeta =
        new PipelineMeta(
            new ByteArrayInputStream(PIPELINE_WITH_LEGACY_TYPE.getBytes(StandardCharsets.UTF_8)),
            metadataProvider,
            variables);

    // The file still loads, ...
    assertEquals("legacy-pipeline-type", pipelineMeta.getName());
    // ... the stale element no longer makes the local engine skip starting its threads, ...
    assertEquals(
        PipelineMeta.PipelineType.Normal,
        new LocalPipelineEngine(pipelineMeta).getPipelineType(),
        "a pipeline_type left in the file by an older release must not affect the engine");
    // ... and saving the pipeline again drops it.
    assertFalse(pipelineMeta.getXml(variables).contains("pipeline_type"));
  }

  @Test
  void eachEngineCarriesItsOwnExecutionModel() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("shared-metadata");

    // Hop GUI hands the same PipelineMeta instance to every engine it creates for the tab.
    LocalPipelineEngine local = new LocalPipelineEngine(pipelineMeta);
    SingleThreadedTestEngine singleThreaded = new SingleThreadedTestEngine(pipelineMeta);

    assertEquals(PipelineMeta.PipelineType.SingleThreaded, singleThreaded.getPipelineType());
    assertEquals(
        PipelineMeta.PipelineType.Normal,
        local.getPipelineType(),
        "one engine's execution model must not leak into another engine over the same pipeline");
  }

  @Test
  void anEngineCanBeDrivenSingleThreadedWithoutTouchingTheMetadata() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("embedded-sub-pipeline");

    // Transforms that embed a sub-pipeline (Simple Mapping, Kafka Consumer, the Beam and Spark
    // workers) push rows through it one batch at a time and say so on the engine they create.
    LocalPipelineEngine subPipeline = new LocalPipelineEngine(pipelineMeta);
    subPipeline.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);

    assertEquals(PipelineMeta.PipelineType.SingleThreaded, subPipeline.getPipelineType());
    assertFalse(pipelineMeta.getXml(variables).contains("pipeline_type"));
  }
}
