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
 *
 */

package org.apache.hop.run;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/** The shutdown hook against the local pipeline and workflow engines, without mocks. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HopRunShutdownHookEngineTest {

  @BeforeAll
  static void beforeAll() throws Exception {
    HopEnvironment.init();
  }

  /** The Injector waits for rows that never come, so the pipeline only ends when it's stopped. */
  @Test
  void runningPipelineIsStopped() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("wait-for-rows");
    TransformMeta injector = new TransformMeta("Injector", new InjectorMeta());
    TransformMeta dummy = new TransformMeta("Dummy", new DummyMeta());
    pipelineMeta.addTransform(injector);
    pipelineMeta.addTransform(dummy);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(injector, dummy));

    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();
    pipeline.addRowProducer("Injector", 0);
    pipeline.startThreads();

    HopRunShutdownHook hook =
        new HopRunShutdownHook(new LogChannel("test"), Duration.ofSeconds(30), () -> {});
    hook.track(pipeline);
    assertFalse(pipeline.isFinished());

    CompletableFuture<Void> signal = CompletableFuture.runAsync(hook);
    pipeline.waitUntilFinished();
    hook.runFinished();
    signal.get(10, TimeUnit.SECONDS);

    assertTrue(pipeline.isStopped(), "The pipeline should be stopped");
    assertTrue(pipeline.isFinished(), "The pipeline should be finished");
  }

  /**
   * The signal arrives before the workflow starts, and the workflow clears its stopped flag when it
   * starts. The hook has to repeat the stop for the workflow to end.
   */
  @Test
  void workflowSignalledBeforeItStartsIsStopped() throws Exception {
    String filename =
        new File(getClass().getResource("/org/apache/hop/run/run-until-stopped.hwf").toURI())
            .getPath();
    WorkflowMeta workflowMeta =
        new WorkflowMeta(new Variables(), filename, new MemoryMetadataProvider());
    LocalWorkflowEngine workflow = new LocalWorkflowEngine(workflowMeta);

    HopRunShutdownHook hook =
        new HopRunShutdownHook(new LogChannel("test"), Duration.ofSeconds(30), () -> {});
    hook.track(workflow);

    CompletableFuture<Void> signal = CompletableFuture.runAsync(hook);
    Thread.sleep(500);
    workflow.startExecution();
    hook.runFinished();
    signal.get(10, TimeUnit.SECONDS);

    assertTrue(workflow.isStopped(), "The workflow should be stopped");
  }
}
