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

package org.apache.hop.reflection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.reflection.pipeline.meta.PipelineLog;
import org.apache.hop.reflection.workflow.meta.WorkflowLog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

class RunMetadataTest {

  /** Counts how often metadata is asked for. */
  private static final class CountingMetadataProvider extends MemoryMetadataProvider {
    final AtomicInteger reads = new AtomicInteger();

    @Override
    public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer(Class<T> managedClass)
        throws HopException {
      reads.incrementAndGet();
      return super.getSerializer(managedClass);
    }
  }

  @SuppressWarnings("unchecked")
  private static IPipelineEngine<PipelineMeta> pipeline(
      IPipelineEngine<PipelineMeta> parentPipeline, IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getParentPipeline()).thenReturn(parentPipeline);
    when(pipeline.getParentWorkflow()).thenReturn(parentWorkflow);
    return pipeline;
  }

  @SuppressWarnings("unchecked")
  private static IWorkflowEngine<WorkflowMeta> workflow(
      IWorkflowEngine<WorkflowMeta> parentWorkflow, IPipelineEngine<PipelineMeta> parentPipeline) {
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getParentWorkflow()).thenReturn(parentWorkflow);
    when(workflow.getParentPipeline()).thenReturn(parentPipeline);
    return workflow;
  }

  @Test
  void theRunIsTheTopLevelPipelineOrWorkflow() {
    IWorkflowEngine<WorkflowMeta> top = workflow(null, null);
    IPipelineEngine<PipelineMeta> inWorkflow = pipeline(null, top);
    IPipelineEngine<PipelineMeta> child = pipeline(inWorkflow, null);
    IWorkflowEngine<WorkflowMeta> workflowInPipeline = workflow(null, child);
    IPipelineEngine<PipelineMeta> deepest = pipeline(null, workflowInPipeline);

    assertSame(top, RunMetadata.root(top));
    assertSame(top, RunMetadata.root(child));
    assertSame(top, RunMetadata.root(deepest));

    IPipelineEngine<PipelineMeta> alone = pipeline(null, null);
    assertSame(alone, RunMetadata.root(alone));
  }

  @Test
  void metadataIsReadOncePerRunAndType() throws Exception {
    CountingMetadataProvider provider = new CountingMetadataProvider();
    provider.getSerializer(PipelineLog.class).save(new PipelineLog("first"));
    provider.reads.set(0);

    IPipelineEngine<PipelineMeta> top = pipeline(null, null);
    IPipelineEngine<PipelineMeta> childA = pipeline(top, null);
    IPipelineEngine<PipelineMeta> childB = pipeline(top, null);

    List<PipelineLog> forTop = RunMetadata.loadAll(top, provider, PipelineLog.class);
    List<PipelineLog> forA = RunMetadata.loadAll(childA, provider, PipelineLog.class);
    List<PipelineLog> forB = RunMetadata.loadAll(childB, provider, PipelineLog.class);

    assertEquals(1, provider.reads.get());
    assertEquals(1, forTop.size());
    assertEquals("first", forTop.get(0).getName());
    assertSame(forTop, forA);
    assertSame(forTop, forB);

    // another type is read on its own, once
    assertEquals(0, RunMetadata.loadAll(childA, provider, WorkflowLog.class).size());
    RunMetadata.loadAll(childB, provider, WorkflowLog.class);
    assertEquals(2, provider.reads.get());
  }

  @Test
  void aChangeIsSeenByTheNextRunNotByTheOneGoingOn() throws Exception {
    CountingMetadataProvider provider = new CountingMetadataProvider();
    provider.getSerializer(PipelineLog.class).save(new PipelineLog("first"));

    IPipelineEngine<PipelineMeta> run = pipeline(null, null);
    assertEquals(1, RunMetadata.loadAll(run, provider, PipelineLog.class).size());

    provider.getSerializer(PipelineLog.class).save(new PipelineLog("second"));

    // the run that is going on keeps what it read when it started
    assertEquals(1, RunMetadata.loadAll(pipeline(run, null), provider, PipelineLog.class).size());
    // a new run reads again
    IPipelineEngine<PipelineMeta> nextRun = pipeline(null, null);
    assertEquals(2, RunMetadata.loadAll(nextRun, provider, PipelineLog.class).size());
  }
}
