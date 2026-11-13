/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.workflow.actions.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.execution.lifecycle.ExecutionHandle;
import org.apache.hop.execution.lifecycle.ExecutionKind;
import org.apache.hop.execution.lifecycle.ExecutionLifecycle;
import org.apache.hop.execution.lifecycle.ExecutionObserver;
import org.apache.hop.execution.lifecycle.ExecutionState;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ActionPipelineAsyncLifecycleTest {
  @Transform(id = "AsyncActionProbe", name = "Async action probe")
  public static class ProbeMeta extends BaseTransformMeta<Probe, ProbeData> {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    boolean failure;
  }

  public static class ProbeData extends BaseTransformData {}

  public static class Probe extends BaseTransform<ProbeMeta, ProbeData> {
    public Probe(
        TransformMeta transform,
        ProbeMeta meta,
        ProbeData data,
        int copy,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transform, meta, data, copy, pipelineMeta, pipeline);
    }

    @Override
    public boolean processRow() throws HopException {
      assertSame(this, ExecutionLifecycle.current().getSubject());
      meta.entered.countDown();
      try {
        if (!meta.release.await(5, TimeUnit.SECONDS)) {
          throw new HopException("test release timeout");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HopException(e);
      }
      if (meta.failure) {
        throw new HopException("synthetic child failure");
      }
      setOutputDone();
      return false;
    }
  }

  @Test
  @Timeout(15)
  void realNoWaitPipelineContinuesPastActionEndIncludingFailedChild() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(ProbeMeta.class.getName(), TransformPluginType.class, Transform.class);
    for (final boolean fail : new boolean[] {false, true}) {
      final var probe = new ProbeMeta();
      probe.failure = fail;
      final var childMeta = new PipelineMeta();
      childMeta.setName("same-child-file");
      childMeta.addTransform(new TransformMeta("probe", probe));
      final var child = new LocalPipelineEngine(childMeta);
      final var parentMeta = new WorkflowMeta();
      parentMeta.setName("parent");
      final var parent = new LocalWorkflowEngine(parentMeta);
      parent.setLogLevel(LogLevel.NOTHING);
      final var action = spy(new ActionPipeline());
      action.setName("same-launcher");
      action.setParentWorkflow(parent);
      action.setWaitingToFinish(false);
      action.setRunConfiguration("local");
      doReturn(childMeta).when(action).getPipelineMeta(any(), any());
      final var seen = new AtomicReference<ExecutionHandle>();
      final var state = new AtomicReference<ExecutionState>();
      final var finishes = new AtomicInteger();
      final var finished = new CountDownLatch(1);
      try (final var registration =
              ExecutionLifecycle.addObserver(
                  new ExecutionObserver() {
                    @Override
                    public void started(ExecutionHandle handle) {
                      if (handle.getSubject() == child) {
                        seen.set(handle);
                      }
                    }

                    @Override
                    public void finished(
                        ExecutionHandle handle, ExecutionState terminal, Throwable failure) {
                      if (handle.getSubject() == child) {
                        state.set(terminal);
                        finishes.incrementAndGet();
                        finished.countDown();
                      }
                    }
                  });
          final var factory = mockStatic(PipelineEngineFactory.class)) {
        factory
            .when(
                () ->
                    PipelineEngineFactory.createPipelineEngine(
                        action, "local", action.getMetadataProvider(), childMeta))
            .thenReturn(child);
        ExecutionLifecycle.begin(parent, null, ExecutionKind.WORKFLOW, "parent", 0);
        final var launcher =
            ExecutionLifecycle.begin(action, parent, ExecutionKind.ACTION, "same-launcher", 0);
        try (final var scope = ExecutionLifecycle.activate(action)) {
          action.execute(new Result(), 0);
        }
        assertTrue(probe.entered.await(5, TimeUnit.SECONDS));
        assertSame(launcher, seen.get().getParent());
        ExecutionLifecycle.finish(action, ExecutionState.SUCCEEDED, null);
        ExecutionLifecycle.finish(parent, ExecutionState.SUCCEEDED, null);
        assertNull(state.get(), "No-wait launcher did not finish its blocked child");
        probe.release.countDown();
        child.waitUntilFinished();
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertEquals(fail ? ExecutionState.FAILED : ExecutionState.SUCCEEDED, state.get());
        assertEquals(1, finishes.get());
        assertNull(ExecutionLifecycle.current());
        assertNull(ExecutionLifecycle.get(child));
      } finally {
        probe.release.countDown();
      }
    }
  }
}
