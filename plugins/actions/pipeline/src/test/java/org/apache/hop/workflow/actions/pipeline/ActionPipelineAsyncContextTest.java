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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.execution.context.ExecutionContexts;
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

class ActionPipelineAsyncContextTest {
  private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

  @Transform(id = "AsyncActionProbe", name = "Async action probe")
  public static class ProbeMeta extends BaseTransformMeta<Probe, ProbeData> {
    private final AtomicReference<Thread> worker = new AtomicReference<>();
    private final AtomicReference<String> active = new AtomicReference<>();
    private final AtomicReference<String> logId = new AtomicReference<>();
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private boolean failure;
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
      meta.worker.set(Thread.currentThread());
      meta.active.set(CURRENT.get());
      meta.logId.set(getLogChannelId());
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
      final var activated = ConcurrentHashMap.<String>newKeySet();
      final var state = new AtomicReference<Boolean>();
      final var finishes = new AtomicInteger();
      final var mismatchedScopes = new AtomicInteger();
      final var failures = new AtomicInteger();
      child.addExecutionFailureListener(failure -> failures.incrementAndGet());
      final var finished = new CountDownLatch(1);
      child.addExecutionFinishedListener(
          engine -> {
            state.set(engine.getResult().getNrErrors() == 0);
            finishes.incrementAndGet();
            finished.countDown();
          });
      try (final var registration =
              ExecutionContexts.addListener(
                  logChannelId -> {
                    final var previous = CURRENT.get();
                    CURRENT.set(logChannelId);
                    activated.add(logChannelId);
                    return () -> {
                      if (!logChannelId.equals(CURRENT.get())) {
                        mismatchedScopes.incrementAndGet();
                      }
                      if (previous == null) {
                        CURRENT.remove();
                      } else {
                        CURRENT.set(previous);
                      }
                    };
                  });
          final var factory = mockStatic(PipelineEngineFactory.class)) {
        factory
            .when(
                () ->
                    PipelineEngineFactory.createPipelineEngine(
                        action, "local", action.getMetadataProvider(), childMeta))
            .thenReturn(child);
        try (final var scope = ExecutionContexts.activate(action.getLogChannelId())) {
          action.execute(new Result(), 0);
          assertEquals(action.getLogChannelId(), CURRENT.get());
        }
        assertNull(CURRENT.get());
        assertTrue(probe.entered.await(5, TimeUnit.SECONDS));
        assertEquals(probe.logId.get(), probe.active.get());
        assertEquals(
            action.getLogChannelId(),
            LoggingRegistry.getInstance()
                .getLoggingObject(child.getLogChannelId())
                .getParent()
                .getLogChannelId());
        assertEquals(
            child.getLogChannelId(),
            LoggingRegistry.getInstance()
                .getLoggingObject(probe.logId.get())
                .getParent()
                .getLogChannelId());
        assertTrue(activated.contains(child.getLogChannelId()));
        assertTrue(activated.contains(probe.logId.get()));
        assertNull(state.get(), "No-wait launcher did not finish its blocked child");
        probe.release.countDown();
        child.waitUntilFinished();
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        probe.worker.get().join(5000);
        assertFalse(probe.worker.get().isAlive(), "Pipeline worker must finish cleanup");
        assertEquals(fail, failures.get() > 0);
        assertEquals(!fail, state.get());
        assertEquals(1, finishes.get());
        assertEquals(0, mismatchedScopes.get());
        assertNull(CURRENT.get());
      } finally {
        probe.release.countDown();
        final var thread = probe.worker.get();
        if (thread != null) {
          thread.join(5000);
        }
      }
    }
  }
}
