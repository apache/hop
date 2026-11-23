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
package org.apache.hop.workflow.actions.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.execution.context.ExecutionContexts;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ActionWorkflowAsyncContextTest {
  private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

  @BeforeAll
  static void initializeEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Test
  // Allow cold Mockito instrumentation on busy CI hosts; handshakes remain bounded below.
  @Timeout(60)
  void realNoWaitChildStartsAfterLauncherReturnsIncludingFailedChild() throws Exception {
    for (final boolean fail : new boolean[] {false, true}) {
      final var entered = new CountDownLatch(1);
      final var release = new CountDownLatch(1);
      final var finished = new CountDownLatch(1);
      final var actionActive = new AtomicReference<String>();
      final var actionLogId = new AtomicReference<String>();
      final var childMeta = new WorkflowMeta();
      childMeta.setName("same-file");
      childMeta.addAction(
          new ActionMeta(
              new ActionStart("start") {
                @Override
                public Result execute(final Result previousResult, final int nr) {
                  actionActive.set(CURRENT.get());
                  actionLogId.set(getLogChannelId());
                  previousResult.setNrErrors(fail ? 1 : 0);
                  previousResult.setResult(!fail);
                  return previousResult;
                }
              }));
      final var parentMeta = new WorkflowMeta();
      parentMeta.setName("parent");
      final var parent = new LocalWorkflowEngine(parentMeta);
      parent.setLogLevel(LogLevel.NOTHING);
      final var action = spy(new ActionWorkflow());
      action.setName("same-launcher");
      action.setParentWorkflow(parent);
      action.setWaitingToFinish(false);
      action.setRunConfiguration("local");
      doReturn(childMeta).when(action).getWorkflowMeta(any(), any());
      final var worker = new AtomicReference<Thread>();
      final var restored = new AtomicReference<String>();
      // Match the real factory's logging-parent constructor even though engine selection is mocked.
      final var child =
          new LocalWorkflowEngine(childMeta, action) {
            @Override
            public Result startExecution() {
              worker.set(Thread.currentThread());
              CURRENT.set("worker-before");
              entered.countDown();
              try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                  throw new IllegalStateException("test child release timeout");
                }
                return super.startExecution();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              } finally {
                restored.set(CURRENT.get());
                CURRENT.remove();
              }
            }
          };
      final var activated = ConcurrentHashMap.<String>newKeySet();
      final var childActive = new AtomicReference<String>();
      child.addExecutionStartedListener(engine -> childActive.set(CURRENT.get()));
      final var childState = new AtomicReference<Boolean>();
      final var ends = new AtomicInteger();
      final var mismatchedScopes = new AtomicInteger();
      final var failures = new AtomicInteger();
      child.addExecutionFailureListener(failure -> failures.incrementAndGet());
      child.addExecutionFinishedListener(
          engine -> {
            childState.set(engine.getResult().isResult());
            ends.incrementAndGet();
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
          final var factory = mockStatic(WorkflowEngineFactory.class)) {
        factory
            .when(
                () ->
                    WorkflowEngineFactory.createWorkflowEngine(
                        action, "local", action.getMetadataProvider(), childMeta, action))
            .thenReturn(child);
        try {
          try (final var scope = ExecutionContexts.activate(action.getLogChannelId())) {
            action.execute(new Result(), 0);
            assertEquals(action.getLogChannelId(), CURRENT.get());
          }
          assertNull(CURRENT.get());
          assertTrue(entered.await(5, TimeUnit.SECONDS));
          assertNull(childState.get(), "No-wait launcher must return before child completion");
          assertNull(actionLogId.get(), "Child execution is still delayed after launcher return");
          release.countDown();
          assertTrue(finished.await(5, TimeUnit.SECONDS));
          assertEquals(fail, failures.get() > 0);
          assertEquals(!fail, childState.get());
          assertEquals(fail ? 1 : 0, child.getResult().getNrErrors());
          assertEquals(!fail, child.getResult().isResult());
          assertEquals(1, ends.get());
          assertNotNull(actionLogId.get());
          assertEquals(actionLogId.get(), actionActive.get());
          assertEquals(child.getLogChannelId(), childActive.get());
          assertTrue(activated.contains(child.getLogChannelId()));
          assertTrue(activated.contains(actionLogId.get()));
          assertEquals(
              action.getLogChannelId(),
              LoggingRegistry.getInstance()
                  .getLoggingObject(child.getLogChannelId())
                  .getParent()
                  .getLogChannelId());
          assertEquals(
              child.getLogChannelId(),
              LoggingRegistry.getInstance()
                  .getLoggingObject(actionLogId.get())
                  .getParent()
                  .getLogChannelId());
        } finally {
          release.countDown();
          final var thread = worker.get();
          if (thread != null) {
            // Engine completion does not imply the runner's callbacks/cleanup have returned.
            thread.join(5000);
            assertFalse(thread.isAlive(), "No-wait workflow runner must terminate");
          }
        }
        assertEquals("worker-before", restored.get());
        assertEquals(0, mismatchedScopes.get());
        assertNull(CURRENT.get());
      }
    }
  }
}
