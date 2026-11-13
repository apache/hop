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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.execution.lifecycle.ExecutionHandle;
import org.apache.hop.execution.lifecycle.ExecutionKind;
import org.apache.hop.execution.lifecycle.ExecutionLifecycle;
import org.apache.hop.execution.lifecycle.ExecutionObserver;
import org.apache.hop.execution.lifecycle.ExecutionState;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ActionWorkflowAsyncLifecycleTest {
  public static class Start extends ActionBase {
    @Override
    public boolean isStart() {
      return true;
    }

    @Override
    public Result execute(Result result, int nr) {
      result.setResult(true);
      return result;
    }
  }

  @Test
  @Timeout(10)
  void realNoWaitLauncherCapturesActionBeforeParentEndsAndDoesNotWaitForChild() throws Exception {
    HopEnvironment.init();
    final var entered = new CountDownLatch(1);
    final var release = new CountDownLatch(1);
    final var finished = new CountDownLatch(1);
    final var childMeta = new WorkflowMeta();
    childMeta.setName("same-file");
    final var start = new Start();
    start.setName("start");
    childMeta.addAction(new ActionMeta(start));
    final var child =
        new LocalWorkflowEngine(childMeta) {
          @Override
          public Result startExecution() {
            entered.countDown();
            try {
              if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test child release timeout");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
            return super.startExecution();
          }
        };
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
    final var seen = new AtomicReference<ExecutionHandle>();
    final var ends = new ConcurrentHashMap<Object, Integer>();
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
                      ExecutionHandle handle, ExecutionState state, Throwable failure) {
                    ends.merge(handle.getSubject(), 1, Integer::sum);
                    if (handle.getSubject() == child) {
                      finished.countDown();
                    }
                  }
                });
        final var factory = mockStatic(WorkflowEngineFactory.class)) {
      factory
          .when(
              () ->
                  WorkflowEngineFactory.createWorkflowEngine(
                      action, "local", action.getMetadataProvider(), childMeta, action))
          .thenReturn(child);
      ExecutionLifecycle.begin(parent, null, ExecutionKind.WORKFLOW, "parent", 0);
      final var launcher =
          ExecutionLifecycle.begin(action, parent, ExecutionKind.ACTION, "same-launcher", 0);
      try (final var scope = ExecutionLifecycle.activate(action)) {
        action.execute(new Result(), 0);
      }
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertNull(seen.get(), "Child has not begun, yet no-wait action already returned");
      ExecutionLifecycle.finish(action, ExecutionState.SUCCEEDED, null);
      ExecutionLifecycle.finish(parent, ExecutionState.SUCCEEDED, null);
      release.countDown();
      assertTrue(finished.await(5, TimeUnit.SECONDS));
      assertSame(launcher, seen.get().getParent());
      assertEquals(1, ends.get(child));
      assertEquals(1, ends.get(action));
      assertEquals(1, ends.get(parent));
      assertNull(ExecutionLifecycle.current());
      assertNull(ExecutionLifecycle.get(child));
    } finally {
      release.countDown();
    }
  }
}
