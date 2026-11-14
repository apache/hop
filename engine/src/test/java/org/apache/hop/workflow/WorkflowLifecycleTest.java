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
package org.apache.hop.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.execution.lifecycle.ExecutionHandle;
import org.apache.hop.execution.lifecycle.ExecutionKind;
import org.apache.hop.execution.lifecycle.ExecutionLifecycle;
import org.apache.hop.execution.lifecycle.ExecutionObserver;
import org.apache.hop.execution.lifecycle.ExecutionState;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class WorkflowLifecycleTest {
  @BeforeEach
  void initializeLogging() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void observesActualClonesAndCreatesFreshIdentitiesOnRepeatedRuns() {
    final var workflow = workflow(false);
    final var recorder = new Recorder();
    final List<ExecutionHandle> callbacks = new ArrayList<>();
    final List<IAction> callbackSubjects = new ArrayList<>();
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            callbacks.add(ExecutionLifecycle.current());
            callbackSubjects.add(action);
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {
            callbacks.add(ExecutionLifecycle.current());
            callbackSubjects.add(action);
          }
        });
    try (var registration = ExecutionLifecycle.addObserver(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      assertEquals(0, workflow.startExecution().getNrErrors());
      assertNull(ExecutionLifecycle.current());
      assertNull(ExecutionLifecycle.get(workflow));
    }
    final var roots =
        recorder.started.stream().filter(h -> h.getKind() == ExecutionKind.WORKFLOW).toList();
    assertEquals(2, roots.size());
    assertNotSame(roots.get(0), roots.get(1));
    assertEquals(12, callbacks.size());
    for (int i = 0; i < callbacks.size(); i += 2) {
      final var handle = callbacks.get(i);
      assertSame(handle, callbacks.get(i + 1));
      assertSame(callbackSubjects.get(i), handle.getSubject());
      assertSame(callbackSubjects.get(i + 1), handle.getSubject());
      assertSame(workflow, handle.getParent().getSubject());
      assertEquals(ExecutionKind.ACTION, handle.getKind());
      assertNotSame(
          workflow.getWorkflowMeta().findAction(handle.getName()).getAction(), handle.getSubject());
      assertEquals(ExecutionState.SUCCEEDED, recorder.finished.get(handle));
    }
    assertEquals(recorder.started.size(), recorder.finished.size());
  }

  @Test
  void parallelBranchesActivateWorkflowAndRemainSiblingActions() {
    final var workflow = workflow(true);
    final var recorder = new Recorder();
    final List<ExecutionHandle> callbacks = Collections.synchronizedList(new ArrayList<>());
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            callbacks.add(ExecutionLifecycle.current());
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {}
        });
    try (var registration = ExecutionLifecycle.addObserver(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      assertNull(ExecutionLifecycle.current());
    }
    assertEquals(3, callbacks.size());
    final var root = recorder.started.getFirst();
    for (final var callback : callbacks) {
      assertSame(root, callback.getParent());
      assertEquals(ExecutionState.SUCCEEDED, recorder.finished.get(callback));
    }
  }

  @Test
  void failingBeforeListenerRestoresClassloaderAndContextWithoutCallingAfter() {
    final var workflow = workflow(false);
    final var recorder = new Recorder();
    final var originalLoader = Thread.currentThread().getContextClassLoader();
    final List<IAction> after = new ArrayList<>();
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            throw new IllegalStateException("before failed");
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {
            after.add(action);
          }
        });
    final var external = new Object();
    try (var registration = ExecutionLifecycle.addObserver(recorder)) {
      final var parent =
          ExecutionLifecycle.begin(external, null, ExecutionKind.ACTION, "external", 0);
      try (var scope = ExecutionLifecycle.activate(external)) {
        assertTrue(workflow.startExecution().getNrErrors() > 0);
        assertSame(parent, ExecutionLifecycle.current());
        assertSame(originalLoader, Thread.currentThread().getContextClassLoader());
      } finally {
        ExecutionLifecycle.finish(external, ExecutionState.SUCCEEDED, null);
      }
      assertNull(ExecutionLifecycle.current());
    }
    assertTrue(after.isEmpty());
    assertEquals(3, recorder.finished.size());
    assertEquals(
        2, recorder.finished.values().stream().filter(s -> s == ExecutionState.FAILED).count());
  }

  @Test
  void falseEvaluationWithoutErrorsIsNotALifecycleFailure() {
    final var workflow = workflow(false);
    final var recorder = new Recorder();
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {}

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {
            result.setResult(false);
          }
        });
    try (var registration = ExecutionLifecycle.addObserver(recorder)) {
      final var result = workflow.startExecution();
      assertFalse(result.isResult());
      assertEquals(0, result.getNrErrors());
    }
    assertTrue(recorder.finished.values().stream().allMatch(s -> s == ExecutionState.SUCCEEDED));
  }

  @Test
  void asyncChildRetainsExactInvokingCloneAfterParentFinishes() throws InterruptedException {
    final var workflow = workflow(false);
    final var child = workflow(false);
    final var recorder = new Recorder();
    final var invokingHandle = new AtomicReference<ExecutionHandle>();
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            if ("first".equals(meta.getName())) {
              invokingHandle.set(ExecutionLifecycle.current());
              ExecutionLifecycle.associate(child, action);
            }
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {}
        });
    try (var registration = ExecutionLifecycle.addObserver(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      final var runner = new Thread(child::startExecution, "workflow-lifecycle-async-test");
      runner.start();
      runner.join(10000);
      assertFalse(runner.isAlive());
      assertNull(ExecutionLifecycle.current());
    }
    final var childHandle =
        recorder.started.stream().filter(h -> h.getSubject() == child).findFirst().orElseThrow();
    assertSame(invokingHandle.get(), childHandle.getParent());
    assertEquals(ExecutionState.SUCCEEDED, recorder.finished.get(childHandle));
  }

  @Test
  void localSetupAndCleanupUseTheSameWorkflowHandle() {
    final var phases = new ArrayList<ExecutionHandle>();
    final var workflow =
        new LocalWorkflowEngine(workflow(false).getWorkflowMeta()) {
          @Override
          public void lookupExecutionInformationLocation() {
            phases.add(ExecutionLifecycle.current());
          }

          @Override
          public synchronized void stopExecutionInfoTimer() {
            phases.add(ExecutionLifecycle.current());
          }
        };
    final var recorder = new Recorder();
    try (var registration = ExecutionLifecycle.addObserver(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
    }
    assertTrue(phases.size() >= 2);
    final var root = recorder.started.getFirst();
    phases.forEach(handle -> assertSame(root, handle));
    assertEquals(
        1, recorder.started.stream().filter(h -> h.getKind() == ExecutionKind.WORKFLOW).count());
  }

  private LocalWorkflowEngine workflow(final boolean parallel) {
    final var meta = new WorkflowMeta();
    meta.setName("observed workflow");
    final var start = new ActionMeta(new ActionStart("START"));
    final var firstAction = new ActionDummy();
    firstAction.setName("first");
    final var first = new ActionMeta(firstAction);
    final var secondAction = new ActionDummy();
    secondAction.setName("second");
    final var second = new ActionMeta(secondAction);
    meta.addAction(start);
    meta.addAction(first);
    meta.addAction(second);
    final var firstHop = new WorkflowHopMeta(start, first);
    firstHop.setUnconditional(true);
    meta.addWorkflowHop(firstHop);
    final var secondHop = new WorkflowHopMeta(parallel ? start : first, second);
    secondHop.setUnconditional(true);
    meta.addWorkflowHop(secondHop);
    start.setLaunchingInParallel(parallel);
    return new LocalWorkflowEngine(meta);
  }

  private static class Recorder implements ExecutionObserver {
    private final List<ExecutionHandle> started = Collections.synchronizedList(new ArrayList<>());
    private final Map<ExecutionHandle, ExecutionState> finished =
        Collections.synchronizedMap(new IdentityHashMap<>());

    @Override
    public void started(final ExecutionHandle handle) {
      started.add(handle);
    }

    @Override
    public void finished(
        final ExecutionHandle handle, final ExecutionState state, final Throwable failure) {
      finished.put(handle, state);
    }
  }
}
