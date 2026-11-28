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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.Result;
import org.apache.hop.core.extension.ExtensionPointMap;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.plugins.IClassLoadingPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.execution.context.ExecutionContextListener;
import org.apache.hop.execution.context.ExecutionContexts;
import org.apache.hop.execution.context.ExecutionScope;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class WorkflowExecutionContextTest {
  private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();

  private record Callback(
      boolean before,
      IAction action,
      IAction template,
      String activeId,
      String parentId,
      Thread thread) {}

  @BeforeEach
  void initializeLogging() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  private void recordActions(final LocalWorkflowEngine workflow, final List<Callback> callbacks) {
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          private void record(final boolean before, final ActionMeta meta, final IAction action) {
            callbacks.add(
                new Callback(
                    before,
                    action,
                    meta.getAction(),
                    CONTEXT.get(),
                    action.getParentWorkflow().getLogChannelId(),
                    Thread.currentThread()));
          }

          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            record(true, meta, action);
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {
            record(false, meta, action);
          }
        });
  }

  private void verifyActions(final List<Callback> callbacks, final Recorder recorder) {
    assertFalse(callbacks.isEmpty());
    for (final var callback : callbacks) {
      assertNotNull(callback.activeId());
      assertEquals(callback.action().getLogChannel().getLogChannelId(), callback.activeId());
      assertNotSame(
          callback.template(), callback.action(), "Listeners must see the executable clone");
      assertTrue(
          recorder.opened.containsKey(
              new Recorder.Activation(callback.activeId(), callback.thread())));
      final var matching = callbacks.stream().filter(c -> c.action() == callback.action()).toList();
      assertEquals(2, matching.size());
      assertTrue(matching.getFirst().before());
      assertFalse(matching.getLast().before());
      assertEquals(matching.getFirst().activeId(), matching.getLast().activeId());
      assertEquals(matching.getFirst().thread(), matching.getLast().thread());
    }
  }

  @Test
  void observesActualClonesAndCreatesFreshIdentitiesOnRepeatedRuns() {
    final var workflow = workflow(false);
    final var recorder = new Recorder();
    final List<Callback> callbacks = new CopyOnWriteArrayList<>();
    recordActions(workflow, callbacks);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      final var firstId = workflow.getLogChannelId();
      assertNull(CONTEXT.get());
      assertEquals(0, workflow.startExecution().getNrErrors());
      final var secondId = workflow.getLogChannelId();
      assertNotEquals(firstId, secondId);
      assertEquals(12, callbacks.size());
      verifyActions(callbacks, recorder);
      for (int i = 0; i < callbacks.size(); i++) {
        assertEquals(i < 6 ? firstId : secondId, callbacks.get(i).parentId());
      }
      assertEquals(6, callbacks.stream().map(Callback::activeId).distinct().count());
      recorder.verifyClosed();
    }
  }

  @Test
  void repeatedEquivalentActionClonesUnderOneWorkflowHaveDistinctChannels() {
    final var workflow = workflow(true);
    final var meta = workflow.getWorkflowMeta();
    final var shared = new ActionMeta(new ActionDummy());
    shared.setName("shared");
    meta.addAction(shared);
    for (final var name : List.of("first", "second")) {
      final var hop = new WorkflowHopMeta(meta.findAction(name), shared);
      hop.setUnconditional(true);
      meta.addWorkflowHop(hop);
    }
    final var recorder = new Recorder();
    final List<Callback> callbacks = new CopyOnWriteArrayList<>();
    recordActions(workflow, callbacks);
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      verifyActions(callbacks, recorder);
      final var repeated =
          callbacks.stream()
              .filter(Callback::before)
              .filter(c -> "shared".equals(c.action().getName()))
              .toList();
      assertEquals(2, repeated.size());
      assertNotSame(repeated.get(0).action(), repeated.get(1).action());
      assertNotEquals(repeated.get(0).activeId(), repeated.get(1).activeId());
      repeated.forEach(c -> assertEquals(workflow.getLogChannelId(), c.parentId()));
      recorder.verifyClosed();
    }
  }

  @Test
  void parallelBranchesActivateIndependentSiblingActions() {
    final var workflow = workflow(true);
    final var recorder = new Recorder();
    final List<Callback> callbacks = new CopyOnWriteArrayList<>();
    final var branches = new CountDownLatch(2);
    recordActions(workflow, callbacks);
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            if (!"START".equals(meta.getName())) {
              branches.countDown();
              try {
                if (!branches.await(5, TimeUnit.SECONDS)) {
                  throw new IllegalStateException("Branches did not execute concurrently");
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
              }
              assertEquals(action.getLogChannel().getLogChannelId(), CONTEXT.get());
            }
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {}
        });
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      assertEquals(0, branches.getCount());
      assertEquals(6, callbacks.size());
      verifyActions(callbacks, recorder);
      callbacks.forEach(c -> assertEquals(workflow.getLogChannelId(), c.parentId()));
      assertEquals(
          2,
          callbacks.stream()
              .filter(Callback::before)
              .filter(c -> !"START".equals(c.action().getName()))
              .map(Callback::thread)
              .distinct()
              .count());
      recorder.verifyClosed();
    }
  }

  @Test
  void failingBeforeListenerRestoresClassloaderAndOuterContextWithoutCallingAfter() {
    final var workflow = workflow(false);
    final var recorder = new Recorder();
    final var originalLoader = Thread.currentThread().getContextClassLoader();
    final List<IAction> after = new ArrayList<>();
    final List<String> before = new ArrayList<>();
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            before.add(CONTEXT.get());
            assertEquals(action.getLogChannel().getLogChannelId(), CONTEXT.get());
            Thread.currentThread().setContextClassLoader(new ClassLoader(originalLoader) {});
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
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      CONTEXT.set("outer-caller-context");
      try {
        assertTrue(workflow.startExecution().getNrErrors() > 0);
        assertEquals("outer-caller-context", CONTEXT.get());
        assertSame(originalLoader, Thread.currentThread().getContextClassLoader());
      } finally {
        CONTEXT.remove();
        Thread.currentThread().setContextClassLoader(originalLoader);
      }
      assertEquals(1, before.size());
      assertNotNull(before.getFirst());
      assertTrue(after.isEmpty());
      assertNotNull(workflow.getRunFailure());
      recorder.verifyClosed();
    }
  }

  @Test
  void skippedActionStillFiresAfterExtensionWithoutCallingActionListeners() throws Exception {
    final var workflow = workflow(false);
    final List<Callback> callbacks = new ArrayList<>();
    recordActions(workflow, callbacks);
    final var previousResult = new AtomicReference<Result>();
    final List<WorkflowExecutionExtension> before = new ArrayList<>();
    final List<WorkflowExecutionExtension> after = new ArrayList<>();
    final List<TestPlugin> plugins = new ArrayList<>();
    try {
      plugins.add(
          extension(
              HopExtensionPoint.WorkflowBeforeActionExecution,
              (log, variables, subject) -> {
                final var extension = (WorkflowExecutionExtension) subject;
                before.add(extension);
                if ("first".equals(extension.actionMeta.getName())) {
                  previousResult.set(extension.result);
                  extension.executeAction = false;
                }
              }));
      plugins.add(
          extension(
              HopExtensionPoint.WorkflowAfterActionExecution,
              (log, variables, subject) -> after.add((WorkflowExecutionExtension) subject)));

      final var result = workflow.startExecution();
      assertEquals(0, result.getNrErrors());
      assertTrue(result.isResult());
      assertEquals(
          List.of("START", "first", "second"),
          before.stream().map(e -> e.actionMeta.getName()).toList());
      assertEquals(
          List.of("START", "first", "second"),
          after.stream().map(e -> e.actionMeta.getName()).toList());
      final var skipped = after.get(1);
      assertSame(workflow, skipped.workflow);
      assertSame(workflow.getWorkflowMeta().findAction("first"), skipped.actionMeta);
      assertFalse(skipped.executeAction);
      assertNotNull(previousResult.get());
      assertSame(previousResult.get(), skipped.result);
      assertSame(previousResult.get(), skipped.actionExecutionResult);
      assertNull(skipped.actionLogChannelId);
      assertEquals(
          List.of("START", "START", "second", "second"),
          callbacks.stream().map(c -> c.action().getName()).toList());
      assertEquals(
          List.of(true, false, true, false), callbacks.stream().map(Callback::before).toList());
      for (final var executed : List.of(after.getFirst(), after.getLast())) {
        assertTrue(executed.executeAction);
        assertNotNull(executed.actionExecutionResult);
        assertNotNull(executed.actionLogChannelId);
      }
    } finally {
      plugins.forEach(ExtensionPointMap.getInstance()::removeExtensionPoint);
    }
  }

  @Test
  void successfulActionRestoresClassloaderBeforeAfterExtension() throws Exception {
    final var workflow = workflow(false);
    final var originalLoader = Thread.currentThread().getContextClassLoader();
    final var actionLoader = new ClassLoader(originalLoader) {};
    final List<ClassLoader> listenerLoaders = new ArrayList<>();
    final List<ClassLoader> extensionLoaders = new ArrayList<>();
    final List<String> completed = new ArrayList<>();
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            Thread.currentThread().setContextClassLoader(actionLoader);
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {
            listenerLoaders.add(Thread.currentThread().getContextClassLoader());
          }
        });
    final var plugin =
        extension(
            HopExtensionPoint.WorkflowAfterActionExecution,
            (log, variables, subject) -> {
              final var extension = (WorkflowExecutionExtension) subject;
              completed.add(extension.actionMeta.getName());
              extensionLoaders.add(Thread.currentThread().getContextClassLoader());
            });
    try {
      final var result = workflow.startExecution();
      assertEquals(0, result.getNrErrors());
      assertTrue(result.isResult());
      assertEquals(List.of("START", "first", "second"), completed);
      assertEquals(3, listenerLoaders.size());
      listenerLoaders.forEach(loader -> assertSame(actionLoader, loader));
      assertEquals(3, extensionLoaders.size());
      extensionLoaders.forEach(loader -> assertSame(originalLoader, loader));
      assertSame(originalLoader, Thread.currentThread().getContextClassLoader());
    } finally {
      ExtensionPointMap.getInstance().removeExtensionPoint(plugin);
      Thread.currentThread().setContextClassLoader(originalLoader);
    }
  }

  private interface TestPlugin extends IPlugin, IClassLoadingPlugin {}

  private TestPlugin extension(
      final HopExtensionPoint point, final IExtensionPoint<Object> callback) throws Exception {
    final var plugin = mock(TestPlugin.class);
    when(plugin.getName()).thenReturn(point.id);
    when(plugin.getIds()).thenReturn(new String[] {"workflow-context-" + point.id});
    doReturn(IExtensionPoint.class).when(plugin).getMainType();
    when(plugin.loadClass(IExtensionPoint.class)).thenReturn(callback);
    ExtensionPointMap.getInstance().addExtensionPoint(plugin);
    return plugin;
  }

  @Test
  void falseEvaluationWithoutErrorsRestoresContext() {
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
            assertEquals(action.getLogChannel().getLogChannelId(), CONTEXT.get());
            result.setResult(false);
          }
        });
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      final var result = workflow.startExecution();
      assertFalse(result.isResult());
      assertEquals(0, result.getNrErrors());
      assertNotNull(workflow.getRunFailure(), "A false workflow result is an existing run failure");
      recorder.verifyClosed();
    }
  }

  @Test
  void delayedChildRetainsExactInvokingCloneAfterParentFinishes() throws Exception {
    final var workflow = workflow(false);
    final var child = workflow(false);
    final var recorder = new Recorder();
    final var invokingAction = new AtomicReference<IAction>();
    final var invokingId = new AtomicReference<String>();
    workflow.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> engine, ActionMeta meta, IAction action) {
            if ("first".equals(meta.getName())) {
              invokingAction.set(action);
              invokingId.set(action.getLogChannel().getLogChannelId());
              child.setParentLoggingObject((ILoggingObject) action);
            }
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> engine,
              ActionMeta meta,
              IAction action,
              Result result) {}
        });
    final List<Callback> childCallbacks = new CopyOnWriteArrayList<>();
    recordActions(child, childCallbacks);
    try (final var registration = ExecutionContexts.addListener(recorder);
        final var worker = Executors.newSingleThreadExecutor()) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      assertNull(CONTEXT.get());
      worker
          .submit(
              () -> {
                assertNull(CONTEXT.get());
                assertEquals(0, child.startExecution().getNrErrors());
                assertNull(CONTEXT.get());
              })
          .get(10, TimeUnit.SECONDS);
      worker
          .submit(() -> assertNull(CONTEXT.get(), "Reused worker must not retain child context"))
          .get(10, TimeUnit.SECONDS);
      assertSame(invokingAction.get(), child.getParent());
      assertEquals(invokingId.get(), child.getParent().getLogChannelId());
      assertEquals(
          workflow.getLogChannelId(),
          LoggingRegistry.getInstance()
              .getLoggingObject(invokingId.get())
              .getParent()
              .getLogChannelId());
      verifyActions(childCallbacks, recorder);
      childCallbacks.forEach(c -> assertEquals(child.getLogChannelId(), c.parentId()));
      recorder.verifyClosed();
    }
  }

  @Test
  void localSetupDoesNotActivateAStaleChannelAndCleanupActivatesTheRunChannel() {
    final var phases = new ArrayList<String>();
    final var phaseIds = new ArrayList<String>();
    final var workflow =
        new LocalWorkflowEngine(workflow(false).getWorkflowMeta()) {
          private void record() {
            phases.add(CONTEXT.get());
            phaseIds.add(getLogChannelId());
          }

          @Override
          public void lookupExecutionInformationLocation() {
            record();
          }

          @Override
          public synchronized void stopExecutionInfoTimer() {
            record();
          }
        };
    final var recorder = new Recorder();
    try (final var registration = ExecutionContexts.addListener(recorder)) {
      assertEquals(0, workflow.startExecution().getNrErrors());
      assertTrue(phases.size() >= 2);
      assertNull(phases.getFirst(), "Setup precedes creation of the run channel");
      for (int i = 1; i < phases.size(); i++) {
        assertEquals(workflow.getLogChannelId(), phaseIds.get(i));
        assertEquals(phaseIds.get(i), phases.get(i));
      }
      recorder.verifyClosed();
    }
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

  private static class Recorder implements ExecutionContextListener {
    private record Activation(String id, Thread thread) {}

    private final Map<Activation, AtomicInteger> opened = new ConcurrentHashMap<>();
    private final Map<Activation, AtomicInteger> closed = new ConcurrentHashMap<>();
    private final Map<Activation, AtomicInteger> active = new ConcurrentHashMap<>();
    private final List<String> violations = new CopyOnWriteArrayList<>();

    @Override
    public ExecutionScope activate(final String logChannelId) {
      final var previous = CONTEXT.get();
      final var activation = new Activation(logChannelId, Thread.currentThread());
      opened.computeIfAbsent(activation, ignored -> new AtomicInteger()).incrementAndGet();
      active.computeIfAbsent(activation, ignored -> new AtomicInteger()).incrementAndGet();
      CONTEXT.set(logChannelId);
      return () -> {
        if (activation.thread() != Thread.currentThread()) {
          violations.add("Scope closed on another thread: " + logChannelId);
        }
        if (!logChannelId.equals(CONTEXT.get())) {
          violations.add("Scope did not restore in nesting order: " + logChannelId);
        }
        closed.computeIfAbsent(activation, ignored -> new AtomicInteger()).incrementAndGet();
        if (active.get(activation).decrementAndGet() < 0) {
          violations.add("Scope closed more than once: " + logChannelId);
        }
        if (previous == null) {
          CONTEXT.remove();
        } else {
          CONTEXT.set(previous);
        }
      };
    }

    private void verifyClosed() {
      assertFalse(opened.isEmpty());
      assertEquals(opened.keySet(), closed.keySet());
      opened.forEach(
          (key, count) -> assertEquals(count.get(), closed.get(key).get(), key.toString()));
      active.forEach((key, count) -> assertEquals(0, count.get(), key.toString()));
      assertTrue(violations.isEmpty(), violations.toString());
      assertNull(CONTEXT.get());
    }
  }
}
