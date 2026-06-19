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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowTest {

  int count = 10000;

  private abstract class WorkflowKicker implements Runnable {
    protected IWorkflowEngine<WorkflowMeta> workflow;
    protected int c = 0;
    protected CountDownLatch start;
    protected int max = count;

    WorkflowKicker(IWorkflowEngine<WorkflowMeta> workflow, CountDownLatch start) {
      this.workflow = workflow;
      this.start = start;
      this.workflow.setLogLevel(LogLevel.MINIMAL);
    }

    public void await() {
      try {
        start.await();
      } catch (InterruptedException e) {
        throw new HopRuntimeException();
      }
    }

    public boolean isStopped() {
      c++;
      return c >= max;
    }
  }

  private class WorkflowStoppedListenerAdder extends WorkflowKicker {
    WorkflowStoppedListenerAdder(IWorkflowEngine<WorkflowMeta> workflow, CountDownLatch start) {
      super(workflow, start);
    }

    @Override
    public void run() {
      await();
      while (!isStopped()) {
        workflow.addExecutionStoppedListener(w -> {});
      }
    }
  }

  private class WorkflowStopExecutionCaller extends WorkflowKicker {
    WorkflowStopExecutionCaller(IWorkflowEngine<WorkflowMeta> workflow, CountDownLatch start) {
      super(workflow, start);
    }

    @Override
    public void run() {
      await();
      while (!isStopped()) {
        workflow.stopExecution();
      }
    }
  }

  /** Used by {@link ActionBlockingForWorkflowStopTest} to coordinate with the test thread. */
  private static volatile CountDownLatch blockingEntered;

  private static volatile CountDownLatch blockingRelease;

  @Action(id = "ActionBlockingForWorkflowStopTest", name = "Blocking action for workflow stop test")
  public static class ActionBlockingForWorkflowStopTest extends ActionBase implements IAction {

    public ActionBlockingForWorkflowStopTest() {
      super("", "", "ActionBlockingForWorkflowStopTest");
    }

    @Override
    public Result execute(Result prevResult, int nr) throws HopException {
      CountDownLatch entered = blockingEntered;
      CountDownLatch release = blockingRelease;
      if (entered != null) {
        entered.countDown();
      }
      if (release != null) {
        try {
          if (!release.await(60, TimeUnit.SECONDS)) {
            throw new HopException("Timed out waiting in blocking test action");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HopException(e);
        }
      }
      Result r = prevResult == null ? new Result() : prevResult.clone();
      r.setResult(true);
      r.setNrErrors(0);
      r.setStopped(false);
      return r;
    }
  }

  @BeforeAll
  static void beforeClass() throws HopException, HopPluginException {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            ActionBlockingForWorkflowStopTest.class.getName(),
            ActionPluginType.class,
            Action.class);
  }

  @BeforeEach
  void before() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  /**
   * When a workflow is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  void testTwoWorkflowsGetSameLogChannelId() {
    WorkflowMeta meta = mock(WorkflowMeta.class);

    IWorkflowEngine<WorkflowMeta> workflow1 = new LocalWorkflowEngine(meta);
    IWorkflowEngine<WorkflowMeta> workflow2 = new LocalWorkflowEngine(meta);

    assertEquals(workflow1.getLogChannelId(), workflow2.getLogChannelId());
  }

  /** Test that workflow stop listeners can be accessed concurrently */
  @Test
  void testExecutionStoppedListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch(1);
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine();
    WorkflowStopExecutionCaller stopper = new WorkflowStopExecutionCaller(workflow, start);
    WorkflowStoppedListenerAdder adder = new WorkflowStoppedListenerAdder(workflow, start);
    startThreads(stopper, adder, start);
    assertEquals(count, adder.c, "All workflow stop listeners is added");
    assertEquals(count, stopper.c, "All stop call success");
  }

  private void startThreads(Runnable run1, Runnable run2, CountDownLatch start)
      throws InterruptedException {
    Thread thread1 = new Thread(run1);
    Thread thread2 = new Thread(run2);
    thread1.start();
    thread2.start();
    start.countDown();
    thread1.join();
    thread2.join();
  }

  /**
   * When stop is requested while an action is running, that action can still finish with a
   * successful Result that has stopped=false. The workflow result must still report stopped so
   * transactional handling can roll back.
   */
  @Test
  void workflowResultReflectsStopWhenStoppedDuringActionExecution() throws Exception {
    blockingEntered = new CountDownLatch(1);
    blockingRelease = new CountDownLatch(1);
    try {
      WorkflowMeta meta = new WorkflowMeta();
      meta.setName("workflow-stop-result-test");

      ActionStart start = new ActionStart("START");
      ActionMeta startMeta = new ActionMeta(start);
      meta.addAction(startMeta);

      ActionBlockingForWorkflowStopTest blocking = new ActionBlockingForWorkflowStopTest();
      blocking.setName("blocking");
      ActionMeta blockingMeta = new ActionMeta(blocking);
      meta.addAction(blockingMeta);

      ActionDummy neverRun = new ActionDummy();
      neverRun.setName("never-run");
      ActionMeta neverRunMeta = new ActionMeta(neverRun);
      meta.addAction(neverRunMeta);

      meta.addWorkflowHop(new WorkflowHopMeta(startMeta, blockingMeta));
      WorkflowHopMeta hopToNext = new WorkflowHopMeta(blockingMeta, neverRunMeta);
      hopToNext.setUnconditional();
      meta.addWorkflowHop(hopToNext);

      LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
      engine.setLogLevel(LogLevel.MINIMAL);

      Thread runner = new Thread(engine::startExecution);
      runner.start();
      assertTrue(blockingEntered.await(30, TimeUnit.SECONDS));
      engine.stopExecution();
      blockingRelease.countDown();
      runner.join(60000);
      assertTrue(engine.getResult().isStopped(), "Result should reflect user stop for rollback");
    } finally {
      blockingEntered = null;
      blockingRelease = null;
    }
  }
}
