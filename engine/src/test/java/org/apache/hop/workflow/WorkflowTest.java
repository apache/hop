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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowTest {
  private static final String STRING_DEFAULT = "<def>";
  private IWorkflowEngine<WorkflowMeta> mockedWorkflow;
  private Database mockedDataBase;
  private IVariables mockedVariableSpace;
  private IHopMetadataProvider mockedMetadataProvider;
  private WorkflowMeta mockedWorkflowMeta;
  private ActionMeta mockedActionMeta;
  private ActionStart mockedActionStart;
  private LogChannel mockedLogChannel;
  int count = 10000;

  private abstract class WorkflowKicker implements Runnable {
    protected IWorkflowEngine<WorkflowMeta> workflow;
    protected int c = 0;
    protected CountDownLatch start;
    protected int max = count;

    WorkflowKicker(IWorkflowEngine<WorkflowMeta> workflow, CountDownLatch start) {
      this.workflow = workflow;
      this.start = start;
    }

    public void await() {
      try {
        start.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
    }

    public boolean isStopped() {
      c++;
      return c >= max;
    }
  }

  private class WorkflowFinishedListenerAdder extends WorkflowKicker {
    WorkflowFinishedListenerAdder(IWorkflowEngine<WorkflowMeta> workflow, CountDownLatch start) {
      super(workflow, start);
    }

    @Override
    public void run() {
      await();
      while (!isStopped()) {
        workflow.addExecutionFinishedListener(w -> {});
      }
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

  @BeforeClass
  public static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void init() {
    mockedDataBase = mock(Database.class);
    mockedWorkflow = mock(Workflow.class);
    mockedVariableSpace = mock(IVariables.class);
    mockedMetadataProvider = mock(IHopMetadataProvider.class);
    mockedWorkflowMeta = mock(WorkflowMeta.class);
    mockedActionMeta = mock(ActionMeta.class);
    mockedActionStart = mock(ActionStart.class);
    mockedLogChannel = mock(LogChannel.class);
  }

  /**
   * When a workflow is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  public void testTwoWorkflowsGetSameLogChannelId() {
    WorkflowMeta meta = mock(WorkflowMeta.class);

    IWorkflowEngine<WorkflowMeta> workflow1 = new LocalWorkflowEngine(meta);
    IWorkflowEngine<WorkflowMeta> workflow2 = new LocalWorkflowEngine(meta);

    assertEquals(workflow1.getLogChannelId(), workflow2.getLogChannelId());
  }

  /**
   * Test that workflow stop listeners can be accessed concurrently
   *
   * @throws InterruptedException
   */
  @Test
  public void testExecutionStoppedListenersConcurrentModification() throws InterruptedException {
    CountDownLatch start = new CountDownLatch(1);
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine();
    WorkflowStopExecutionCaller stopper = new WorkflowStopExecutionCaller(workflow, start);
    WorkflowStoppedListenerAdder adder = new WorkflowStoppedListenerAdder(workflow, start);
    startThreads(stopper, adder, start);
    assertEquals("All workflow stop listeners is added", count, adder.c);
    assertEquals("All stop call success", count, stopper.c);
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
}
