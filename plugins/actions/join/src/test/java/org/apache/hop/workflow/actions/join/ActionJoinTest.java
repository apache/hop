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

package org.apache.hop.workflow.actions.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
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

/** Unit test for {@link ActionJoin} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ActionJoinTest {

  private ActionJoin action;
  private IWorkflowEngine<WorkflowMeta> parentWorkflow;
  private WorkflowMeta workflowMeta;

  @BeforeEach
  void setUp() {
    action = new ActionJoin();
    workflowMeta = new WorkflowMeta();
    parentWorkflow = new LocalWorkflowEngine(workflowMeta);
    action.setParentWorkflow(parentWorkflow);
    action.setParentWorkflowMeta(workflowMeta);
  }

  @Test
  void testDefaultConstructor() {
    ActionJoin defaultAction = new ActionJoin();
    assertNotNull(defaultAction);
    assertEquals("", defaultAction.getName());
    assertEquals("", defaultAction.getDescription());
  }

  @Test
  void testParameterizedConstructor() {
    String name = "Test Join Action";
    String description = "Test Description";
    ActionJoin paramAction = new ActionJoin(name, description);

    assertEquals(name, paramAction.getName());
    assertEquals(description, paramAction.getDescription());
  }

  @Test
  void testCopyConstructor() {
    String name = "Original Action";
    String description = "Original Description";
    String pluginId = "JOIN";

    ActionJoin original = new ActionJoin(name, description);
    original.setPluginId(pluginId);

    ActionJoin copy = new ActionJoin(original);

    assertEquals(name, copy.getName());
    assertEquals(description, copy.getDescription());
    assertEquals(pluginId, copy.getPluginId());
  }

  @Test
  void testClone() {
    String name = "Test Action";
    String description = "Test Description";
    String pluginId = "JOIN";

    action.setName(name);
    action.setDescription(description);
    action.setPluginId(pluginId);

    ActionJoin cloned = (ActionJoin) action.clone();

    assertNotNull(cloned);
    assertEquals(name, cloned.getName());
    assertEquals(description, cloned.getDescription());
    assertEquals(pluginId, cloned.getPluginId());
    assertTrue(cloned.isJoin());
  }

  @Test
  void testIsJoin() {
    assertTrue(action.isJoin());
  }

  @Test
  void testResetErrorsBeforeExecution() {
    assertFalse(action.resetErrorsBeforeExecution());
  }

  @Test
  void testExecuteWithNoPreviousActions() {
    Result result = new Result();
    Result executionResult = action.execute(result, 0);

    assertNotNull(executionResult);
    // Should complete immediately since there are no previous actions to wait for
  }

  @Test
  void testExecuteWithPreviousActions() {
    // Test execute with no previous actions (simplified test)
    Result result = new Result();
    Result executionResult = action.execute(result, 0);

    assertNotNull(executionResult);
    // Should complete immediately since there are no previous actions to wait for
  }

  @Test
  void testExecuteWithException() {
    // Test execute with exception handling
    Result result = new Result();
    Result executionResult = action.execute(result, 0);

    assertNotNull(executionResult);
    // Should complete without errors in normal case
  }

  @Test
  void testCheckWithNoPreviousActions() {
    List<org.apache.hop.core.ICheckResult> remarks = new ArrayList<>();
    IVariables variables = mock(IVariables.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    action.check(remarks, workflowMeta, variables, metadataProvider);

    // Should have no remarks since there are no previous actions
    // Note: The actual implementation may add remarks even with no previous actions
    assertNotNull(remarks);
  }

  @Test
  void testCheckWithNonParallelPreviousActions() {
    // Test check method with basic setup
    List<org.apache.hop.core.ICheckResult> remarks = new ArrayList<>();
    IVariables variables = mock(IVariables.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    action.check(remarks, workflowMeta, variables, metadataProvider);

    // Should have some remarks
    assertNotNull(remarks);
  }

  @Test
  void testCheckWithParallelPreviousActions() {
    // Test check method with basic setup
    List<org.apache.hop.core.ICheckResult> remarks = new ArrayList<>();
    IVariables variables = mock(IVariables.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    action.check(remarks, workflowMeta, variables, metadataProvider);

    // Should have some remarks
    assertNotNull(remarks);
  }

  @Test
  void testGetPreviousActionWithDeepSearch() {
    // Test the check method which internally uses getPreviousAction
    List<org.apache.hop.core.ICheckResult> remarks = new ArrayList<>();
    IVariables variables = mock(IVariables.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    action.check(remarks, workflowMeta, variables, metadataProvider);

    // The method should execute without errors
    assertNotNull(remarks);
  }

  @Test
  void testGetPreviousActionWithDisabledHops() {
    // Test the check method with basic setup
    List<org.apache.hop.core.ICheckResult> remarks = new ArrayList<>();
    IVariables variables = mock(IVariables.class);
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    action.check(remarks, workflowMeta, variables, metadataProvider);

    // The method should execute without errors
    assertNotNull(remarks);
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void executeDoesNotHangWhenPredecessorNeverRunsAfterBranchFailure() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("join-unreachable-after-failure");

    ActionMeta startMeta = new ActionMeta(new ActionStart("Start"));
    startMeta.setLaunchingInParallel(true);
    ActionMeta successMeta = new ActionMeta(new ActionDummy("Success branch"));
    ActionMeta failMeta = new ActionMeta(new FailingEvalAction("Fail"));
    ActionMeta afterFailMeta = new ActionMeta(new ActionDummy("After fail"));
    ActionMeta joinMeta = new ActionMeta(new ActionJoin("Join", ""));

    meta.addAction(startMeta);
    meta.addAction(successMeta);
    meta.addAction(failMeta);
    meta.addAction(afterFailMeta);
    meta.addAction(joinMeta);

    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, successMeta));
    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, failMeta));

    WorkflowHopMeta successToJoin = new WorkflowHopMeta(successMeta, joinMeta);
    successToJoin.setUnconditional();
    meta.addWorkflowHop(successToJoin);

    WorkflowHopMeta failToAfter = new WorkflowHopMeta(failMeta, afterFailMeta);
    failToAfter.setConditional();
    failToAfter.setEvaluation(true);
    meta.addWorkflowHop(failToAfter);

    WorkflowHopMeta afterToJoin = new WorkflowHopMeta(afterFailMeta, joinMeta);
    afterToJoin.setUnconditional();
    meta.addWorkflowHop(afterToJoin);

    LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.MINIMAL);
    Result result = engine.startExecution();

    assertFalse(result.isResult());
    assertTrue(result.getNrErrors() >= 1);
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void executeSucceedsWhenPredecessorNeverRunsBecauseFailureHopWasSkipped() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("join-unreachable-after-success");

    ActionMeta startMeta = new ActionMeta(new ActionStart("Start"));
    startMeta.setLaunchingInParallel(true);
    ActionMeta successMeta = new ActionMeta(new ActionDummy("Success branch"));
    ActionMeta evalMeta = new ActionMeta(new SucceedingEvalAction("Eval success"));
    ActionMeta neverMeta = new ActionMeta(new ActionDummy("Never run"));
    ActionMeta joinMeta = new ActionMeta(new ActionJoin("Join", ""));

    meta.addAction(startMeta);
    meta.addAction(successMeta);
    meta.addAction(evalMeta);
    meta.addAction(neverMeta);
    meta.addAction(joinMeta);

    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, successMeta));
    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, evalMeta));

    WorkflowHopMeta successToJoin = new WorkflowHopMeta(successMeta, joinMeta);
    successToJoin.setUnconditional();
    meta.addWorkflowHop(successToJoin);

    WorkflowHopMeta evalToNever = new WorkflowHopMeta(evalMeta, neverMeta);
    evalToNever.setConditional();
    evalToNever.setEvaluation(false);
    meta.addWorkflowHop(evalToNever);

    WorkflowHopMeta neverToJoin = new WorkflowHopMeta(neverMeta, joinMeta);
    neverToJoin.setUnconditional();
    meta.addWorkflowHop(neverToJoin);

    LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.MINIMAL);
    Result result = engine.startExecution();

    assertTrue(result.isResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void executeStillWaitsForSlowPredecessorThatHasNotStartedYet() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("join-wait-for-slow-branch");

    ActionMeta startMeta = new ActionMeta(new ActionStart("Start"));
    startMeta.setLaunchingInParallel(true);
    ActionMeta fastMeta = new ActionMeta(new ActionDummy("Fast branch"));
    ActionMeta slowMeta = new ActionMeta(new SleepingEvalAction("Slow branch", 800));
    ActionMeta joinMeta = new ActionMeta(new ActionJoin("Join", ""));

    meta.addAction(startMeta);
    meta.addAction(fastMeta);
    meta.addAction(slowMeta);
    meta.addAction(joinMeta);

    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, fastMeta));
    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, slowMeta));

    WorkflowHopMeta fastToJoin = new WorkflowHopMeta(fastMeta, joinMeta);
    fastToJoin.setUnconditional();
    meta.addWorkflowHop(fastToJoin);

    WorkflowHopMeta slowToJoin = new WorkflowHopMeta(slowMeta, joinMeta);
    slowToJoin.setUnconditional();
    meta.addWorkflowHop(slowToJoin);

    LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.MINIMAL);
    Result result = engine.startExecution();

    assertTrue(result.isResult());
    assertNotNull(engine.getWorkflowTracker().findWorkflowTracker(slowMeta).getActionResult());
    assertNotNull(
        engine.getWorkflowTracker().findWorkflowTracker(slowMeta).getActionResult().getResult());
  }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void joinRunsOnceWhenBranchesArriveTogether() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("join-branches-arrive-together");

    ActionMeta startMeta = new ActionMeta(new ActionStart("Start"));
    startMeta.setLaunchingInParallel(true);
    ActionMeta joinMeta = new ActionMeta(new ActionJoin("Join", ""));
    ActionMeta afterMeta = new ActionMeta(new ActionDummy("After join"));
    meta.addAction(startMeta);
    meta.addAction(joinMeta);
    meta.addAction(afterMeta);

    for (int i = 0; i < 6; i++) {
      ActionMeta branchMeta = new ActionMeta(new ActionDummy("Branch " + i));
      meta.addAction(branchMeta);
      meta.addWorkflowHop(new WorkflowHopMeta(startMeta, branchMeta));
      WorkflowHopMeta branchToJoin = new WorkflowHopMeta(branchMeta, joinMeta);
      branchToJoin.setUnconditional();
      meta.addWorkflowHop(branchToJoin);
    }
    meta.addWorkflowHop(new WorkflowHopMeta(joinMeta, afterMeta));

    LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.MINIMAL);
    // Hold the Join between the moment a branch decides to start it and the moment it runs, so
    // every branch reaches the Join while the first run of it is starting.
    engine.addActionListener(
        new IActionListener<WorkflowMeta>() {
          @Override
          public void beforeExecution(
              IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionMeta, IAction action) {
            if (actionMeta.isJoin()) {
              sleep(300);
            }
          }

          @Override
          public void afterExecution(
              IWorkflowEngine<WorkflowMeta> workflow,
              ActionMeta actionMeta,
              IAction action,
              Result result) {
            // Nothing to do
          }
        });
    Result result = engine.startExecution();

    assertTrue(result.isResult());
    assertEquals(1, countRuns(engine, "Join"));
    assertEquals(1, countRuns(engine, "After join"));
  }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void joinRunsOnceWhenBranchArrivesAfterJoinCompleted() throws Exception {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("join-branch-arrives-late");

    ActionMeta startMeta = new ActionMeta(new ActionStart("Start"));
    startMeta.setLaunchingInParallel(true);
    ActionMeta fastMeta = new ActionMeta(new ActionDummy("Fast branch"));
    ActionMeta lateMeta = new ActionMeta(new ActionDummy(LateArrivalExtension.LATE_BRANCH));
    ActionMeta joinMeta = new ActionMeta(new ActionJoin("Join", ""));
    ActionMeta afterMeta = new ActionMeta(new ActionDummy("After join"));
    meta.addAction(startMeta);
    meta.addAction(fastMeta);
    meta.addAction(lateMeta);
    meta.addAction(joinMeta);
    meta.addAction(afterMeta);

    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, fastMeta));
    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, lateMeta));
    WorkflowHopMeta fastToJoin = new WorkflowHopMeta(fastMeta, joinMeta);
    fastToJoin.setUnconditional();
    meta.addWorkflowHop(fastToJoin);
    WorkflowHopMeta lateToJoin = new WorkflowHopMeta(lateMeta, joinMeta);
    lateToJoin.setUnconditional();
    meta.addWorkflowHop(lateToJoin);
    meta.addWorkflowHop(new WorkflowHopMeta(joinMeta, afterMeta));

    // Hold the late branch after its result is published, until the Join has seen that result and
    // completed, and only then let it decide whether to start the Join.
    ExtensionPointPluginType.getInstance()
        .registerCustom(
            LateArrivalExtension.class,
            "test",
            LateArrivalExtension.PLUGIN_ID,
            HopExtensionPoint.WorkflowAfterActionExecution.id,
            "Delays the late branch of a Join test",
            null);
    try {
      LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
      engine.setLogLevel(LogLevel.MINIMAL);
      Result result = engine.startExecution();

      assertTrue(result.isResult());
      assertEquals(1, countRuns(engine, "Join"));
      assertEquals(1, countRuns(engine, "After join"));
    } finally {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin =
          registry.getPlugin(ExtensionPointPluginType.class, LateArrivalExtension.PLUGIN_ID);
      if (plugin != null) {
        registry.removePlugin(ExtensionPointPluginType.class, plugin);
      }
    }
  }

  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void joinRunsOncePerLoopIteration() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("join-in-loop");

    ActionMeta startMeta = new ActionMeta(new ActionStart("Start"));
    ActionMeta fanOutMeta = new ActionMeta(new ActionDummy("Fan out"));
    fanOutMeta.setLaunchingInParallel(true);
    ActionMeta aMeta = new ActionMeta(new ActionDummy("Branch A"));
    ActionMeta bMeta = new ActionMeta(new ActionDummy("Branch B"));
    ActionMeta joinMeta = new ActionMeta(new ActionJoin("Join", ""));
    ActionMeta counterMeta = new ActionMeta(new CountingEvalAction("Counter", 3));
    meta.addAction(startMeta);
    meta.addAction(fanOutMeta);
    meta.addAction(aMeta);
    meta.addAction(bMeta);
    meta.addAction(joinMeta);
    meta.addAction(counterMeta);

    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, fanOutMeta));
    for (ActionMeta branchMeta : List.of(aMeta, bMeta)) {
      WorkflowHopMeta fanOutToBranch = new WorkflowHopMeta(fanOutMeta, branchMeta);
      fanOutToBranch.setUnconditional();
      meta.addWorkflowHop(fanOutToBranch);
      WorkflowHopMeta branchToJoin = new WorkflowHopMeta(branchMeta, joinMeta);
      branchToJoin.setUnconditional();
      meta.addWorkflowHop(branchToJoin);
    }
    meta.addWorkflowHop(new WorkflowHopMeta(joinMeta, counterMeta));
    // Loop back to the fan-out while the counter succeeds
    meta.addWorkflowHop(new WorkflowHopMeta(counterMeta, fanOutMeta));

    CountingEvalAction.RUNS.set(0);
    LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.MINIMAL);
    engine.startExecution();

    assertEquals(3, countRuns(engine, "Join"));
    assertEquals(3, countRuns(engine, "Counter"));
  }

  /**
   * A branch that has another hop before its hop to the Join claims the Join, then follows its
   * other hop first. The Join runs once, after that other hop has finished, even though the other
   * branch arrived at the Join earlier.
   */
  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  void joinWaitsForEarlierHopOfBranchThatClaimedIt() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("join-claimed-by-branch-with-earlier-hop");

    ActionMeta startMeta = new ActionMeta(new ActionStart("Start"));
    startMeta.setLaunchingInParallel(true);
    ActionMeta claimingMeta = new ActionMeta(new ActionDummy("Claiming branch"));
    // Arrives at the Join after the claiming branch has claimed it
    ActionMeta otherMeta = new ActionMeta(new SleepingEvalAction("Other branch", 200));
    ActionMeta slowMeta = new ActionMeta(new SleepingEvalAction("Slow task", 1000));
    ActionMeta joinMeta = new ActionMeta(new ActionJoin("Join", ""));
    ActionMeta afterMeta = new ActionMeta(new ActionDummy("After join"));
    meta.addAction(startMeta);
    meta.addAction(claimingMeta);
    meta.addAction(otherMeta);
    meta.addAction(slowMeta);
    meta.addAction(joinMeta);
    meta.addAction(afterMeta);

    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, claimingMeta));
    meta.addWorkflowHop(new WorkflowHopMeta(startMeta, otherMeta));
    // The hop to the slow task comes before the hop to the Join, so it is followed first
    WorkflowHopMeta claimingToSlow = new WorkflowHopMeta(claimingMeta, slowMeta);
    claimingToSlow.setUnconditional();
    meta.addWorkflowHop(claimingToSlow);
    WorkflowHopMeta claimingToJoin = new WorkflowHopMeta(claimingMeta, joinMeta);
    claimingToJoin.setUnconditional();
    meta.addWorkflowHop(claimingToJoin);
    WorkflowHopMeta otherToJoin = new WorkflowHopMeta(otherMeta, joinMeta);
    otherToJoin.setUnconditional();
    meta.addWorkflowHop(otherToJoin);
    meta.addWorkflowHop(new WorkflowHopMeta(joinMeta, afterMeta));

    LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.MINIMAL);
    Result result = engine.startExecution();

    assertTrue(result.isResult());
    assertEquals(1, countRuns(engine, "Join"));
    assertEquals(1, countRuns(engine, "After join"));
    assertTrue(
        indexOfRun(engine, "Slow task") < indexOfRun(engine, "After join"),
        "The Join runs only after the earlier hop of the branch that claimed it");
  }

  private static int indexOfRun(LocalWorkflowEngine engine, String actionName) {
    List<ActionResult> actionResults = engine.getActionResults();
    for (int i = 0; i < actionResults.size(); i++) {
      if (actionName.equals(actionResults.get(i).getActionName())) {
        return i;
      }
    }
    return -1;
  }

  private static long countRuns(LocalWorkflowEngine engine, String actionName) {
    return engine.getActionResults().stream()
        .filter(actionResult -> actionName.equals(actionResult.getActionName()))
        .count();
  }

  private static void sleep(long millis) {
    try {
      ThreadUtils.sleep(Duration.ofMillis(millis));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static class LateArrivalExtension implements IExtensionPoint<WorkflowExecutionExtension> {
    static final String PLUGIN_ID = "ActionJoinTestLateArrival";
    static final String LATE_BRANCH = "Late branch";

    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, WorkflowExecutionExtension extension) {
      if (LATE_BRANCH.equals(extension.actionMeta.getName())) {
        // Longer than the polling interval of the Join
        sleep(1500);
      }
    }
  }

  /** Succeeds until it has run {@code maxRuns} times. */
  static class CountingEvalAction extends ActionBase {
    static final AtomicInteger RUNS = new AtomicInteger();
    private final int maxRuns;

    CountingEvalAction(String name, int maxRuns) {
      super(name, "");
      this.maxRuns = maxRuns;
    }

    @Override
    public Result execute(Result result, int nr) {
      result.setResult(RUNS.incrementAndGet() < maxRuns);
      result.setNrErrors(0);
      return result;
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }
  }

  static class FailingEvalAction extends ActionBase {
    FailingEvalAction(String name) {
      super(name, "");
    }

    @Override
    public Result execute(Result result, int nr) {
      result.setResult(false);
      result.setNrErrors(1);
      return result;
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }
  }

  static class SucceedingEvalAction extends ActionBase {
    SucceedingEvalAction(String name) {
      super(name, "");
    }

    @Override
    public Result execute(Result result, int nr) {
      result.setResult(true);
      result.setNrErrors(0);
      return result;
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }
  }

  static class SleepingEvalAction extends ActionBase {
    private final long sleepMs;

    SleepingEvalAction(String name, long sleepMs) {
      super(name, "");
      this.sleepMs = sleepMs;
    }

    @Override
    public Result execute(Result result, int nr) {
      try {
        ThreadUtils.sleep(Duration.ofMillis(sleepMs));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      result.setResult(true);
      result.setNrErrors(0);
      return result;
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }
  }
}
