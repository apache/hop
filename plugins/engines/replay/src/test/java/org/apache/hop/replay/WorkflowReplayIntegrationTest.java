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

package org.apache.hop.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.replay.engine.ReplayWorkflowEngine;
import org.apache.hop.replay.engine.ReplayWorkflowRunConfiguration;
import org.apache.hop.replay.workflow.WorkflowReplayGateAfterActionXp;
import org.apache.hop.replay.workflow.WorkflowReplayGateBeforeActionXp;
import org.apache.hop.replay.workflow.WorkflowReplayGateFinishedXp;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class WorkflowReplayIntegrationTest {

  private static final List<String> REGISTERED = new ArrayList<>();

  @TempDir Path tempFolder;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
    ensureExtension(
        WorkflowReplayGateBeforeActionXp.class,
        "WorkflowReplayGateBeforeActionXp",
        HopExtensionPoint.WorkflowBeforeActionExecution.id);
    ensureExtension(
        WorkflowReplayGateAfterActionXp.class,
        "WorkflowReplayGateAfterActionXp",
        HopExtensionPoint.WorkflowAfterActionExecution.id);
    ensureExtension(
        WorkflowReplayGateFinishedXp.class,
        "WorkflowReplayGateFinishedXp",
        HopExtensionPoint.WorkflowFinish.id);
  }

  @AfterAll
  static void tearDownAfterClass() {
    PluginRegistry registry = PluginRegistry.getInstance();
    for (String id : REGISTERED) {
      IPlugin plugin = registry.getPlugin(ExtensionPointPluginType.class, id);
      if (plugin != null) {
        registry.removePlugin(ExtensionPointPluginType.class, plugin);
      }
    }
    REGISTERED.clear();
  }

  @Test
  void parallelBranchesSkipOnlyTheirOwnSealedActions() throws Exception {
    List<String> executed = new CopyOnWriteArrayList<>();
    String spool = spoolDir("parallel");
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("parallel-replay");

    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    start.setLaunchingInParallel(true);
    ActionMeta a1 = gated(executed, "A1", 0, null, null, spool);
    ActionMeta a2 = gated(executed, "A2", 1, null, null, null);
    ActionMeta b1 = gated(executed, "B1", 0, null, null, spool);
    ActionMeta b2 = gated(executed, "B2", 0, null, null, spool);
    meta.addAction(start);
    meta.addAction(a1);
    meta.addAction(a2);
    meta.addAction(b1);
    meta.addAction(b2);
    meta.addWorkflowHop(new WorkflowHopMeta(start, a1));
    meta.addWorkflowHop(new WorkflowHopMeta(a1, a2));
    meta.addWorkflowHop(new WorkflowHopMeta(start, b1));
    meta.addWorkflowHop(new WorkflowHopMeta(b1, b2));

    LocalWorkflowEngine local = runLocal(meta);
    assertFalse(local.getResult().isResult());
    assertEquals(Set.of("A1", "A2", "B1", "B2"), new HashSet<>(executed));

    executed.clear();
    ReplayWorkflowEngine replay = runReplay(meta, local.getLogChannelId(), null);
    assertTrue(replay.getResult().isResult());
    assertEquals(Set.of("A2"), new HashSet<>(executed));
    assertTrue(restored(replay, "A1"));
    assertTrue(restored(replay, "B1"));
    assertTrue(restored(replay, "B2"));
    assertFalse(restored(replay, "A2"));
  }

  @Test
  void startActionRunsWhileEarlierSealedActionsStaySkipped() throws Exception {
    List<String> executed = new CopyOnWriteArrayList<>();
    String spool = spoolDir("start-sealed");
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("start-sealed");

    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    ActionMeta first = gated(executed, "First", 0, "FROM_GATE", "kept", spool);
    ActionMeta second = gated(executed, "Second", 0, null, null, spool);
    ActionMeta resume = gated(executed, "Resume", 1, null, null, spool);
    ((TracingAction) resume.getAction()).requiredVariable = "FROM_GATE";
    ((TracingAction) resume.getAction()).requiredValue = "kept";
    link(meta, start, first, second, resume);

    LocalWorkflowEngine local = runLocal(meta);
    assertFalse(local.getResult().isResult());
    assertEquals(List.of("First", "Second", "Resume"), executed);

    executed.clear();
    ReplayWorkflowEngine replay = runReplay(meta, local.getLogChannelId(), "Resume");
    assertTrue(replay.getResult().isResult());
    assertEquals(List.of("Resume"), executed);
    assertTrue(restored(replay, "First"));
    assertTrue(restored(replay, "Second"));
    assertEquals("kept", replay.getVariable("FROM_GATE"));
  }

  @Test
  void ungatedActionBeforeTheStartActionRunsTheRestOfThatPath() throws Exception {
    List<String> executed = new CopyOnWriteArrayList<>();
    String spool = spoolDir("start-ungated");
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("start-ungated");

    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    ActionMeta truncate = gated(executed, "Truncate", 0, null, null, null);
    ActionMeta load = gated(executed, "Load", 0, null, null, spool);
    ActionMeta resume = gated(executed, "Resume", 1, null, null, null);
    link(meta, start, truncate, load, resume);

    LocalWorkflowEngine local = runLocal(meta);
    assertFalse(local.getResult().isResult());

    executed.clear();
    ReplayWorkflowEngine replay = runReplay(meta, local.getLogChannelId(), "Resume");
    assertTrue(replay.getResult().isResult());
    assertEquals(List.of("Truncate", "Load", "Resume"), executed);
    assertFalse(restored(replay, "Load"));
  }

  @Test
  void successfulReplayDropsTheGatesSoTheNextReplayRunsEverything() throws Exception {
    List<String> executed = new CopyOnWriteArrayList<>();
    String spool = spoolDir("invalidate");
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("invalidate");

    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    ActionMeta first = gated(executed, "First", 0, null, null, spool);
    ActionMeta resume = gated(executed, "Resume", 1, null, null, null);
    link(meta, start, first, resume);

    LocalWorkflowEngine local = runLocal(meta);
    executed.clear();
    ReplayWorkflowEngine replay = runReplay(meta, local.getLogChannelId(), null);
    assertTrue(replay.getResult().isResult());
    assertEquals(List.of("Resume"), executed);
    assertFalse(
        HopVfs.getFileObject(
                ReplayGateUtil.getExecutionSpoolPath(
                    local,
                    ReplayGateUtil.getReplayGate(first),
                    "invalidate",
                    local.getLogChannelId()),
                local)
            .exists());

    executed.clear();
    ReplayWorkflowEngine again = runReplay(meta, local.getLogChannelId(), null);
    assertTrue(again.getResult().isResult());
    assertEquals(List.of("First", "Resume"), executed);
  }

  private String spoolDir(String name) {
    return tempFolder.resolve(name).toUri().toString();
  }

  private static void link(WorkflowMeta meta, ActionMeta... actions) {
    for (ActionMeta action : actions) {
      meta.addAction(action);
    }
    for (int i = 0; i < actions.length - 1; i++) {
      meta.addWorkflowHop(new WorkflowHopMeta(actions[i], actions[i + 1]));
    }
  }

  private static ActionMeta gated(
      List<String> executed,
      String name,
      int failures,
      String variableName,
      String variableValue,
      String spoolDir) {
    TracingAction action = new TracingAction(executed, name, failures, variableName, variableValue);
    ActionMeta actionMeta = new ActionMeta(action);
    if (spoolDir != null) {
      ReplayGate gate = new ReplayGate();
      gate.setEnabled(true);
      gate.setSpoolDirectory(spoolDir);
      ReplayGateUtil.storeReplayGate(actionMeta, gate);
    }
    return actionMeta;
  }

  private static LocalWorkflowEngine runLocal(WorkflowMeta meta) throws HopException {
    LocalWorkflowEngine engine = new LocalWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.ERROR);
    Result result = engine.startExecution();
    assertNotFailedToStart(result);
    return engine;
  }

  private static ReplayWorkflowEngine runReplay(
      WorkflowMeta meta, String sourceExecutionId, String startActionName) throws HopException {
    ReplayWorkflowEngine engine = new ReplayWorkflowEngine(meta);
    engine.setLogLevel(LogLevel.ERROR);
    ReplayWorkflowRunConfiguration config = new ReplayWorkflowRunConfiguration();
    config.setSourceExecutionId(sourceExecutionId);
    config.setStartActionName(startActionName);
    engine.setWorkflowRunConfiguration(
        new WorkflowRunConfiguration("replay", "", "", config, false));
    Result result = engine.startExecution();
    assertNotFailedToStart(result);
    return engine;
  }

  private static void assertNotFailedToStart(Result result) {
    assertTrue(result != null, "workflow returned no result");
  }

  private static boolean restored(IWorkflowEngine<WorkflowMeta> workflow, String actionName) {
    for (ActionResult actionResult : workflow.getActionResults()) {
      if (actionName.equals(actionResult.getActionName()) && actionResult.isCheckpoint()) {
        return true;
      }
    }
    return false;
  }

  private static void ensureExtension(Class<?> clazz, String id, String extensionPointId)
      throws HopPluginException {
    PluginRegistry registry = PluginRegistry.getInstance();
    if (registry.getPlugin(ExtensionPointPluginType.class, id) != null) {
      return;
    }
    ExtensionPointPluginType.getInstance()
        .registerCustom(clazz, "replay-test", id, extensionPointId, id, null);
    REGISTERED.add(id);
  }

  public static class TracingAction extends ActionBase implements IAction {
    private final List<String> executed;
    private final AtomicInteger failuresLeft;
    private final String variableName;
    private final String variableValue;
    private String requiredVariable;
    private String requiredValue;

    public TracingAction(
        List<String> executed,
        String name,
        int failures,
        String variableName,
        String variableValue) {
      super(name, "");
      this.executed = executed;
      this.failuresLeft = new AtomicInteger(failures);
      this.variableName = variableName;
      this.variableValue = variableValue;
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }

    @Override
    public Result execute(Result prevResult, int nr) throws HopException {
      executed.add(getName());
      if (variableName != null) {
        getParentWorkflow().setVariable(variableName, variableValue);
      }
      Result result = new Result();
      if (requiredVariable != null
          && !requiredValue.equals(getParentWorkflow().getVariable(requiredVariable))) {
        result.setResult(false);
        result.setNrErrors(1);
        return result;
      }
      if (failuresLeft.getAndDecrement() > 0) {
        result.setResult(false);
        result.setNrErrors(1);
        return result;
      }
      result.setResult(true);
      result.setNrErrors(0);
      return result;
    }
  }
}
