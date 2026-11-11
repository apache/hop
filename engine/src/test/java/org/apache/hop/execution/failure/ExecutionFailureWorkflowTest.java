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

package org.apache.hop.execution.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.failure.ExecutionFailure;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.core.failure.FailureComponent;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/** Failures of actions and workflows, run on the local workflow engine. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ExecutionFailureWorkflowTest {

  enum Mode {
    SUCCEED,
    THROWS,
    ERRORS,
    CHILD_FAILED,
    FALSE_WITHOUT_ERRORS,
    PASS_ERRORS_ON
  }

  /** The failure a child pipeline would have left in the result. */
  static ExecutionFailure childFailure;

  @Action(id = "ExecutionFailureTestAction", name = "Execution failure test action")
  public static class FailingAction extends ActionBase implements IAction {
    Mode mode = Mode.SUCCEED;

    public FailingAction() {
      super("failing", "", "ExecutionFailureTestAction");
    }

    /** Its result picks the hop to follow, so a failure can be handled. */
    @Override
    public boolean isEvaluation() {
      return true;
    }

    @Override
    public Result execute(Result prevResult, int nr) throws HopException {
      switch (mode) {
        case THROWS:
          throw new HopException(
              "Unable to log in", new SQLException("Password authentication failed", "28P01"));
        case ERRORS:
          logError("Unable to write the file: No space left on device");
          prevResult.setResult(false);
          prevResult.setNrErrors(1);
          return prevResult;
        case CHILD_FAILED:
          Result child = new Result();
          child.setNrErrors(1);
          child.getFailures().add(childFailure);
          prevResult.add(child);
          prevResult.setResult(false);
          return prevResult;
        case PASS_ERRORS_ON:
          // Like Write to log: a true result, the error count left as it was
          prevResult.setResult(true);
          return prevResult;
        case FALSE_WITHOUT_ERRORS:
          prevResult.setResult(false);
          prevResult.setNrErrors(0);
          return prevResult;
        default:
          prevResult.setResult(true);
          prevResult.setNrErrors(0);
          return prevResult;
      }
    }
  }

  @BeforeAll
  static void beforeAll() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(FailingAction.class.getName(), ActionPluginType.class, Action.class);
  }

  private final List<ExecutionFailure> announced = new CopyOnWriteArrayList<>();

  /** START, then the failing action, then on failure a Dummy when handled is true. */
  private LocalWorkflowEngine workflowWith(Mode mode, boolean handled) {
    return workflowWith(mode, handled ? Mode.SUCCEED : null);
  }

  /** START, then the failing action, then on failure an action in the handler mode, if any. */
  private LocalWorkflowEngine workflowWith(Mode mode, Mode handlerMode) {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("failure-test");
    ActionMeta start = new ActionMeta(new ActionStart("START"));
    meta.addAction(start);
    FailingAction failing = new FailingAction();
    failing.mode = mode;
    ActionMeta failingMeta = new ActionMeta(failing);
    meta.addAction(failingMeta);
    meta.addWorkflowHop(new WorkflowHopMeta(start, failingMeta));
    if (handlerMode == Mode.SUCCEED) {
      ActionDummy dummy = new ActionDummy();
      dummy.setName("handle");
      addOnFailure(meta, failingMeta, new ActionMeta(dummy));
    } else if (handlerMode != null) {
      FailingAction handler = new FailingAction();
      handler.setName("handle");
      handler.mode = handlerMode;
      addOnFailure(meta, failingMeta, new ActionMeta(handler));
    }
    LocalWorkflowEngine workflow = new LocalWorkflowEngine(meta);
    workflow.addExecutionFailureListener(announced::add);
    return workflow;
  }

  private static void addOnFailure(WorkflowMeta meta, ActionMeta from, ActionMeta to) {
    meta.addAction(to);
    WorkflowHopMeta onFailure = new WorkflowHopMeta(from, to);
    onFailure.setEvaluation(false);
    meta.addWorkflowHop(onFailure);
  }

  @Test
  void succeedingWorkflowReportsNothing() {
    LocalWorkflowEngine workflow = workflowWith(Mode.SUCCEED, false);
    Result result = workflow.startExecution();

    assertTrue(result.isResult());
    assertNull(workflow.getRunFailure());
    assertTrue(workflow.getFailures().isEmpty());
    assertTrue(result.getFailures().isEmpty());
    assertTrue(announced.isEmpty());
  }

  @Test
  void actionThatThrows() {
    LocalWorkflowEngine workflow = workflowWith(Mode.THROWS, false);
    Result result = workflow.startExecution();

    assertFalse(result.isResult());
    assertEquals(1, workflow.getFailures().size());
    ExecutionFailure actionFailure = workflow.getFailures().get(0);
    FailureComponent component = actionFailure.getComponent();
    assertEquals(FailureComponent.KIND_ACTION, component.getKind());
    assertEquals("failing", component.getName());
    assertEquals("ExecutionFailureTestAction", component.getPluginId());
    assertEquals(FailureCategory.AUTHENTICATION, actionFailure.getCategory());
    assertEquals("28P01", actionFailure.getCause().getSqlState());
    assertEquals("failure-test", actionFailure.getRun().getName());

    ExecutionFailure runFailure = workflow.getRunFailure();
    assertNotNull(runFailure);
    assertSame(actionFailure, runFailure.getCausedBy());
    assertEquals(FailureCategory.AUTHENTICATION, runFailure.getCategory());
    assertEquals(List.of(actionFailure, runFailure), announced);
    assertEquals(List.of(runFailure), result.getFailures());
  }

  @Test
  void actionWhoseChildFailed() {
    childFailure = new ExecutionFailure();
    childFailure.setCategory(FailureCategory.SCHEMA_MISMATCH);
    try {
      LocalWorkflowEngine workflow = workflowWith(Mode.CHILD_FAILED, false);
      workflow.startExecution();

      ExecutionFailure actionFailure = workflow.getFailures().get(0);
      assertSame(childFailure, actionFailure.getCausedBy());
      assertEquals(FailureCategory.SCHEMA_MISMATCH, actionFailure.getCategory());
      assertNull(actionFailure.getCause());

      ExecutionFailure runFailure = workflow.getRunFailure();
      assertSame(childFailure, runFailure.getRootCause());
      assertEquals(FailureCategory.SCHEMA_MISMATCH, runFailure.getCategory());
    } finally {
      childFailure = null;
    }
  }

  @Test
  void handledFailureIsAnActionFailureOnly() {
    LocalWorkflowEngine workflow = workflowWith(Mode.ERRORS, true);
    Result result = workflow.startExecution();

    assertTrue(result.isResult());
    assertEquals(1, workflow.getFailures().size());
    ExecutionFailure actionFailure = workflow.getFailures().get(0);
    assertTrue(actionFailure.getCause().isFromLog());
    assertEquals(FailureCategory.RESOURCE, actionFailure.getCategory());
    assertNull(workflow.getRunFailure());
    assertTrue(result.getFailures().isEmpty());
    assertEquals(workflow.getFailures(), announced);
  }

  @Test
  void anActionThatPassesErrorsOnDidNotFail() {
    LocalWorkflowEngine workflow = workflowWith(Mode.ERRORS, Mode.PASS_ERRORS_ON);
    workflow.startExecution();

    assertEquals(1, workflow.getFailures().size());
    assertEquals("failing", workflow.getFailures().get(0).getComponent().getName());
  }

  @Test
  void falseResultWithoutErrorsIsAWorkflowFailureOnly() {
    LocalWorkflowEngine workflow = workflowWith(Mode.FALSE_WITHOUT_ERRORS, false);
    Result result = workflow.startExecution();

    assertFalse(result.isResult());
    assertTrue(workflow.getFailures().isEmpty());
    ExecutionFailure runFailure = workflow.getRunFailure();
    assertNotNull(runFailure);
    assertNull(runFailure.getCausedBy());
    assertEquals(FailureCategory.UNKNOWN, runFailure.getCategory());
  }
}
