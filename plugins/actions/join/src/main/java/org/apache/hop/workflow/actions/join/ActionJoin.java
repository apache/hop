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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

/** Action type to join parallel execution of a workflow. */
@Action(
    id = "JOIN",
    name = "i18n::ActionJoin.Name",
    description = "i18n::ActionJoin.Description",
    image = "join.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    keywords = "i18n::ActionJoin.Keyword",
    documentationUrl = "/workflow/actions/join.html")
public class ActionJoin extends ActionBase {
  private static final Class<?> PKG = ActionJoin.class;

  public ActionJoin(String name, String description) {
    super(name, description);
  }

  public ActionJoin() {
    this("", "");
  }

  public ActionJoin(ActionJoin other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param result The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result result, int nr) {
    try {
      // Find previous actions to join
      List<ActionMeta> prevActions = getPreviousAction(this, new ArrayList<>(), false);

      while (!parentWorkflow.isStopped()) {
        ThreadUtils.sleep(Duration.ofMillis(500L));
        boolean completed = true;
        boolean success = true;
        int errors = 0;

        // Checks if all previous actions have completed, or can never run
        for (ActionMeta actionMeta : prevActions) {
          Result actionResult = getFinishedActionResult(actionMeta);
          if (actionResult != null) {
            if (!actionResult.isResult()) {
              WorkflowHopMeta hopMeta = findWorkflowHop(actionMeta);
              // If one previous action has failure and the hop is true evaluation, repeat failure
              // to the join action
              if (hopMeta != null && !hopMeta.isUnconditional() && hopMeta.isEvaluation()) {
                success = false;
                errors++;
              }
            }
          } else if (willNeverExecute(actionMeta, new HashSet<>())) {
            // Predecessor was skipped because an upstream hop was not followed (for example a
            // failed action with only a success hop toward this branch). Do not wait forever.
            if (isUnreachableBecauseOfFailure(actionMeta, new HashSet<>())) {
              success = false;
              errors++;
            }
            if (isBasic()) {
              logBasic(
                  BaseMessages.getString(
                      PKG, "ActionJoin.Log.PredecessorUnreachable", actionMeta.getName()));
            }
          } else {
            completed = false;
          }
        }

        // If all previous actions have a result or can never execute
        if (completed) {
          result.setResult(success);
          result.setNrErrors(errors);
          break;
        }
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      result.setNrErrors(1);
      result.setResult(false);
      logError(BaseMessages.getString(PKG, "ActionJoin.Error.CouldNotExecute") + e);
    }

    return result;
  }

  @Override
  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous action.
    return false;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isJoin() {
    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    List<ActionMeta> prevActions = getPreviousAction(this, new ArrayList<>(), true);

    boolean isLaunchingInParallel = false;
    for (ActionMeta actionMeta : prevActions) {
      isLaunchingInParallel |= actionMeta.isLaunchingInParallel();
    }

    if (!isLaunchingInParallel) {
      String message = BaseMessages.getString(PKG, "ActionJoin.CheckResult.NoParallelExecution");
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, this));
    }
  }

  /**
   * Finds a workflow hop from the specified action and to this action.
   *
   * @param from the starting action for the workflow hop to be found
   * @return the {@code WorkflowHopMeta} object representing the hop from the specified starting
   *     action to this action, or {@code null} if no such hop exists
   */
  public WorkflowHopMeta findWorkflowHop(ActionMeta from) {
    for (WorkflowHopMeta hop : this.parentWorkflowMeta.getWorkflowHops()) {
      if (hop.getFromAction() != null
          && hop.getToAction() != null
          && hop.getFromAction().equals(from)
          && hop.getToAction().getAction().equals(this)) {
        return hop;
      }
    }
    return null;
  }

  /**
   * Result of a finished action, or {@code null} if it has not started or is still running. Matches
   * {@link org.apache.hop.workflow.Workflow} hop following: a tracker with a null result is the
   * "started" marker, not a completed execution.
   */
  private Result getFinishedActionResult(ActionMeta actionMeta) {
    WorkflowTracker<?> tracker =
        parentWorkflow.getWorkflowTracker().findWorkflowTracker(actionMeta);
    if (tracker == null) {
      return null;
    }

    ActionResult actionResult = tracker.getActionResult();
    if (actionResult == null) {
      return null;
    }
    return actionResult.getResult();
  }

  /**
   * Same condition as {@link org.apache.hop.workflow.Workflow} when deciding whether to execute the
   * next action after {@code fromAction} finished with {@code fromResult}.
   */
  private static boolean isHopFollowed(
      WorkflowHopMeta hop, ActionMeta fromAction, Result fromResult) {
    return hop.isUnconditional()
        || (fromAction.isEvaluation() && hop.isEvaluation() == fromResult.isResult());
  }

  /**
   * True when {@code actionMeta} has not started and every enabled incoming hop is dead: the
   * previous action finished without following the hop, or that previous action itself will never
   * execute. Conservatively returns false if a predecessor is still running or is about to start.
   */
  private boolean willNeverExecute(ActionMeta actionMeta, Set<ActionMeta> visiting) {
    if (getFinishedActionResult(actionMeta) != null) {
      return false;
    }
    if (parentWorkflow.getWorkflowTracker().findWorkflowTracker(actionMeta) != null
        || actionMeta.isStart()) {
      return false;
    }
    if (!visiting.add(actionMeta)) {
      return false;
    }

    List<WorkflowHopMeta> incoming = findIncomingHops(actionMeta);
    if (incoming.isEmpty()) {
      return true;
    }

    for (WorkflowHopMeta hop : incoming) {
      ActionMeta fromAction = hop.getFromAction();
      if (fromAction == null) {
        continue;
      }

      Result fromResult = getFinishedActionResult(fromAction);
      if (fromResult != null) {
        if (isHopFollowed(hop, fromAction, fromResult)) {
          return false;
        }
      } else if (!willNeverExecute(fromAction, visiting)) {
        return false;
      }
    }
    return true;
  }

  /**
   * True when the action is unreachable because a success hop was not followed after a failure.
   * False when it is unreachable because a failure hop was not followed after a success, so Join
   * should not fail.
   */
  private boolean isUnreachableBecauseOfFailure(ActionMeta actionMeta, Set<ActionMeta> visiting) {
    if (!visiting.add(actionMeta)) {
      return false;
    }
    for (WorkflowHopMeta hop : findIncomingHops(actionMeta)) {
      ActionMeta fromAction = hop.getFromAction();
      if (fromAction == null) {
        continue;
      }
      Result fromResult = getFinishedActionResult(fromAction);
      if (fromResult != null) {
        if (!isHopFollowed(hop, fromAction, fromResult)
            && !fromResult.isResult()
            && !hop.isUnconditional()
            && hop.isEvaluation()) {
          return true;
        }
      } else if (willNeverExecute(fromAction, new HashSet<>())
          && isUnreachableBecauseOfFailure(fromAction, visiting)) {
        return true;
      }
    }
    return false;
  }

  private List<WorkflowHopMeta> findIncomingHops(ActionMeta toAction) {
    List<WorkflowHopMeta> incoming = new ArrayList<>();
    if (parentWorkflowMeta == null) {
      return incoming;
    }

    for (WorkflowHopMeta hop : parentWorkflowMeta.getWorkflowHops()) {
      if (hop.isEnabled() && hop.getToAction() != null && hop.getToAction().equals(toAction)) {
        incoming.add(hop);
      }
    }
    return incoming;
  }

  /** Find previous actions */
  private List<ActionMeta> getPreviousAction(
      IAction action, List<ActionMeta> prevActions, boolean deep) {

    List<WorkflowHopMeta> hops = this.parentWorkflowMeta.getWorkflowHops();
    for (WorkflowHopMeta hop : hops) {
      if (hop.isEnabled() && hop.getToAction().getName().equals(action.getName())) {
        ActionMeta actionMeta = hop.getFromAction();
        prevActions.add(actionMeta);

        if (deep && !actionMeta.isJoin()) {
          getPreviousAction(actionMeta.getAction(), prevActions, true);
        }
      }
    }

    return prevActions;
  }
}
