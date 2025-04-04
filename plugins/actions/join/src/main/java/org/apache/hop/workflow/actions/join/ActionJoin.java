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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
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

  @Override
  public Object clone() {
    return new ActionJoin(this);
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

      var workflowTracker = this.parentWorkflow.getWorkflowTracker();
      while (!parentWorkflow.isStopped()) {
        Thread.sleep(500L);
        boolean hasAllResult = true;
        for (ActionMeta actionMeta : prevActions) {
          var tracker = workflowTracker.findWorkflowTracker(actionMeta);
          if (tracker != null) {
            if (tracker.getActionResult().getResult() == null) {
              hasAllResult = false;
            }
          }
        }

        // If all previous action have a result
        if (hasAllResult) {
          break;
        }
      }
    } catch (Exception e) {
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
