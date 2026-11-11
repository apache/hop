/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lineage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Emits an action's {@code STARTED} lineage event once the workflow has cloned the action, so the
 * event carries the run id of the action that actually executes. One listener per workflow, added
 * by {@link #register(IWorkflowEngine)}.
 *
 * <p>The listener remembers the action runs it started. An action whose {@code execute()} throws
 * never reaches the {@code WorkflowAfterActionExecution} extension point that emits its {@code
 * FINISHED} or {@code FAILED}, so {@link #failUnfinishedActions(IWorkflowEngine)} closes those runs
 * as {@code FAILED} when the workflow ends.
 */
public final class LineageActionStartListener implements IActionListener<WorkflowMeta> {

  /** The action runs that started and have no terminal event yet, by run id. */
  private final Map<String, IAction> unfinishedActions = new ConcurrentHashMap<>();

  private LineageActionStartListener() {}

  /** Adds the listener to a workflow, unless it is already there (a workflow can run again). */
  @SuppressWarnings("unchecked")
  public static void register(IWorkflowEngine<WorkflowMeta> workflow) {
    if (workflow == null || workflow.getActionListeners() == null || find(workflow) != null) {
      return;
    }
    workflow.addActionListener(new LineageActionStartListener());
  }

  /** The action run ended with a {@code FINISHED} or {@code FAILED} event. */
  public static void actionEnded(IWorkflowEngine<WorkflowMeta> workflow, String actionRunId) {
    LineageActionStartListener listener = find(workflow);
    if (listener != null && !Utils.isEmpty(actionRunId)) {
      listener.unfinishedActions.remove(actionRunId);
    }
  }

  /**
   * Emits {@code FAILED} for every action run of this workflow that started but never ended: its
   * {@code execute()} threw, so the workflow skipped the extension point that ends it. Without this
   * a sink keeps those runs open forever.
   */
  public static void failUnfinishedActions(IWorkflowEngine<WorkflowMeta> workflow) {
    LineageActionStartListener listener = find(workflow);
    if (listener == null || listener.unfinishedActions.isEmpty()) {
      return;
    }
    List<IAction> unfinished = new ArrayList<>(listener.unfinishedActions.values());
    listener.unfinishedActions.clear();
    for (IAction action : unfinished) {
      LineageRunLifecycleEmitter.emitActionFailedWithoutResult(workflow, action);
    }
  }

  @SuppressWarnings("unchecked")
  private static LineageActionStartListener find(IWorkflowEngine<WorkflowMeta> workflow) {
    List<IActionListener> listeners = workflow != null ? workflow.getActionListeners() : null;
    if (listeners != null) {
      for (IActionListener listener : listeners) {
        if (listener instanceof LineageActionStartListener lineageListener) {
          return lineageListener;
        }
      }
    }
    return null;
  }

  /**
   * Runs inside the workflow's action execution, before the action's own error handling: a lineage
   * problem is logged and may never fail the action.
   */
  @Override
  public void beforeExecution(
      IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionMeta, IAction action) {
    try {
      LineageRunLifecycleEmitter.emitActionStarted(workflow, action);
      ILogChannel actionLog = action != null ? action.getLogChannel() : null;
      String runId = actionLog != null ? actionLog.getLogChannelId() : null;
      if (!Utils.isEmpty(runId)) {
        unfinishedActions.put(runId, action);
      }
    } catch (RuntimeException e) {
      ILogChannel log = workflow != null ? workflow.getLogChannel() : null;
      if (log != null) {
        log.logError(
            "Unable to emit the lineage start event of action "
                + (actionMeta != null ? actionMeta.getName() : null),
            e);
      }
    }
  }

  @Override
  public void afterExecution(
      IWorkflowEngine<WorkflowMeta> workflow,
      ActionMeta actionMeta,
      IAction action,
      Result result) {
    // FINISHED and FAILED come from the WorkflowAfterActionExecution extension point, which the
    // workflow reaches whenever the action returned a result. An action that threw is closed by
    // failUnfinishedActions() at the end of the workflow.
  }
}
