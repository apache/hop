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

import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Emits an action's {@code STARTED} lineage event once the workflow has cloned the action, so the
 * event carries the run id of the action that actually executes. Registered once per workflow run
 * by {@link #register(IWorkflowEngine)}.
 */
public final class LineageActionStartListener implements IActionListener<WorkflowMeta> {

  private static final LineageActionStartListener INSTANCE = new LineageActionStartListener();

  private LineageActionStartListener() {}

  /** Adds the listener to a workflow, unless it is already there (a workflow can run again). */
  @SuppressWarnings("unchecked")
  public static void register(IWorkflowEngine<WorkflowMeta> workflow) {
    if (workflow == null) {
      return;
    }
    List<IActionListener> listeners = workflow.getActionListeners();
    if (listeners == null || listeners.contains(INSTANCE)) {
      return;
    }
    workflow.addActionListener(INSTANCE);
  }

  @Override
  public void beforeExecution(
      IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionMeta, IAction action) {
    LineageRunLifecycleEmitter.emitActionStarted(workflow, action);
  }

  @Override
  public void afterExecution(
      IWorkflowEngine<WorkflowMeta> workflow,
      ActionMeta actionMeta,
      IAction action,
      Result result) {
    // FINISHED and FAILED come from the WorkflowAfterActionExecution extension point, which also
    // sees an action that failed before it returned a result.
  }
}
