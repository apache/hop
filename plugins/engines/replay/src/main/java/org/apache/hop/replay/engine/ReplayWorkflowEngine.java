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

package org.apache.hop.replay.engine;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Result;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePlugin;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;

@WorkflowEnginePlugin(
    id = "Replay",
    name = "Hop replay workflow engine",
    description =
        "Skips workflow actions whose sealed replay gates still match this execution and runs the rest")
public class ReplayWorkflowEngine extends LocalWorkflowEngine
    implements IWorkflowEngine<WorkflowMeta> {

  private static final Class<?> PKG = ReplayWorkflowEngine.class;

  /**
   * True on this execution path once an action has to run. Threads started afterwards inherit the
   * value, so a parallel branch keeps its own decision.
   */
  private final InheritableThreadLocal<Boolean> liveExecution =
      new InheritableThreadLocal<>() {
        @Override
        protected Boolean initialValue() {
          return Boolean.FALSE;
        }
      };

  @Getter @Setter private boolean replaying = true;

  public ReplayWorkflowEngine() {
    super();
  }

  public ReplayWorkflowEngine(WorkflowMeta workflowMeta) {
    super(workflowMeta);
  }

  public ReplayWorkflowEngine(WorkflowMeta workflowMeta, ILoggingObject parent) {
    super(workflowMeta, parent);
  }

  @Override
  public IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration() {
    return new ReplayWorkflowRunConfiguration();
  }

  public ReplayWorkflowRunConfiguration getReplayWorkflowRunConfiguration() {
    if (workflowRunConfiguration != null
        && workflowRunConfiguration.getEngineRunConfiguration()
            instanceof ReplayWorkflowRunConfiguration replayConfig) {
      return replayConfig;
    }
    return null;
  }

  public boolean isLiveExecution() {
    return Boolean.TRUE.equals(liveExecution.get());
  }

  public void markLiveExecution() {
    liveExecution.set(Boolean.TRUE);
  }

  public void recordRestoredAction(ActionMeta actionMeta, Result result) {
    if (actionMeta == null) {
      return;
    }
    String filename = null;
    if (actionMeta.getAction() != null) {
      filename = resolve(actionMeta.getAction().getFilename());
    }
    ActionResult actionResult =
        new ActionResult(
            result,
            getLogChannelId(),
            BaseMessages.getString(PKG, "ReplayWorkflowEngine.Restored.Comment"),
            null,
            actionMeta.getName(),
            filename);
    actionResult.setCheckpoint(true);
    getWorkflowTracker().addWorkflowTracker(new WorkflowTracker<>(getWorkflowMeta(), actionResult));
    synchronized (actionResults) {
      actionResults.add(actionResult);
    }
  }

  @Override
  public Result startExecution() {
    liveExecution.set(Boolean.FALSE);
    this.replaying = true;
    return super.startExecution();
  }
}
