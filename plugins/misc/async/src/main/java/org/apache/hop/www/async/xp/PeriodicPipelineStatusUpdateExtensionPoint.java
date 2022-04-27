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

package org.apache.hop.www.async.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.server.HttpUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.HopServerPipelineStatus;
import org.apache.hop.www.async.AsyncWebService;
import org.apache.hop.www.async.Defaults;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

@ExtensionPoint(
    id = "PeriodicPipelineStatusUpdateExtensionPoint",
    extensionPointId = "PipelinePrepareExecution",
    description =
        "A pipeline is being prepared for execution.  If the parent is a pipeline action and a service is set: monitor")
public class PeriodicPipelineStatusUpdateExtensionPoint
    implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {
  @Override
  public void callExtensionPoint(
      ILogChannel iLogChannel, IVariables iVariables, final IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {

    // If the parent is not an action we're not interested
    //
    if (!(pipeline.getParent() instanceof IAction)) {
      return;
    }

    // Only pipeline actions are interesting to us
    //
    IAction action = (IAction) pipeline.getParent();
    if (!action.isPipeline()) {
      return;
    }

    // The service for this action is stored in the workflow runtime...
    //
    IWorkflowEngine<WorkflowMeta> workflow = action.getParentWorkflow();
    Map<String, Object> extensionDataMap = workflow.getExtensionDataMap();

    // The service is stored under the name of the action...
    //
    AsyncWebService service =
        (AsyncWebService)
            extensionDataMap.get(Defaults.createWorkflowExtensionDataKey(action.getName()));
    if (service == null) {
      return;
    }

    final IWorkflowEngine<WorkflowMeta> grandParentWorkflow = getGrandParentWorkflow(pipeline);
    if (grandParentWorkflow == null) {
      pipeline
          .getLogChannel()
          .logError(
              "WARNING: the grand parent workflow for pipeline '"
                  + pipeline.getObjectName()
                  + "' couldn't be found");
      return;
    }

    TimerTask timerTask =
        new TimerTask() {
          @Override
          public void run() {
            try {
              updatePipelineStatus(pipeline, service, grandParentWorkflow);
            } catch (IOException e) {
              pipeline.getLogChannel().logError("Error updating async service pipeline status", e);
            }
          }
        };

    final Timer timer = new Timer();

    // Update the status this every 1s
    //
    timer.schedule(timerTask, 0, 1000L);

    // When the pipeline is done, cancel the timer and update the status one last time...
    //
    pipeline.addExecutionFinishedListener(
        engine -> {
          timerTask.cancel();
          timer.cancel();
          try {
            updatePipelineStatus(engine, service, grandParentWorkflow);
          } catch (IOException e) {
            engine.getLogChannel().logError("Error updating async service pipeline status", e);
          }
        });
  }

  private void updatePipelineStatus(
      IPipelineEngine<PipelineMeta> pipeline,
      AsyncWebService service,
      IWorkflowEngine<WorkflowMeta> grandParentWorkflow)
      throws IOException {
    // Get the status...
    //
    HopServerPipelineStatus status = getPipelineStatus(pipeline);

    // Store the status in the grandparent workflow...
    //
    String key =
        Defaults.createWorkflowPipelineStatusExtensionDataKey(
            pipeline.getObjectName(), service.getName());
    grandParentWorkflow.getExtensionDataMap().put(key, status);
  }

  private IWorkflowEngine<WorkflowMeta> getGrandParentWorkflow(
      IPipelineEngine<PipelineMeta> pipeline) {
    if (pipeline.getParentWorkflow() != null) {
      return getGrandParentWorkflow(pipeline.getParentWorkflow());
    }
    if (pipeline.getParentPipeline() != null) {
      return getGrandParentWorkflow(pipeline.getParentPipeline());
    }
    return null;
  }

  private IWorkflowEngine<WorkflowMeta> getGrandParentWorkflow(
      IWorkflowEngine<WorkflowMeta> workflow) {
    if (workflow.getParentWorkflow() != null) {
      return getGrandParentWorkflow(workflow.getParentWorkflow());
    }
    if (workflow.getParentPipeline() != null) {
      return getGrandParentWorkflow(workflow.getParentPipeline());
    }

    // No parent? That means this is the grand-parent
    return workflow;
  }

  private HopServerPipelineStatus getPipelineStatus(IPipelineEngine<PipelineMeta> pipeline)
      throws IOException {
    HopServerPipelineStatus pipelineStatus =
        new HopServerPipelineStatus(
            pipeline.getPipelineMeta().getName(),
            pipeline.getContainerId(),
            pipeline.getStatusDescription());

    String logText =
        HopLogStore.getAppender()
            .getBuffer(pipeline.getLogChannel().getLogChannelId(), false)
            .toString();

    pipelineStatus.setFirstLoggingLineNr(-1);
    pipelineStatus.setLastLoggingLineNr(-1);
    pipelineStatus.setLogDate(new Date());
    pipelineStatus.setExecutionStartDate(pipeline.getExecutionStartDate());
    pipelineStatus.setExecutionEndDate(pipeline.getExecutionEndDate());

    for (IEngineComponent component : pipeline.getComponents()) {
      if ((component.isRunning())
          || (component.getStatus() != EngineComponent.ComponentExecutionStatus.STATUS_EMPTY)) {
        TransformStatus transformStatus = new TransformStatus(component);
        pipelineStatus.getTransformStatusList().add(transformStatus);
      }
    }

    // The log can be quite large at times, we are going to putIfAbsent a base64 encoding
    // around a compressed
    // stream
    // of bytes to handle this one.
    //
    String loggingString = HttpUtil.encodeBase64ZippedString(logText);
    pipelineStatus.setLoggingString(loggingString);

    // Also set the result object...
    // Ignore the result for now
    // Causes issues with result files and concurrent hashmaps
    //

    // Is the pipeline paused?
    //
    pipelineStatus.setPaused(pipeline.isPaused());

    return pipelineStatus;
  }
}
