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

package org.apache.hop.reflection.workflow.xp;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.reflection.pipeline.xp.PipelineStartLoggingXp;
import org.apache.hop.reflection.workflow.meta.WorkflowLog;
import org.apache.hop.reflection.workflow.transform.WorkflowLogging;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

@ExtensionPoint(
    id = "WorkflowStartLoggingXp",
    extensionPointId = "WorkflowStart",
    description = "At the start of a workflow, handle any Workflow Log metadata objects")
public class WorkflowStartLoggingXp implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow)
      throws HopException {

    IHopMetadataProvider metadataProvider = workflow.getMetadataProvider();
    IHopMetadataSerializer<WorkflowLog> serializer =
        metadataProvider.getSerializer(WorkflowLog.class);
    List<WorkflowLog> workflowLogs = serializer.loadAll();

    for (WorkflowLog workflowLog : workflowLogs) {
      handleWorkflowLog(log, workflowLog, workflow, variables);
    }
  }

  private void handleWorkflowLog(
      final ILogChannel log,
      final WorkflowLog workflowLog,
      final IWorkflowEngine<WorkflowMeta> workflow,
      final IVariables variables)
      throws HopException {

    // See if we need to do anything at all...
    //
    if (!workflowLog.isEnabled()) {
      return;
    }

    // If we log parent (root) workflows only we don't want a parent
    //
    if (workflowLog.isLoggingParentsOnly()
        && (workflow.getParentPipeline() != null || workflow.getParentWorkflow() != null)) {
      return;
    }

    // Load the pipeline filename specified in the Workflow Log object...
    //
    final String loggingPipelineFilename = variables.resolve(workflowLog.getPipelineFilename());

    // See if the file exists...
    FileObject loggingFileObject = HopVfs.getFileObject(loggingPipelineFilename);
    try {
      if (!loggingFileObject.exists()) {
        log.logBasic(
            "WARNING: The Workflow Log pipeline file '"
                + loggingPipelineFilename
                + "' couldn't be found to execute.");
        return;
      }
    } catch (Exception e) {
      workflow.stopExecution();
      throw new HopException(
          "Error handling Workflow Log metadata object '"
              + workflowLog.getName()
              + "' at the start of pipeline: "
              + workflow,
          e);
    }

    if (workflowLog.getWorkflowToLog().isEmpty()) {
      logWorkflow(workflowLog, workflow, variables, loggingPipelineFilename);
    } else {
      for (String workflowToLog : workflowLog.getWorkflowToLog()) {
        String workflowUri = HopVfs.getFileObject(workflow.getFilename()).getPublicURIString();
        String workflowToLogUri =
            HopVfs.getFileObject(variables.resolve(workflowToLog)).getPublicURIString();
        if (workflowUri.equals(workflowToLogUri)) {
          logWorkflow(workflowLog, workflow, variables, loggingPipelineFilename);
        }
      }
    }
  }

  private void logWorkflow(
      WorkflowLog workflowLog,
      IWorkflowEngine<WorkflowMeta> workflow,
      IVariables variables,
      String loggingPipelineFilename)
      throws HopException {
    try {
      final Timer timer = new Timer();

      if (workflowLog.isExecutingAtStart()) {
        executeLoggingPipeline(workflowLog, "start", loggingPipelineFilename, workflow, variables);
      }

      if (workflowLog.isExecutingAtEnd()) {
        workflow.addExecutionFinishedListener(
            engine -> {
              executeLoggingPipeline(
                  workflowLog, "end", loggingPipelineFilename, workflow, variables);
              ExecutorUtil.cleanup(timer);
            });
      }

      if (workflowLog.isExecutingPeriodically()) {
        int intervalInSeconds =
            Const.toInt(variables.resolve(workflowLog.getIntervalInSeconds()), -1);
        if (intervalInSeconds > 0) {
          TimerTask timerTask =
              new TimerTask() {
                @Override
                public void run() {
                  try {
                    executeLoggingPipeline(
                        workflowLog, "interval", loggingPipelineFilename, workflow, variables);
                  } catch (Exception e) {
                    throw new RuntimeException(
                        "Unable to do interval logging for Workflow Log object '"
                            + workflowLog.getName()
                            + "'",
                        e);
                  }
                }
              };
          timer.schedule(timerTask, intervalInSeconds * 1000L, intervalInSeconds * 1000L);
        }
      }
    } catch (Exception e) {
      workflow.stopExecution();
      throw new HopException(
          "Error handling Workflow Log metadata object '"
              + workflowLog.getName()
              + "' at the start of pipeline: "
              + workflow,
          e);
    }
  }

  private synchronized void executeLoggingPipeline(
      WorkflowLog pipelineLog,
      String loggingPhase,
      String loggingPipelineFilename,
      IWorkflowEngine<WorkflowMeta> workflow,
      IVariables variables)
      throws HopException {

    PipelineMeta loggingPipelineMeta =
        new PipelineMeta(loggingPipelineFilename, workflow.getMetadataProvider(), variables);

    // Create a local pipeline engine...
    //
    LocalPipelineEngine loggingPipeline =
        new LocalPipelineEngine(loggingPipelineMeta, variables, workflow);

    // Do NOT link to parent to avoid stopped() being transferred to the logging pipeline(s).
    loggingPipeline.setParent(null);
    loggingPipeline.setParentWorkflow(null);

    // Flag it as a logging pipeline so we don't log ourselves...
    //
    loggingPipeline.getExtensionDataMap().put(PipelineStartLoggingXp.PIPELINE_LOGGING_FLAG, "Y");

    // Only log errors
    loggingPipeline.setLogLevel(LogLevel.ERROR);
    loggingPipeline.prepareExecution();

    // Grab the WorkflowLogging transforms and inject the pipeline information...
    //
    for (TransformMetaDataCombi combi : loggingPipeline.getTransforms()) {
      if (combi.transform instanceof WorkflowLogging workflowLogging) {
        workflowLogging.setLoggingWorkflow(workflow);
        workflowLogging.setLoggingPhase(loggingPhase);
      }
    }

    // Execute the logging pipeline to save the logging information
    //
    loggingPipeline.startThreads();
    loggingPipeline.waitUntilFinished();
  }
}
