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
 *
 */

package org.apache.hop.reflection.workflow.transform;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.List;

public class WorkflowLogging extends BaseTransform<WorkflowLoggingMeta, WorkflowLoggingData> {

  private IWorkflowEngine<WorkflowMeta> loggingWorkflow;
  private String loggingPhase;

  public WorkflowLogging(
      TransformMeta transformMeta,
      WorkflowLoggingMeta meta,
      WorkflowLoggingData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    if (loggingWorkflow == null) {
      logBasic("This transform will produce output when called by the Pipeline Log configuration");
      setOutputDone();
      return false;
    }

    // Calculate the output fields of this transform
    //
    IRowMeta outputRowMeta = new RowMeta();
    meta.getFields(outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    WorkflowMeta workflowMeta = loggingWorkflow.getWorkflowMeta();

    // Generate the pipeline row...
    //
    Object[] pipelineRow = RowDataUtil.allocateRowData(outputRowMeta.size());
    int index = 0;

    // Logging date: start date of the logging pipeline
    pipelineRow[index++] = getPipeline().getExecutionStartDate();

    // The logging phase
    pipelineRow[index++] = loggingPhase;

    // Name of the workflow
    pipelineRow[index++] = workflowMeta.getName();

    // Filename of the workflow
    pipelineRow[index++] = workflowMeta.getFilename();

    // Start date the workflow
    pipelineRow[index++] = loggingWorkflow.getExecutionStartDate();

    // End date the workflow
    pipelineRow[index++] = loggingWorkflow.getExecutionEndDate();

    // Log channel ID of the workflow
    pipelineRow[index++] = loggingWorkflow.getLogChannelId();

    // Log channel ID of the parent
    ILoggingObject parent = loggingWorkflow.getParent();
    pipelineRow[index++] = parent == null ? null : parent.getLogChannelId();

    // Logging text of the workflow
    pipelineRow[index++] =
        HopLogStore.getAppender().getBuffer(loggingWorkflow.getLogChannelId(), false).toString();

    // Result object *after* execution:
    Result result = loggingWorkflow.getResult();

    // Current number of errors in the workflow
    pipelineRow[index++] = result == null ? null : (long) result.getNrErrors();

    // Workflow status description
    pipelineRow[index++] = loggingWorkflow.getStatusDescription();

    int startIndex = index;

    List<ActionResult> actionResults = loggingWorkflow.getActionResults();

    // If we have an empty list of actionResults, make sure to emit a single row
    // This is the case in the "start" phase when didn't run any actions yet.
    // We still want to have a trigger in the logging pipeline but with only the workflow
    // information...
    //
    if (meta.isLoggingActionResults() && !actionResults.isEmpty()) {

      // Log all the transform copies
      //
      for (ActionResult actionResult : actionResults) {
        index = startIndex;
        result = actionResult.getResult();
        Object[] transformRow = RowDataUtil.createResizedCopy(pipelineRow, outputRowMeta.size());

        // Name of the action
        transformRow[index++] = actionResult.getActionName();

        // Copy number...
        transformRow[index++] = result.getEntryNr();

        // Result (true/false)
        transformRow[index++] = result.getResult();

        // Log channel ID
        transformRow[index++] = actionResult.getLogChannelId();

        // Logging text of action
        transformRow[index++] = result.getLogText();

        // Errors
        transformRow[index++] = result.getNrErrors();

        // Action log date
        transformRow[index++] = actionResult.getLogDate();

        // Execution duration
        transformRow[index++] = result.getElapsedTimeMillis();

        // Exit status
        transformRow[index++] = (long) result.getExitStatus();

        // Exit status
        transformRow[index++] = result.getNrFilesRetrieved();

        // Action filename
        transformRow[index++] = actionResult.getActionFilename();

        // Action comment
        transformRow[index++] = actionResult.getComment();

        // Action reason
        transformRow[index++] = actionResult.getReason();

        // Send it on its way...
        //
        putRow(outputRowMeta, transformRow);
      }

    } else {
      putRow(outputRowMeta, pipelineRow);
    }

    // All done in one go!
    //
    setOutputDone();
    return false;
  }

  /**
   * Gets loggingWorkflow
   *
   * @return value of loggingWorkflow
   */
  public IWorkflowEngine<WorkflowMeta> getLoggingWorkflow() {
    return loggingWorkflow;
  }

  /** @param loggingWorkflow The loggingWorkflow to set */
  public void setLoggingWorkflow(IWorkflowEngine<WorkflowMeta> loggingWorkflow) {
    this.loggingWorkflow = loggingWorkflow;
  }

  /**
   * Gets loggingPhase
   *
   * @return value of loggingPhase
   */
  public String getLoggingPhase() {
    return loggingPhase;
  }

  /** @param loggingPhase The loggingPhase to set */
  public void setLoggingPhase(String loggingPhase) {
    this.loggingPhase = loggingPhase;
  }
}
