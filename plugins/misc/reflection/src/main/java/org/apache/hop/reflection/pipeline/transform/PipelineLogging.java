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

package org.apache.hop.reflection.pipeline.transform;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import java.util.List;

public class PipelineLogging extends BaseTransform<PipelineLoggingMeta, PipelineLoggingData>
{
  private IPipelineEngine<PipelineMeta> loggingPipeline;
  private String loggingPhase;

  public PipelineLogging(
      TransformMeta transformMeta,
      PipelineLoggingMeta meta,
      PipelineLoggingData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    if (loggingPipeline == null) {
      logBasic("This transform will produce output when called by the Pipeline Log configuration");
      setOutputDone();
      return false;
    }

    // Calculate the output fields of this transform
    //
    IRowMeta outputRowMeta = new RowMeta();
    meta.getFields(outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    PipelineMeta pipelineMeta = loggingPipeline.getPipelineMeta();

    // Generate the pipeline row...
    //
    Object[] pipelineRow = RowDataUtil.allocateRowData(outputRowMeta.size());
    int index = 0;

    // Logging date: start date of the logging pipeline
    pipelineRow[index++] = getPipeline().getExecutionStartDate();

    // The logging phase
    pipelineRow[index++] = loggingPhase;

    // Name of the pipeline
    pipelineRow[index++] = pipelineMeta.getName();

    // Filename of the pipeline
    pipelineRow[index++] = pipelineMeta.getFilename();

    // Start date the pipeline
    pipelineRow[index++] = loggingPipeline.getExecutionStartDate();

    // End date the pipeline
    pipelineRow[index++] = loggingPipeline.getExecutionEndDate();

    // Log channel ID of the pipeline
    pipelineRow[index++] = loggingPipeline.getLogChannelId();

    // Parent Log channel ID
    ILoggingObject parent = loggingPipeline.getParent();
    pipelineRow[index++] = parent == null ? null : parent.getLogChannelId();

    // Logging text of the pipeline
    pipelineRow[index++] =
        HopLogStore.getAppender().getBuffer(loggingPipeline.getLogChannelId(), false).toString();

    // Number of errors in the pipeline
    pipelineRow[index++] = (long) loggingPipeline.getErrors();

    // Pipeline status description
    pipelineRow[index++] = loggingPipeline.getStatusDescription();

    int startIndex = index;

    List<IEngineComponent> components = loggingPipeline.getComponents();

    // If we have an empty list of transforms, make sure to emit a single row
    // This is the case in the "start" phase when don't have any engine components yet.
    // We still want to have a trigger in the pipeline but with only the pipeline information...
    //
    if (meta.isLoggingTransforms() && !components.isEmpty()) {

      // Log all the transform copies
      //
      for (IEngineComponent component : components) {
        index = startIndex;
        Object[] transformRow = RowDataUtil.createResizedCopy(pipelineRow, outputRowMeta.size());

        // Name of the transform
        transformRow[index++] = component.getName();

        // Copy number...
        transformRow[index++] = (long) component.getCopyNr();

        // Status description
        transformRow[index++] = component.getStatusDescription();

        // Log channel ID
        transformRow[index++] = component.getLogChannelId();

        // Logging text of transform
        transformRow[index++] = component.getLogText();

        // Lines read
        transformRow[index++] = component.getLinesRead();

        // Lines written
        transformRow[index++] = component.getLinesWritten();

        // Lines input
        transformRow[index++] = component.getLinesInput();

        // Lines output
        transformRow[index++] = component.getLinesOutput();

        // Lines updated
        transformRow[index++] = component.getLinesUpdated();

        // Lines rejected
        transformRow[index++] = component.getLinesRejected();

        // Errors
        transformRow[index++] = component.getErrors();

        // Execution start
        transformRow[index++] = component.getExecutionStartDate();

        // Execution end
        transformRow[index++] = component.getExecutionEndDate();

        // Execution duration
        transformRow[index] = component.getExecutionDuration();

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
   * Gets loggingPipeline
   *
   * @return value of loggingPipeline
   */
  public IPipelineEngine<PipelineMeta> getLoggingPipeline() {
    return loggingPipeline;
  }

  /** @param loggingPipeline The loggingPipeline to set */
  public void setLoggingPipeline(IPipelineEngine<PipelineMeta> loggingPipeline) {
    this.loggingPipeline = loggingPipeline;
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
