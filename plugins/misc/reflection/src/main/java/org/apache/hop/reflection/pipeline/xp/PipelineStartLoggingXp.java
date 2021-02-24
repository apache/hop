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

package org.apache.hop.reflection.pipeline.xp;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.reflection.pipeline.meta.PipelineLog;
import org.apache.hop.reflection.pipeline.transform.PipelineLogging;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

@ExtensionPoint(
    id = "PipelineStartLoggingXp",
    extensionPointId = "PipelinePrepareExecution",
    description = "At the start of a pipeline, handle any Pipeline Log metadata objects")
public class PipelineStartLoggingXp implements IExtensionPoint<Pipeline> {

  public static final String PIPELINE_LOGGING_FLAG = "PipelineLoggingActive";

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Pipeline pipeline)
      throws HopException {

    // Prevent recursive logging of the logging pipeline
    //
    if (pipeline.getExtensionDataMap().get(PIPELINE_LOGGING_FLAG)!=null) {
      return;
    }


    IHopMetadataProvider metadataProvider = pipeline.getMetadataProvider();
    IHopMetadataSerializer<PipelineLog> serializer =
        metadataProvider.getSerializer(PipelineLog.class);
    List<PipelineLog> pipelineLogs = serializer.loadAll();

    for (PipelineLog pipelineLog : pipelineLogs) {
      handlePipelineLog(log, pipelineLog, pipeline, variables);
    }
  }

  private void handlePipelineLog(
      final ILogChannel log,
      final PipelineLog pipelineLog,
      final IPipelineEngine<PipelineMeta> pipeline,
      final IVariables variables)
      throws HopException {
    try {

      // See if we need to do anything at all...
      //
      if (!pipelineLog.isEnabled()) {
        return;
      }

      // If we log parent (root) pipelines only we don't want a parent
      //
      if (pipelineLog.isLoggingParentsOnly()) {
        if (pipeline.getParentWorkflow() != null || pipeline.getParentPipeline() != null) {
          return;
        }
      }

      // Load the pipeline filename specified in the Pipeline Log object...
      //
      final String loggingPipelineFilename = variables.resolve(pipelineLog.getPipelineFilename());

      // See if the file exists...
      FileObject loggingFileObject = HopVfs.getFileObject(loggingPipelineFilename);
      if (!loggingFileObject.exists()) {
        log.logBasic(
            "WARNING: The Pipeline Log pipeline file '"
                + loggingPipelineFilename
                + "' couldn't be found to execute.");
        return;
      }

      final Timer timer = new Timer();

      if (pipelineLog.isExecutingAtStart()) {
        executeLoggingPipeline(pipelineLog, "start", loggingPipelineFilename, pipeline, variables);
      }

      if (pipelineLog.isExecutingAtEnd()) {
        pipeline.addExecutionFinishedListener(
            engine -> {
              executeLoggingPipeline(
                  pipelineLog, "end", loggingPipelineFilename, pipeline, variables);
              timer.cancel();
            });
        pipeline.addExecutionStoppedListener(
            engine -> {
              try {
                executeLoggingPipeline(
                    pipelineLog, "stop", loggingPipelineFilename, pipeline, variables);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Unable to do interval logging for Pipeline Log object '"
                        + pipelineLog.getName()
                        + "'",
                    e);
              }
              timer.cancel();
            });
      }

      if (pipelineLog.isExecutingPeriodically()) {
        int intervalInSeconds =
            Const.toInt(variables.resolve(pipelineLog.getIntervalInSeconds()), -1);
        if (intervalInSeconds > 0) {
          TimerTask timerTask =
              new TimerTask() {
                @Override
                public void run() {
                  try {
                    executeLoggingPipeline(
                        pipelineLog, "interval", loggingPipelineFilename, pipeline, variables);
                  } catch (Exception e) {
                    throw new RuntimeException(
                        "Unable to do interval logging for Pipeline Log object '"
                            + pipelineLog.getName()
                            + "'",
                        e);
                  }
                }
              };
          timer.schedule(timerTask, intervalInSeconds * 1000L, intervalInSeconds * 1000L);
        }
      }

    } catch (Exception e) {
      pipeline.stopAll();
      throw new HopException(
          "Error handling Pipeline Log metadata object '"
              + pipelineLog.getName()
              + "' at the start of pipeline: "
              + pipeline,
          e);
    }
  }

  private synchronized void executeLoggingPipeline(
      PipelineLog pipelineLog,
      String loggingPhase,
      String loggingPipelineFilename,
      IPipelineEngine<PipelineMeta> pipeline,
      IVariables variables)
      throws HopException {

    PipelineMeta loggingPipelineMeta =
        new PipelineMeta(loggingPipelineFilename, pipeline.getMetadataProvider(), true, variables);

    // Create a local pipeline engine...
    //
    LocalPipelineEngine loggingPipeline =
        new LocalPipelineEngine(loggingPipelineMeta, variables, pipeline);

    // Flag it as a logging pipeline so we don't log ourselves...
    //
    loggingPipeline.getExtensionDataMap().put(PIPELINE_LOGGING_FLAG, "Y");

    // Only log errors
    loggingPipeline.setLogLevel(LogLevel.ERROR);
    loggingPipeline.prepareExecution();

    // Grab the WorkflowLogging transforms and inject the pipeline information...
    //
    for (TransformMetaDataCombi combi : loggingPipeline.getTransforms()) {
      if (combi.transform instanceof PipelineLogging) {
        PipelineLogging pipelineLogging = (PipelineLogging) combi.transform;

        pipelineLogging.setLoggingPipeline(pipeline);
        pipelineLogging.setLoggingPhase(loggingPhase);
      }
    }

    // Execute the logging pipeline to save the logging information
    //
    loggingPipeline.startThreads();
    loggingPipeline.waitUntilFinished();
  }
}
