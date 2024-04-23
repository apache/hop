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

package org.apache.hop.beam.engines.dataflow;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.hop.beam.engines.BeamPipelineEngine;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

@PipelineEnginePlugin(
    id = "BeamDataFlowPipelineEngine",
    name = "Beam DataFlow pipeline engine",
    description =
        "This allows you to run your pipeline on Google Cloud Platform DataFlow, provided by the Apache Beam community")
@GuiPlugin
public class BeamDataFlowPipelineEngine extends BeamPipelineEngine
    implements IPipelineEngine<PipelineMeta> {

  public static final String DETAIL_DATAFLOW_JOB_ID = "dataflow.job.id";
  public static final String DETAIL_DATAFLOW_PROJECT_ID = "dataflow.project.id";
  public static final String DETAIL_DATAFLOW_REGION = "dataflow.region";

  @Override
  public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    BeamDataFlowPipelineRunConfiguration runConfiguration =
        new BeamDataFlowPipelineRunConfiguration();
    runConfiguration.setUserAgent("Hop");
    return runConfiguration;
  }

  @Override
  public void validatePipelineRunConfigurationClass(
      IPipelineEngineRunConfiguration engineRunConfiguration) throws HopException {
    if (!(engineRunConfiguration instanceof BeamDataFlowPipelineRunConfiguration)) {
      throw new HopException(
          "A Beam Direct pipeline engine needs a direct run configuration, not of class "
              + engineRunConfiguration.getClass().getName());
    }
  }

  @Override
  protected void updatePipelineState(IExecutionInfoLocation iLocation) throws HopException {
    ExecutionState executionState =
        ExecutionStateBuilder.fromExecutor(BeamDataFlowPipelineEngine.this, -1).build();

    // Add Dataflow specific information to the execution state.
    // This can then be picked up
    //
    if (beamPipelineResults != null) {
      DataflowPipelineJob dataflowPipelineJob = (DataflowPipelineJob) beamPipelineResults;
      executionState.getDetails().put(DETAIL_DATAFLOW_JOB_ID, dataflowPipelineJob.getJobId());
      executionState
          .getDetails()
          .put(DETAIL_DATAFLOW_PROJECT_ID, dataflowPipelineJob.getProjectId());
      executionState.getDetails().put(DETAIL_DATAFLOW_REGION, dataflowPipelineJob.getRegion());
    }

    iLocation.updateExecutionState(executionState);

    // Also update the state of the components
    //
    for (IEngineComponent component : getComponents()) {
      ExecutionState transformState = ExecutionStateBuilder.fromTransform(this, component).build();
      iLocation.updateExecutionState(transformState);
    }
  }
}
