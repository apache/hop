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

package org.apache.hop.beam.engines.flink;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import lombok.Getter;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.hop.beam.engines.BeamPipelineEngine;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;

@PipelineEnginePlugin(
    id = "BeamFlinkPipelineEngine",
    name = "Beam Flink pipeline engine",
    description = "This is a Flink pipeline engine provided by the Apache Beam community")
public class BeamFlinkPipelineEngine extends BeamPipelineEngine
    implements IPipelineEngine<PipelineMeta> {

  private static final Class<?> PKG = BeamFlinkPipelineEngine.class;

  /** Execution-information detail for the Flink job id submitted for this pipeline. */
  public static final String DETAIL_FLINK_JOB_ID = "flink.job.id";

  private static final String COLLECTION_MASTER = "[collection]";

  /** Job id Flink will submit. Null for the collection master, which does not start a job. */
  @Getter private String flinkJobId;

  private Path flinkJobConfDir;

  @Override
  public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    BeamFlinkPipelineRunConfiguration runConfiguration = new BeamFlinkPipelineRunConfiguration();
    runConfiguration.setUserAgent("Hop");
    return runConfiguration;
  }

  @Override
  public void validatePipelineRunConfigurationClass(
      IPipelineEngineRunConfiguration engineRunConfiguration) throws HopException {
    if (!(engineRunConfiguration instanceof BeamFlinkPipelineRunConfiguration)) {
      throw new HopException(
          "A Beam Direct pipeline engine needs a direct run configuration, not of class "
              + engineRunConfiguration.getClass().getName());
    }
  }

  @Override
  public void prepareExecution() throws HopException {
    super.prepareExecution();
    assignFlinkJobId();
  }

  @Override
  public void startThreads() throws HopException {
    try {
      super.startThreads();
    } finally {
      deleteFlinkJobConfiguration();
    }
  }

  @Override
  protected ExecutionState capturePipelineExecutionState() {
    ExecutionState executionState = super.capturePipelineExecutionState();
    if (StringUtils.isNotEmpty(flinkJobId)) {
      if (executionState.getDetails() == null) {
        executionState.setDetails(new HashMap<>());
      }
      executionState.getDetails().put(DETAIL_FLINK_JOB_ID, flinkJobId);
    }
    return executionState;
  }

  /**
   * Fix the id Flink submits. {@code FlinkRunnerResult} keeps no job id, including after a failed
   * attached run.
   */
  private void assignFlinkJobId() throws HopException {
    deleteFlinkJobConfiguration();
    flinkJobId = null;
    if (getBeamPipeline() == null || getBeamPipeline().getOptions() == null) {
      return;
    }
    FlinkPipelineOptions options = getBeamPipeline().getOptions().as(FlinkPipelineOptions.class);
    if (COLLECTION_MASTER.equals(options.getFlinkMaster())) {
      return;
    }
    String jobId = new JobID().toHexString();
    flinkJobConfDir = FlinkJobConfiguration.create(options.getFlinkConfDir(), jobId);
    options.setFlinkConfDir(flinkJobConfDir.toAbsolutePath().toString());
    flinkJobId = jobId;
    logChannel.logBasic(BaseMessages.getString(PKG, "BeamEnginesFlink.JobId.Log", jobId));
  }

  private void deleteFlinkJobConfiguration() {
    Path dir = flinkJobConfDir;
    flinkJobConfDir = null;
    if (dir == null) {
      return;
    }
    try {
      FlinkJobConfiguration.delete(dir);
    } catch (IOException e) {
      logChannel.logDebug("Could not remove temporary Flink configuration directory " + dir, e);
    }
  }
}
