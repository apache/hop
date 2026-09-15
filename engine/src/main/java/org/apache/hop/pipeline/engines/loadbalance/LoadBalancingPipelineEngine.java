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

package org.apache.hop.pipeline.engines.loadbalance;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.execution.ExecutionBuilder;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.engines.remote.RemotePipelineEngine;
import org.apache.hop.server.loadbalance.ILoadBalancingRunConfiguration;
import org.apache.hop.server.loadbalance.LoadBalancingAlgorithm;
import org.apache.hop.server.loadbalance.LoadBalancingAssignment;
import org.apache.hop.server.loadbalance.LoadBalancingCoordinator;
import org.apache.hop.server.loadbalance.LoadBalancingRetryPolicy;
import org.apache.hop.server.loadbalance.ServerHealthSnapshot;
import org.apache.hop.www.HopServerAdmission;

@PipelineEnginePlugin(
    id = "LoadBalancing",
    name = "Hop load-balancing pipeline engine",
    description = "Assigns the pipeline to one Hop server from a configured group")
public class LoadBalancingPipelineEngine extends RemotePipelineEngine {

  public static final String DETAIL_ASSIGNED_SERVER = "assignedServer";
  public static final String DETAIL_ATTEMPT = "attempt";
  public static final String DETAIL_ALGORITHM = "algorithm";
  public static final String DETAIL_CONTAINER_ID = "containerId";
  public static final String DETAIL_LOAD_AT_ASSIGNMENT = "loadAtAssignment";

  private LoadBalancingCoordinator<LoadBalancingPipelineRunConfiguration> coordinator;
  private LoadBalancingAssignment assignment;
  private ExecutionInfoLocation executionInfoLocation;

  public LoadBalancingPipelineEngine() {
    super();
  }

  public LoadBalancingPipelineEngine(PipelineMeta subject) {
    super(subject);
  }

  @Override
  public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new LoadBalancingPipelineRunConfiguration();
  }

  @Override
  public void prepareExecution() throws HopException {
    IPipelineEngineRunConfiguration engineRunConfiguration =
        pipelineRunConfiguration.getEngineRunConfiguration();
    if (!(engineRunConfiguration instanceof ILoadBalancingRunConfiguration)) {
      throw new HopException(
          "The load-balancing pipeline engine expects a load-balancing pipeline configuration");
    }

    this.logChannel = new LogChannel(this, getParent());
    this.logChannel.setLogLevel(logLevel);

    coordinator =
        new LoadBalancingCoordinator<>(
            this, metadataProvider, logChannel, pipelineRunConfiguration.getName(), false);
    coordinator.reload();
    validateRunConfigurationChain(pipelineRunConfiguration);

    LoadBalancingRetryPolicy retry =
        LoadBalancingRetryPolicy.from(coordinator.getCurrentConfig(), this);
    Exception lastError = null;
    while (retry.canAttempt()) {
      int attempt = retry.beginAttempt();
      containerId = null;
      hopServer = null;
      try {
        ServerHealthSnapshot snapshot = coordinator.selectServer();
        selectedHopServerName = snapshot.getHopServerName();
        admissionMaxConcurrent = snapshot.getMaxConcurrent();
        assignment = newAssignment(snapshot, attempt, retry);
        coordinator.saveAssignment(assignment);

        logChannel.logBasic(
            "Executing this pipeline using the Load-balancing Pipeline Engine with run configuration '"
                + pipelineRunConfiguration.getName()
                + "', attempt "
                + attempt
                + " on server '"
                + selectedHopServerName
                + "'");

        super.prepareExecution();

        assignment.setExecutionId(getLogChannelId());
        assignment.setStatus(LoadBalancingAssignment.STATUS_RUNNING);
        assignment.setContainerId(containerId);
        coordinator.saveAssignment(assignment);
        registerLoadBalancingExecutionInformation(snapshot, attempt);
        return;
      } catch (Exception e) {
        lastError = e;
        if (assignment != null) {
          assignment.setLastError(e.getMessage());
          assignment.setStatus(LoadBalancingAssignment.STATUS_RETRYING);
          coordinator.saveAssignment(assignment);
        }
        if (HopServerAdmission.isRetryableRegistrationFailure(e, containerId)
            && retry.canAttempt()) {
          logCapacityWait(attempt, e);
          sleepBackoff(retry);
          continue;
        }
        if (assignment != null) {
          assignment.setStatus(LoadBalancingAssignment.STATUS_FAILED);
          coordinator.saveAssignment(assignment);
        }
        throw new HopException("Error preparing load-balanced pipeline", e);
      }
    }
    if (assignment != null) {
      assignment.setStatus(LoadBalancingAssignment.STATUS_FAILED);
      coordinator.saveAssignment(assignment);
    }
    throw new HopException(
        "Load-balancing gave up after " + retry.getAttempt() + " attempt(s)", lastError);
  }

  @Override
  public void startThreads() throws HopException {
    try {
      super.startThreads();
      addExecutionFinishedListener(engine -> updateFinishedAssignmentAndState());
    } catch (HopException e) {
      if (assignment != null) {
        assignment.setStatus(LoadBalancingAssignment.STATUS_FAILED);
        assignment.setLastError(e.getMessage());
        coordinator.saveAssignment(assignment);
      }
      throw e;
    }
  }

  private LoadBalancingAssignment newAssignment(
      ServerHealthSnapshot snapshot, int attempt, LoadBalancingRetryPolicy retry) {
    LoadBalancingAssignment next = new LoadBalancingAssignment();
    next.setExecutionId(getLogChannelId());
    next.setRunConfigurationName(pipelineRunConfiguration.getName());
    next.setExecutorName(subject == null ? null : subject.getName());
    next.setExecutorType("pipeline");
    next.setServerName(snapshot.getHopServerName());
    next.setAttempt(attempt);
    next.setFirstAttemptEpochMs(retry.getFirstAttemptEpochMs());
    next.setStatus(LoadBalancingAssignment.STATUS_ASSIGNING);
    next.setAlgorithm(
        LoadBalancingAlgorithm.fromCodeOrDescription(
                resolve(coordinator.getCurrentConfig().getAlgorithm()))
            .getCode());
    next.setOccupyingSlotsAtAssignment(snapshot.getOccupyingSlots());
    next.setMaxConcurrent(snapshot.getMaxConcurrent());
    return next;
  }

  private void logCapacityWait(int attempt, Exception error) {
    String server =
        StringUtils.isEmpty(selectedHopServerName)
            ? "the load-balancing group"
            : selectedHopServerName;
    logChannel.logBasic(
        "Load-balancing attempt "
            + attempt
            + " waiting for a free slot on "
            + server
            + ": "
            + error.getMessage());
  }

  private void sleepBackoff(LoadBalancingRetryPolicy retry) {
    long waitMs = Math.min(1000L, retry.remainingWindowMs());
    if (waitMs <= 0) {
      return;
    }
    try {
      Thread.sleep(waitMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void registerLoadBalancingExecutionInformation(
      ServerHealthSnapshot snapshot, int attempt) {
    try {
      lookupExecutionInformationLocation();
      if (executionInfoLocation == null) {
        return;
      }
      IExecutionInfoLocation location = executionInfoLocation.getExecutionInfoLocation();
      location.registerExecution(ExecutionBuilder.fromExecutor(this).build());
      ExecutionState state = ExecutionStateBuilder.fromExecutor(this, -1).build();
      Map<String, String> details =
          state.getDetails() == null ? new HashMap<>() : state.getDetails();
      details.put(DETAIL_ASSIGNED_SERVER, selectedHopServerName);
      details.put(DETAIL_ATTEMPT, Integer.toString(attempt));
      details.put(
          DETAIL_ALGORITHM,
          LoadBalancingAlgorithm.fromCodeOrDescription(
                  resolve(coordinator.getCurrentConfig().getAlgorithm()))
              .getCode());
      details.put(DETAIL_CONTAINER_ID, containerId);
      details.put(
          DETAIL_LOAD_AT_ASSIGNMENT,
          snapshot.getOccupyingSlots() + "/" + snapshot.getMaxConcurrent());
      state.setDetails(details);
      location.updateExecutionState(state);
    } catch (Exception e) {
      logChannel.logError("Error registering load-balancing execution information (non-fatal)", e);
    }
  }

  private void lookupExecutionInformationLocation() throws HopException {
    if (executionInfoLocation != null || pipelineRunConfiguration == null) {
      return;
    }
    String locationName = resolve(pipelineRunConfiguration.getExecutionInfoLocationName());
    if (StringUtils.isEmpty(locationName) || metadataProvider == null) {
      return;
    }
    ExecutionInfoLocation location =
        metadataProvider.getSerializer(ExecutionInfoLocation.class).load(locationName);
    if (location == null) {
      logChannel.logError(
          "Execution information location '" + locationName + "' could not be found");
      return;
    }
    location.getExecutionInfoLocation().initialize(this, metadataProvider);
    executionInfoLocation = location;
  }

  private void updateFinishedAssignmentAndState() {
    try {
      if (assignment != null && coordinator != null) {
        assignment.setStatus(
            getErrors() > 0
                ? LoadBalancingAssignment.STATUS_FAILED
                : LoadBalancingAssignment.STATUS_FINISHED);
        assignment.setContainerId(containerId);
        coordinator.saveAssignment(assignment);
      }
      if (executionInfoLocation != null) {
        IExecutionInfoLocation location = executionInfoLocation.getExecutionInfoLocation();
        ExecutionState state = ExecutionStateBuilder.fromExecutor(this, -1).build();
        Map<String, String> details =
            state.getDetails() == null ? new HashMap<>() : state.getDetails();
        if (assignment != null) {
          details.put(DETAIL_ASSIGNED_SERVER, assignment.getServerName());
          details.put(DETAIL_ATTEMPT, Integer.toString(assignment.getAttempt()));
          details.put(DETAIL_ALGORITHM, assignment.getAlgorithm());
          details.put(DETAIL_CONTAINER_ID, assignment.getContainerId());
        }
        state.setDetails(details);
        location.updateExecutionState(state);
        location.close();
      }
    } catch (Exception e) {
      if (logChannel != null) {
        logChannel.logError("Error updating load-balancing assignment state (non-fatal)", e);
      }
    }
  }
}
