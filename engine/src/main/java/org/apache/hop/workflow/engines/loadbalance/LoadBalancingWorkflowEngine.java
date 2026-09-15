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

package org.apache.hop.workflow.engines.loadbalance;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.execution.ExecutionBuilder;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.server.loadbalance.ILoadBalancingRunConfiguration;
import org.apache.hop.server.loadbalance.LoadBalancingAlgorithm;
import org.apache.hop.server.loadbalance.LoadBalancingAssignment;
import org.apache.hop.server.loadbalance.LoadBalancingCoordinator;
import org.apache.hop.server.loadbalance.LoadBalancingRetryPolicy;
import org.apache.hop.server.loadbalance.ServerHealthSnapshot;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.engine.WorkflowEnginePlugin;
import org.apache.hop.workflow.engines.remote.RemoteWorkflowEngine;
import org.apache.hop.www.HopServerAdmission;

@WorkflowEnginePlugin(
    id = "LoadBalancing",
    name = "Hop load-balancing workflow engine",
    description = "Assigns the workflow to one Hop server from a configured group")
public class LoadBalancingWorkflowEngine extends RemoteWorkflowEngine {

  public static final String DETAIL_ASSIGNED_SERVER = "assignedServer";
  public static final String DETAIL_ATTEMPT = "attempt";
  public static final String DETAIL_ALGORITHM = "algorithm";
  public static final String DETAIL_CONTAINER_ID = "containerId";
  public static final String DETAIL_LOAD_AT_ASSIGNMENT = "loadAtAssignment";

  private LoadBalancingCoordinator<LoadBalancingWorkflowRunConfiguration> coordinator;
  private LoadBalancingAssignment assignment;
  private ExecutionInfoLocation executionInfoLocation;

  @Override
  public IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration() {
    return new LoadBalancingWorkflowRunConfiguration();
  }

  @Override
  protected void submitToRemoteServer() throws HopException {
    IWorkflowEngineRunConfiguration engineRunConfiguration =
        workflowRunConfiguration.getEngineRunConfiguration();
    if (!(engineRunConfiguration instanceof ILoadBalancingRunConfiguration)) {
      throw new HopException(
          "The load-balancing workflow engine expects a load-balancing workflow configuration");
    }

    coordinator =
        new LoadBalancingCoordinator<>(
            this, metadataProvider, logChannel, workflowRunConfiguration.getName(), true);
    coordinator.reload();
    validateRunConfigurationChain(workflowRunConfiguration);

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
            "Executing this workflow using the Load-balancing Workflow Engine with run configuration '"
                + workflowRunConfiguration.getName()
                + "', attempt "
                + attempt
                + " on server '"
                + selectedHopServerName
                + "'");

        super.submitToRemoteServer();
        assignment.setStatus(LoadBalancingAssignment.STATUS_RUNNING);
        assignment.setContainerId(containerId);
        coordinator.saveAssignment(assignment);
        registerLoadBalancingExecutionInformation(snapshot, attempt);
        addExecutionFinishedListener(engine -> updateFinishedAssignmentAndState());
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
        if (e instanceof HopException hopException) {
          throw hopException;
        }
        throw new HopException("Error submitting load-balanced workflow", e);
      }
    }
    if (assignment != null) {
      assignment.setStatus(LoadBalancingAssignment.STATUS_FAILED);
      coordinator.saveAssignment(assignment);
    }
    throw new HopException(
        "Load-balancing gave up after " + retry.getAttempt() + " attempt(s)", lastError);
  }

  private LoadBalancingAssignment newAssignment(
      ServerHealthSnapshot snapshot, int attempt, LoadBalancingRetryPolicy retry) {
    LoadBalancingAssignment next = new LoadBalancingAssignment();
    next.setExecutionId(getLogChannelId());
    next.setRunConfigurationName(workflowRunConfiguration.getName());
    next.setExecutorName(getWorkflowName());
    next.setExecutorType("workflow");
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
    if (executionInfoLocation != null || workflowRunConfiguration == null) {
      return;
    }
    String locationName = resolve(workflowRunConfiguration.getExecutionInfoLocationName());
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
      boolean failed = result != null && (result.getNrErrors() > 0 || !result.getResult());
      if (assignment != null && coordinator != null) {
        assignment.setStatus(
            failed
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
