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

package org.apache.hop.server.loadbalance;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.www.HopServerPipelineStatus;
import org.apache.hop.www.HopServerStatus;
import org.apache.hop.www.HopServerWorkflowStatus;
import org.apache.hop.www.RemoteHopServer;

/** Probes a Hop Server for health, load, and occupying execution slots. */
public final class HopServerProbe {

  private HopServerProbe() {}

  public static ServerHealthSnapshot probe(
      HopServerMeta serverMeta, IVariables variables, boolean enabled, int maxConcurrent) {
    ServerHealthSnapshot snapshot = new ServerHealthSnapshot();
    snapshot.setHopServerName(serverMeta == null ? null : serverMeta.getName());
    snapshot.setEnabled(enabled);
    snapshot.setMaxConcurrent(maxConcurrent);
    if (serverMeta == null) {
      snapshot.setAvailable(false);
      snapshot.setErrorMessage("Hop server metadata is missing");
      return snapshot;
    }

    long startNs = System.nanoTime();
    try {
      RemoteHopServer server = new RemoteHopServer(serverMeta);
      HopServerStatus status = server.requestServerStatus(variables);
      applyStatus(snapshot, status);
      snapshot.setAvailable(true);
    } catch (Exception e) {
      snapshot.setAvailable(false);
      snapshot.setErrorMessage("Error querying Hop server : " + e.getMessage());
    } finally {
      snapshot.setResponseNs(System.nanoTime() - startNs);
    }
    return snapshot;
  }

  public static ServerHealthSnapshot probeWithTimeout(
      HopServerMeta serverMeta,
      IVariables variables,
      boolean enabled,
      int maxConcurrent,
      long timeoutMs) {
    if (timeoutMs <= 0) {
      return probe(serverMeta, variables, enabled, maxConcurrent);
    }
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<ServerHealthSnapshot> future =
          executor.submit(() -> probe(serverMeta, variables, enabled, maxConcurrent));
      return future.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      return ServerHealthSnapshot.unavailable(
          serverMeta == null ? null : serverMeta.getName(),
          enabled,
          maxConcurrent,
          "timeout after " + timeoutMs + "ms");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return ServerHealthSnapshot.unavailable(
          serverMeta == null ? null : serverMeta.getName(),
          enabled,
          maxConcurrent,
          "probe interrupted");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      return ServerHealthSnapshot.unavailable(
          serverMeta == null ? null : serverMeta.getName(),
          enabled,
          maxConcurrent,
          "Error querying Hop server : " + cause.getMessage());
    } finally {
      executor.shutdownNow();
    }
  }

  static void applyStatus(ServerHealthSnapshot snapshot, HopServerStatus status) {
    snapshot.setStatusDescription(status.getStatusDescription());
    snapshot.setShuttingDown(status.isShuttingDown());
    snapshot.setLoadAvg(status.getLoadAvg());
    snapshot.setMemoryFree(status.getMemoryFree());
    snapshot.setMemoryTotal(status.getMemoryTotal());
    snapshot.setCpuCores(status.getCpuCores());
    snapshot.setCpuProcessTime(status.getCpuProcessTime());
    snapshot.setOsName(status.getOsName());
    snapshot.setOsVersion(status.getOsVersion());
    snapshot.setOsArchitecture(status.getOsArchitecture());

    int runningPipelines = 0;
    int finishedPipelines = 0;
    int occupyingPipelines = 0;
    if (status.getPipelineStatusList() != null) {
      for (HopServerPipelineStatus pipelineStatus : status.getPipelineStatusList()) {
        if (pipelineStatus.isRunning()) {
          runningPipelines++;
        }
        if (pipelineStatus.isFinished()) {
          finishedPipelines++;
        }
        if (occupiesPipelineSlot(pipelineStatus)) {
          occupyingPipelines++;
        }
      }
    }
    int runningWorkflows = 0;
    int finishedWorkflows = 0;
    int occupyingWorkflows = 0;
    if (status.getWorkflowStatusList() != null) {
      for (HopServerWorkflowStatus workflowStatus : status.getWorkflowStatusList()) {
        if (workflowStatus.isRunning()) {
          runningWorkflows++;
        }
        if (workflowStatus.isFinished()) {
          finishedWorkflows++;
        }
        if (occupiesWorkflowSlot(workflowStatus)) {
          occupyingWorkflows++;
        }
      }
    }
    snapshot.setRunningPipelines(runningPipelines);
    snapshot.setFinishedPipelines(finishedPipelines);
    snapshot.setRunningWorkflows(runningWorkflows);
    snapshot.setFinishedWorkflows(finishedWorkflows);
    snapshot.setOccupyingSlots(occupyingPipelines + occupyingWorkflows);
  }

  static boolean occupiesPipelineSlot(HopServerPipelineStatus status) {
    return status != null && !status.isFinished() && !status.isStopped();
  }

  static boolean occupiesWorkflowSlot(HopServerWorkflowStatus status) {
    return status != null && !status.isFinished() && !status.isStopped();
  }
}
