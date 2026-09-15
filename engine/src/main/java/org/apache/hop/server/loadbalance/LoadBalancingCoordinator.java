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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.www.HopServerAtCapacityException;

/**
 * Reloads a load-balancing run configuration, probes the server group, and picks one server.
 *
 * @param <C> pipeline or workflow run configuration
 */
public class LoadBalancingCoordinator<C> {

  public static final long DEFAULT_PROBE_TIMEOUT_MS = 3000L;
  public static final long DEFAULT_CONFIG_REFRESH_MS = 10000L;

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final ILogChannel log;
  private final String runConfigurationName;
  private final boolean workflow;
  private final LoadBalancingAssignmentStore assignmentStore;

  private ILoadBalancingRunConfiguration currentConfig;
  private long lastReloadEpochMs;

  public LoadBalancingCoordinator(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      String runConfigurationName,
      boolean workflow) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = log;
    this.runConfigurationName = runConfigurationName;
    this.workflow = workflow;
    this.assignmentStore = new LoadBalancingAssignmentStore(variables, log);
  }

  public ILoadBalancingRunConfiguration reloadIfDue() throws HopException {
    long refreshMs =
        currentConfig == null
            ? 0L
            : Const.toLong(
                variables.resolve(currentConfig.getConfigRefreshIntervalMs()),
                DEFAULT_CONFIG_REFRESH_MS);
    if (currentConfig == null
        || refreshMs <= 0
        || System.currentTimeMillis() - lastReloadEpochMs >= refreshMs) {
      reload();
    }
    return currentConfig;
  }

  public ILoadBalancingRunConfiguration reload() throws HopException {
    if (metadataProvider == null) {
      throw new HopException("No metadata provider is available to reload the run configuration");
    }
    if (StringUtils.isEmpty(runConfigurationName)) {
      throw new HopException("The load-balancing run configuration has no name");
    }
    ILoadBalancingRunConfiguration loaded;
    if (workflow) {
      WorkflowRunConfiguration runConfiguration =
          metadataProvider.getSerializer(WorkflowRunConfiguration.class).load(runConfigurationName);
      if (runConfiguration == null
          || !(runConfiguration.getEngineRunConfiguration()
              instanceof ILoadBalancingRunConfiguration lb)) {
        throw new HopException(
            "Workflow run configuration '"
                + runConfigurationName
                + "' is not a load-balancing configuration");
      }
      loaded = lb;
    } else {
      PipelineRunConfiguration runConfiguration =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigurationName);
      if (runConfiguration == null
          || !(runConfiguration.getEngineRunConfiguration()
              instanceof ILoadBalancingRunConfiguration lb)) {
        throw new HopException(
            "Pipeline run configuration '"
                + runConfigurationName
                + "' is not a load-balancing configuration");
      }
      loaded = lb;
    }
    currentConfig = loaded;
    lastReloadEpochMs = System.currentTimeMillis();
    return currentConfig;
  }

  public ServerHealthSnapshot selectServer() throws HopException {
    ILoadBalancingRunConfiguration config = reloadIfDue();
    List<LoadBalancingServerEntry> servers = config.getServers();
    if (servers == null || servers.isEmpty()) {
      throw new HopException(
          "Load-balancing run configuration '"
              + runConfigurationName
              + "' has no Hop servers configured");
    }

    long probeTimeout =
        Const.toLong(variables.resolve(config.getProbeTimeoutMs()), DEFAULT_PROBE_TIMEOUT_MS);
    List<ServerHealthSnapshot> snapshots = probeServers(servers, probeTimeout);
    LoadBalancingAlgorithm algorithm =
        LoadBalancingAlgorithm.fromCodeOrDescription(variables.resolve(config.getAlgorithm()));
    Optional<ServerHealthSnapshot> selected = LoadBalancingSelector.select(algorithm, snapshots);
    if (selected.isEmpty()) {
      throw noEligibleServerException(snapshots);
    }
    ServerHealthSnapshot snapshot = selected.get();
    if (log != null) {
      log.logBasic(
          "Load-balancing selected Hop server '"
              + snapshot.getHopServerName()
              + "' using algorithm '"
              + algorithm.getCode()
              + "' ("
              + snapshot.getOccupyingSlots()
              + "/"
              + snapshot.getMaxConcurrent()
              + " slots, load "
              + snapshot.getLoadAvg()
              + ", response "
              + snapshot.getResponseNs()
              + "ns)");
    }
    return snapshot;
  }

  public void saveAssignment(LoadBalancingAssignment assignment) {
    if (currentConfig == null || assignment == null) {
      return;
    }
    assignmentStore.save(currentConfig.getStateFolder(), assignment);
  }

  public ILoadBalancingRunConfiguration getCurrentConfig() {
    return currentConfig;
  }

  List<ServerHealthSnapshot> probeServers(List<LoadBalancingServerEntry> servers, long probeTimeout)
      throws HopException {
    ExecutorService executor =
        Executors.newFixedThreadPool(Math.min(Math.max(1, servers.size()), 8));
    try {
      List<CompletableFuture<ServerHealthSnapshot>> futures = new ArrayList<>();
      for (LoadBalancingServerEntry entry : servers) {
        futures.add(CompletableFuture.supplyAsync(() -> probeOne(entry, probeTimeout), executor));
      }
      List<ServerHealthSnapshot> snapshots = new ArrayList<>();
      for (CompletableFuture<ServerHealthSnapshot> future : futures) {
        snapshots.add(future.join());
      }
      return snapshots;
    } finally {
      executor.shutdownNow();
    }
  }

  private ServerHealthSnapshot probeOne(LoadBalancingServerEntry entry, long probeTimeout) {
    String serverName = variables.resolve(entry.getHopServerName());
    int maxConcurrent = (int) Const.toLong(variables.resolve(entry.getMaxConcurrent()), 1L);
    if (StringUtils.isEmpty(serverName)) {
      return ServerHealthSnapshot.unavailable(
          serverName, entry.isEnabled(), maxConcurrent, "Hop server name is empty");
    }
    try {
      HopServerMeta serverMeta =
          metadataProvider.getSerializer(HopServerMeta.class).load(serverName);
      if (serverMeta == null) {
        return ServerHealthSnapshot.unavailable(
            serverName,
            entry.isEnabled(),
            maxConcurrent,
            "Hop server '" + serverName + "' not found");
      }
      return HopServerProbe.probeWithTimeout(
          serverMeta, variables, entry.isEnabled(), maxConcurrent, probeTimeout);
    } catch (Exception e) {
      return ServerHealthSnapshot.unavailable(
          serverName,
          entry.isEnabled(),
          maxConcurrent,
          "Error loading Hop server '" + serverName + "': " + e.getMessage());
    }
  }

  /**
   * When every reachable server is full, throw {@link HopServerAtCapacityException} so the engine
   * waits and retries. Unreachable or misconfigured groups stay a regular {@link HopException}.
   */
  static HopException noEligibleServerException(List<ServerHealthSnapshot> snapshots) {
    if (isGroupAtCapacity(snapshots)) {
      ServerHealthSnapshot atCapacity = firstAtCapacity(snapshots);
      int occupying = atCapacity == null ? 0 : atCapacity.getOccupyingSlots();
      int maxConcurrent = atCapacity == null ? 0 : atCapacity.getMaxConcurrent();
      return new HopServerAtCapacityException(occupying, maxConcurrent);
    }
    return new HopException(describeNoEligibleServer(snapshots));
  }

  static boolean isGroupAtCapacity(List<ServerHealthSnapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return false;
    }
    boolean anyAtCapacity = false;
    for (ServerHealthSnapshot snapshot : snapshots) {
      if (snapshot == null || !snapshot.isEnabled()) {
        continue;
      }
      if (snapshot.isEligible()) {
        return false;
      }
      if (isAtCapacity(snapshot)) {
        anyAtCapacity = true;
      }
    }
    return anyAtCapacity;
  }

  static boolean isAtCapacity(ServerHealthSnapshot snapshot) {
    return snapshot != null
        && snapshot.isEnabled()
        && snapshot.isAvailable()
        && !snapshot.isShuttingDown()
        && snapshot.getMaxConcurrent() > 0
        && snapshot.getOccupyingSlots() >= snapshot.getMaxConcurrent();
  }

  static ServerHealthSnapshot firstAtCapacity(List<ServerHealthSnapshot> snapshots) {
    if (snapshots == null) {
      return null;
    }
    for (ServerHealthSnapshot snapshot : snapshots) {
      if (isAtCapacity(snapshot)) {
        return snapshot;
      }
    }
    return null;
  }

  static String describeNoEligibleServer(List<ServerHealthSnapshot> snapshots) {
    StringBuilder message =
        new StringBuilder("No eligible Hop server in the load-balancing group:");
    if (snapshots == null || snapshots.isEmpty()) {
      message.append(" the server list is empty");
      return message.toString();
    }
    for (ServerHealthSnapshot snapshot : snapshots) {
      message
          .append(Const.CR)
          .append("  - ")
          .append(snapshot.getHopServerName())
          .append(": ")
          .append(snapshot.skipReason());
    }
    return message.toString();
  }
}
