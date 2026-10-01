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

import lombok.Getter;
import lombok.Setter;

/** Result of probing one Hop Server for load-balancing (and Get Server Status). */
@Getter
@Setter
public class ServerHealthSnapshot {

  private String hopServerName;
  private boolean enabled;
  private int maxConcurrent;
  private boolean available;
  private boolean shuttingDown;
  private String statusDescription;
  private String errorMessage;
  private Double loadAvg;
  private Long memoryFree;
  private Long memoryTotal;
  private Integer cpuCores;
  private Long cpuProcessTime;
  private String osName;
  private String osVersion;
  private String osArchitecture;
  private int runningPipelines;
  private int finishedPipelines;
  private int runningWorkflows;
  private int finishedWorkflows;
  private int occupyingSlots;
  private long responseNs;

  public static ServerHealthSnapshot unavailable(
      String hopServerName, boolean enabled, int maxConcurrent, String errorMessage) {
    ServerHealthSnapshot snapshot = new ServerHealthSnapshot();
    snapshot.hopServerName = hopServerName;
    snapshot.enabled = enabled;
    snapshot.maxConcurrent = maxConcurrent;
    snapshot.available = false;
    snapshot.errorMessage = errorMessage;
    return snapshot;
  }

  public boolean isEligible() {
    return enabled
        && available
        && !shuttingDown
        && maxConcurrent > 0
        && occupyingSlots < maxConcurrent;
  }

  public double utilization() {
    if (maxConcurrent <= 0) {
      return Double.POSITIVE_INFINITY;
    }
    return (double) occupyingSlots / (double) maxConcurrent;
  }

  public double loadPerCore() {
    int cores = cpuCores == null || cpuCores <= 0 ? 1 : cpuCores;
    double load = loadAvg == null ? 0.0 : loadAvg;
    return load / cores;
  }

  public String skipReason() {
    if (!enabled) {
      return "disabled";
    }
    if (!available) {
      return errorMessage == null ? "unreachable" : errorMessage;
    }
    if (shuttingDown) {
      return "shutting down";
    }
    if (maxConcurrent <= 0) {
      return "max concurrent is " + maxConcurrent;
    }
    if (occupyingSlots >= maxConcurrent) {
      return "at capacity (" + occupyingSlots + "/" + maxConcurrent + ")";
    }
    return null;
  }
}
