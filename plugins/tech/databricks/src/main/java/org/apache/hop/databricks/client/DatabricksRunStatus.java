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

package org.apache.hop.databricks.client;

import java.util.Objects;

/** Snapshot of a Databricks job run. */
public final class DatabricksRunStatus {

  private final long runId;
  private final Long jobId;
  private final DatabricksRunLifeCycleState lifeCycleState;
  private final String resultState;
  private final String stateMessage;
  private final String runPageUrl;

  public DatabricksRunStatus(
      long runId,
      Long jobId,
      DatabricksRunLifeCycleState lifeCycleState,
      String resultState,
      String stateMessage,
      String runPageUrl) {
    this.runId = runId;
    this.jobId = jobId;
    this.lifeCycleState =
        Objects.requireNonNullElse(lifeCycleState, DatabricksRunLifeCycleState.UNKNOWN);
    this.resultState = resultState;
    this.stateMessage = stateMessage;
    this.runPageUrl = runPageUrl;
  }

  public long getRunId() {
    return runId;
  }

  public Long getJobId() {
    return jobId;
  }

  public DatabricksRunLifeCycleState getLifeCycleState() {
    return lifeCycleState;
  }

  /** e.g. SUCCESS, FAILED, TIMEDOUT, CANCELED when terminated. */
  public String getResultState() {
    return resultState;
  }

  public String getStateMessage() {
    return stateMessage;
  }

  public String getRunPageUrl() {
    return runPageUrl;
  }

  public boolean isSuccess() {
    return lifeCycleState == DatabricksRunLifeCycleState.TERMINATED
        && "SUCCESS".equalsIgnoreCase(resultState);
  }

  public boolean isFailed() {
    if (lifeCycleState == DatabricksRunLifeCycleState.INTERNAL_ERROR) {
      return true;
    }
    if (lifeCycleState != DatabricksRunLifeCycleState.TERMINATED) {
      return false;
    }
    return resultState != null
        && !"SUCCESS".equalsIgnoreCase(resultState)
        && !"CANCELED".equalsIgnoreCase(resultState);
  }

  /** Compact status string for Hop variables. */
  public String toStatusVariable() {
    if (lifeCycleState == DatabricksRunLifeCycleState.TERMINATED && resultState != null) {
      return resultState.toUpperCase();
    }
    return lifeCycleState.name();
  }
}
