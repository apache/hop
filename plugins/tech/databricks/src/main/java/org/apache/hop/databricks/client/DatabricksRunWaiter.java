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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;

/**
 * Polls {@link DatabricksJobsClient#getRun(long)} until the run is terminal, timed out, or the
 * workflow is stopped. Shared by Job Run and Job Wait actions.
 */
public final class DatabricksRunWaiter {

  @FunctionalInterface
  public interface StatusListener {
    void onStatus(DatabricksRunStatus status);
  }

  public interface Hooks {
    /** True when the parent workflow was stopped. */
    boolean isStopped();

    void onStatus(DatabricksRunStatus status);

    void logDetailed(String message);

    void logError(String message);
  }

  private DatabricksRunWaiter() {}

  /**
   * @param timeoutSec 0 or negative = no timeout
   * @param pollSec minimum 1 second between polls
   * @param cancelOnStop if true, call cancelRun when stopped or on timeout
   */
  public static DatabricksRunStatus waitFor(
      DatabricksJobsClient client,
      long runId,
      int timeoutSec,
      int pollSec,
      boolean cancelOnStop,
      Hooks hooks)
      throws HopException, InterruptedException {
    int poll = Math.max(1, pollSec);
    long deadline =
        timeoutSec <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutSec * 1000L;

    while (true) {
      if (hooks != null && hooks.isStopped()) {
        if (cancelOnStop) {
          tryCancel(client, runId, hooks);
        }
        throw new HopException("Workflow was stopped while waiting for Databricks run " + runId);
      }

      DatabricksRunStatus status = client.getRun(runId);
      if (hooks != null) {
        hooks.onStatus(status);
        hooks.logDetailed(
            "Databricks run "
                + runId
                + " state="
                + status.getLifeCycleState()
                + " result="
                + Const.NVL(status.getResultState(), "-"));
      }

      if (status.getLifeCycleState().isTerminal()) {
        return status;
      }

      if (System.currentTimeMillis() >= deadline) {
        if (cancelOnStop) {
          tryCancel(client, runId, hooks);
        }
        throw new HopException(
            "Timed out after " + timeoutSec + " seconds waiting for Databricks run " + runId);
      }

      Thread.sleep(poll * 1000L);
    }
  }

  private static void tryCancel(DatabricksJobsClient client, long runId, Hooks hooks) {
    try {
      client.cancelRun(runId);
    } catch (Exception e) {
      if (hooks != null) {
        hooks.logError("Failed to cancel Databricks run " + runId + ": " + e.getMessage());
      }
    }
  }
}
