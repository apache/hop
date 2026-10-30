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

package org.apache.hop.run;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Stops the pipeline or workflow hop-run is executing when the JVM shuts down on a termination
 * signal (SIGTERM, SIGINT, SIGHUP), for example when Kubernetes or OpenShift drains a node. It
 * stops the execution the same way the Stop button in Hop GUI does, then waits for the run to end,
 * up to a timeout, before the JVM is allowed to exit. Then it runs the given task, which delivers
 * the lineage of the run. SIGKILL cannot be handled.
 */
public class HopRunShutdownHook implements Runnable {

  /** How often the hook checks whether the run has ended, and repeats the stop if needed. */
  static final long CHECK_INTERVAL_MS = 250L;

  /** How long the hook waits before it tries again when stopping the run failed. */
  static final long RETRY_INTERVAL_MS = 1000L;

  /** Longer timeouts are cut to this, so that the deadline can be computed in nanoseconds. */
  static final Duration MAX_TIMEOUT = Duration.ofDays(365L * 100);

  private final ILogChannel log;
  private final Duration timeout;
  private final Runnable afterStop;
  private final CountDownLatch runFinished = new CountDownLatch(1);

  private Execution execution;
  private boolean stopRequested;

  private record Execution(
      String description, Runnable stop, BooleanSupplier stopped, BooleanSupplier finished) {}

  /**
   * @param afterStop runs once the run has stopped, or the timeout has passed, before the JVM
   *     exits. The JVM exits as soon as the hook returns, so work the main thread still does after
   *     the run, like delivering lineage, has to happen here as well.
   */
  public HopRunShutdownHook(ILogChannel log, Duration timeout, Runnable afterStop) {
    this.log = log;
    this.timeout = timeout.compareTo(MAX_TIMEOUT) > 0 ? MAX_TIMEOUT : timeout;
    this.afterStop = afterStop;
  }

  /** The pipeline to stop on a termination signal. Call it once the pipeline has started. */
  public void track(IPipelineEngine<?> pipeline) {
    track(
        new Execution(
            "pipeline '" + pipeline.getPipelineMeta().getName() + "'",
            pipeline::stopAll,
            pipeline::isStopped,
            pipeline::isFinished));
  }

  /** The workflow to stop on a termination signal. Call it before the workflow starts. */
  public void track(IWorkflowEngine<?> workflow) {
    track(
        new Execution(
            "workflow '" + workflow.getWorkflowName() + "'",
            workflow::stopExecution,
            workflow::isStopped,
            workflow::isFinished));
  }

  private synchronized void track(Execution execution) {
    this.execution = execution;
  }

  /** Tells the hook the run is over, so it no longer needs to stop or wait for anything. */
  public void runFinished() {
    runFinished.countDown();
  }

  @Override
  public void run() {
    if (runFinished.getCount() == 0) {
      return;
    }
    synchronized (this) {
      if (stopRequested) {
        return;
      }
      stopRequested = true;
    }
    log.logMinimal(
        "Termination signal received, stopping the run. Waiting up to "
            + timeout.toSeconds()
            + " seconds for it to end.");
    try {
      stopAndWait();
    } finally {
      afterStop.run();
    }
  }

  private void stopAndWait() {
    long deadline = System.nanoTime() + timeout.toNanos();
    Execution target = null;
    boolean stopNeeded = false;
    boolean stopHeld = false;
    int failures = 0;
    long nextAttempt = System.nanoTime();
    try {
      do {
        Execution current;
        synchronized (this) {
          current = execution;
        }
        // The execution may only start after the signal came in.
        //
        if (current != null && current != target) {
          target = current;
          stopNeeded = true;
          stopHeld = false;
          failures = 0;
          nextAttempt = System.nanoTime();
        } else if (current != null && !stopNeeded) {
          // A workflow clears its stopped flag when it starts, which undoes a stop sent just
          // before. Only stop again when the flag was seen set and is cleared now: a remote engine
          // only sets it once the server reports the run as stopped.
          //
          boolean isStopped = current.stopped().getAsBoolean();
          if (stopHeld && !isStopped && !current.finished().getAsBoolean()) {
            stopNeeded = true;
          }
          stopHeld = isStopped;
        }
        if (stopNeeded && System.nanoTime() - nextAttempt >= 0) {
          if (failures == 0) {
            log.logMinimal("Stopping " + current.description());
          }
          try {
            current.stop().run();
            stopNeeded = false;
            stopHeld = current.stopped().getAsBoolean();
            failures = 0;
          } catch (RuntimeException e) {
            // For example a Hop server that can't be reached, or a remote run not submitted yet.
            failures++;
            String message =
                "Stopping " + current.description() + " failed, trying again in a second.";
            if (failures == 1) {
              log.logError(message, e);
            } else {
              log.logError(message);
            }
            nextAttempt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MS);
          }
        }
        if (runFinished.await(CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
          log.logMinimal("The run has stopped, hop-run exits.");
          return;
        }
      } while (System.nanoTime() - deadline < 0);
      log.logError(
          "The run did not stop within "
              + timeout.toSeconds()
              + " seconds, hop-run exits anyway. Use --shutdown-timeout or HOP_RUN_SHUTDOWN_TIMEOUT to wait longer.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
