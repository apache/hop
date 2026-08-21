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

package org.apache.hop.execution;

import java.util.function.BooleanSupplier;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.engine.IPipelineEngine;

/**
 * Wait helpers for nested pipeline and workflow execution with an optional millisecond timeout.
 *
 * <p>Empty, blank, {@code 0} or a non-numeric timeout means wait indefinitely.
 */
public final class ExecutionWait {

  private static final long POLL_MS = 50L;

  private ExecutionWait() {
    // utility
  }

  /**
   * Resolve and parse a wait-timeout specification.
   *
   * @param variables variable space used to resolve the spec
   * @param spec milliseconds as a number or variable expression
   * @return timeout in milliseconds, or {@code 0} for no limit
   */
  public static long parseTimeoutMs(IVariables variables, String spec) {
    String resolved = Const.trim(variables == null ? spec : variables.resolve(Const.NVL(spec, "")));
    if (Utils.isEmpty(resolved)) {
      return 0L;
    }
    long value = Const.toLong(resolved, 0L);
    return Math.max(0L, value);
  }

  /**
   * Wait until {@code done} or {@code abort} is true, or until {@code timeoutMs} elapses.
   *
   * @param done returns true when the work finished
   * @param abort optional extra stop condition (parent stopped); may be {@code null}
   * @param timeoutMs maximum wait in milliseconds; {@code <= 0} means no limit
   * @return {@code false} if the timeout elapsed; {@code true} if done or aborted first
   */
  public static boolean waitFor(BooleanSupplier done, BooleanSupplier abort, long timeoutMs) {
    long deadline = timeoutMs <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
    while (!safeGet(done) && !safeGet(abort)) {
      if (timeoutMs > 0 && System.currentTimeMillis() >= deadline) {
        return false;
      }
      try {
        Thread.sleep(POLL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  /**
   * Wait for a pipeline using the engine's own {@code waitUntilFinished()}. When a positive timeout
   * elapses the pipeline is stopped and this method waits until that stop completes.
   *
   * @return {@code true} if the pipeline finished (or was already stopped) before the timeout;
   *     {@code false} if the timeout elapsed and the pipeline was stopped because of it
   */
  public static boolean waitForPipeline(IPipelineEngine<?> pipeline, long timeoutMs) {
    if (pipeline == null) {
      return true;
    }
    if (timeoutMs <= 0) {
      pipeline.waitUntilFinished();
      return true;
    }

    Thread waiter =
        new Thread(pipeline::waitUntilFinished, "wait-pipeline-" + pipeline.getLogChannelId());
    waiter.setDaemon(true);
    waiter.start();
    try {
      waiter.join(timeoutMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (!waiter.isAlive()) {
      return true;
    }

    pipeline.stopAll();
    joinQuietly(waiter);
    return false;
  }

  /**
   * Wait for a worker thread to finish. On timeout the caller is expected to stop the engine
   * running on that thread, then join the thread.
   *
   * @return {@code false} if the timeout elapsed while the thread was still alive
   */
  public static boolean waitForThread(Thread thread, BooleanSupplier abort, long timeoutMs) {
    if (thread == null) {
      return true;
    }
    long deadline = timeoutMs <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
    while (thread.isAlive() && !safeGet(abort)) {
      if (timeoutMs > 0 && System.currentTimeMillis() >= deadline) {
        return false;
      }
      joinQuietly(thread, POLL_MS);
    }
    return !thread.isAlive() || safeGet(abort);
  }

  public static void joinQuietly(Thread thread) {
    joinQuietly(thread, 0L);
  }

  public static void joinQuietly(Thread thread, long millis) {
    if (thread == null) {
      return;
    }
    try {
      if (millis > 0) {
        thread.join(millis);
      } else {
        thread.join();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static boolean safeGet(BooleanSupplier supplier) {
    return supplier != null && supplier.getAsBoolean();
  }
}
