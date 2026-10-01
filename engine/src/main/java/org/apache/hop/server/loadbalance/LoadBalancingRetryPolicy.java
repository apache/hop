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

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;

/** Tracks retry attempts against a max count and an optional time window. */
public class LoadBalancingRetryPolicy {

  private final int maxRetries;
  private final long retryWindowMs;
  private final long firstAttemptEpochMs;
  private int attempt;

  public LoadBalancingRetryPolicy(int maxRetries, long retryWindowMs) {
    this(maxRetries, retryWindowMs, System.currentTimeMillis());
  }

  public LoadBalancingRetryPolicy(int maxRetries, long retryWindowMs, long firstAttemptEpochMs) {
    this.maxRetries = Math.max(0, maxRetries);
    this.retryWindowMs = Math.max(0, retryWindowMs);
    this.firstAttemptEpochMs = firstAttemptEpochMs;
    this.attempt = 0;
  }

  public static LoadBalancingRetryPolicy from(
      ILoadBalancingRunConfiguration config, IVariables variables) {
    int maxRetries = (int) Const.toLong(variables.resolve(config.getMaxRetries()), 2L);
    long windowMs = Const.toLong(variables.resolve(config.getRetryWindowMs()), 0L);
    return new LoadBalancingRetryPolicy(maxRetries, windowMs);
  }

  public boolean canAttempt() {
    if (attempt > maxRetries) {
      return false;
    }
    if (retryWindowMs > 0 && attempt > 0) {
      return System.currentTimeMillis() - firstAttemptEpochMs < retryWindowMs;
    }
    return true;
  }

  public int beginAttempt() {
    attempt++;
    return attempt;
  }

  public int getAttempt() {
    return attempt;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public long getRetryWindowMs() {
    return retryWindowMs;
  }

  public long getFirstAttemptEpochMs() {
    return firstAttemptEpochMs;
  }

  public long remainingWindowMs() {
    if (retryWindowMs <= 0) {
      return Long.MAX_VALUE;
    }
    return Math.max(0L, retryWindowMs - (System.currentTimeMillis() - firstAttemptEpochMs));
  }
}
