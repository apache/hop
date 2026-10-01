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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LoadBalancingRetryPolicyTest {

  @Test
  void allowsFirstAttemptAndConfiguredRetries() {
    LoadBalancingRetryPolicy retry = new LoadBalancingRetryPolicy(2, 0);
    assertTrue(retry.canAttempt());
    assertEquals(1, retry.beginAttempt());
    assertTrue(retry.canAttempt());
    assertEquals(2, retry.beginAttempt());
    assertTrue(retry.canAttempt());
    assertEquals(3, retry.beginAttempt());
    assertFalse(retry.canAttempt());
  }

  @Test
  void windowOfZeroMeansCountOnly() {
    LoadBalancingRetryPolicy retry = new LoadBalancingRetryPolicy(0, 0);
    assertTrue(retry.canAttempt());
    retry.beginAttempt();
    assertFalse(retry.canAttempt());
  }

  @Test
  void expiredWindowStopsFurtherAttempts() {
    LoadBalancingRetryPolicy retry =
        new LoadBalancingRetryPolicy(10, 1, System.currentTimeMillis() - 50);
    assertTrue(retry.canAttempt());
    retry.beginAttempt();
    assertFalse(retry.canAttempt());
  }
}
