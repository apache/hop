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

package org.apache.hop.server.loadbalance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.www.HopServerAtCapacityException;
import org.junit.jupiter.api.Test;

class LoadBalancingCoordinatorTest {

  @Test
  void groupAtCapacityWhenEveryReachableServerIsFull() {
    ServerHealthSnapshot fullA = eligible("a", 2, 2);
    ServerHealthSnapshot fullB = eligible("b", 2, 2);
    assertTrue(LoadBalancingCoordinator.isGroupAtCapacity(List.of(fullA, fullB)));
    HopException error = LoadBalancingCoordinator.noEligibleServerException(List.of(fullA, fullB));
    assertInstanceOf(HopServerAtCapacityException.class, error);
    assertEquals(2, ((HopServerAtCapacityException) error).getOccupyingSlots());
    assertEquals(2, ((HopServerAtCapacityException) error).getMaxConcurrent());
  }

  @Test
  void groupAtCapacityWhenSomeAreUnreachableAndOthersAreFull() {
    ServerHealthSnapshot down = ServerHealthSnapshot.unavailable("down", true, 2, "timeout");
    ServerHealthSnapshot full = eligible("full", 2, 2);
    assertTrue(LoadBalancingCoordinator.isGroupAtCapacity(List.of(down, full)));
    assertInstanceOf(
        HopServerAtCapacityException.class,
        LoadBalancingCoordinator.noEligibleServerException(List.of(down, full)));
  }

  @Test
  void notAtCapacityWhenAServerStillHasASlot() {
    ServerHealthSnapshot full = eligible("full", 2, 2);
    ServerHealthSnapshot open = eligible("open", 1, 2);
    assertFalse(LoadBalancingCoordinator.isGroupAtCapacity(List.of(full, open)));
  }

  @Test
  void unreachableGroupIsARegularException() {
    ServerHealthSnapshot down = ServerHealthSnapshot.unavailable("down", true, 2, "timeout");
    assertFalse(LoadBalancingCoordinator.isGroupAtCapacity(List.of(down)));
    HopException error = LoadBalancingCoordinator.noEligibleServerException(List.of(down));
    assertFalse(error instanceof HopServerAtCapacityException);
    assertTrue(error.getMessage().contains("No eligible Hop server"));
  }

  private static ServerHealthSnapshot eligible(String name, int occupying, int max) {
    ServerHealthSnapshot snapshot = new ServerHealthSnapshot();
    snapshot.setHopServerName(name);
    snapshot.setEnabled(true);
    snapshot.setAvailable(true);
    snapshot.setOccupyingSlots(occupying);
    snapshot.setMaxConcurrent(max);
    return snapshot;
  }
}
