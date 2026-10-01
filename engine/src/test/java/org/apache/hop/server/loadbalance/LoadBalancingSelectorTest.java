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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LoadBalancingSelectorTest {

  @Test
  void evenLoadPicksTheLeastUtilizedServer() {
    ServerHealthSnapshot busy = eligible("busy", 3, 4, 0.1, 10);
    ServerHealthSnapshot idle = eligible("idle", 0, 4, 2.0, 50);
    Optional<ServerHealthSnapshot> selected =
        LoadBalancingSelector.select(LoadBalancingAlgorithm.EVEN_LOAD, List.of(busy, idle));
    assertTrue(selected.isPresent());
    assertEquals("idle", selected.get().getHopServerName());
  }

  @Test
  void packPicksTheFullestEligibleServer() {
    ServerHealthSnapshot busy = eligible("busy", 3, 4, 2.0, 50);
    ServerHealthSnapshot idle = eligible("idle", 0, 4, 0.1, 10);
    Optional<ServerHealthSnapshot> selected =
        LoadBalancingSelector.select(LoadBalancingAlgorithm.PACK, List.of(busy, idle));
    assertTrue(selected.isPresent());
    assertEquals("busy", selected.get().getHopServerName());
  }

  @Test
  void skipsDisabledDownShuttingDownAndFullServers() {
    ServerHealthSnapshot disabled = eligible("disabled", 0, 4, 0, 1);
    disabled.setEnabled(false);
    ServerHealthSnapshot down = ServerHealthSnapshot.unavailable("down", true, 4, "timeout");
    ServerHealthSnapshot draining = eligible("draining", 0, 4, 0, 1);
    draining.setShuttingDown(true);
    ServerHealthSnapshot full = eligible("full", 4, 4, 0, 1);
    ServerHealthSnapshot ok = eligible("ok", 1, 4, 0, 1);

    Optional<ServerHealthSnapshot> selected =
        LoadBalancingSelector.select(
            LoadBalancingAlgorithm.EVEN_LOAD, List.of(disabled, down, draining, full, ok));
    assertTrue(selected.isPresent());
    assertEquals("ok", selected.get().getHopServerName());
  }

  @Test
  void emptyWhenNothingIsEligible() {
    Optional<ServerHealthSnapshot> selected =
        LoadBalancingSelector.select(
            LoadBalancingAlgorithm.EVEN_LOAD,
            List.of(ServerHealthSnapshot.unavailable("a", true, 4, "down")));
    assertTrue(selected.isEmpty());
  }

  private static ServerHealthSnapshot eligible(
      String name, int occupying, int max, double loadAvg, long responseNs) {
    ServerHealthSnapshot snapshot = new ServerHealthSnapshot();
    snapshot.setHopServerName(name);
    snapshot.setEnabled(true);
    snapshot.setAvailable(true);
    snapshot.setOccupyingSlots(occupying);
    snapshot.setMaxConcurrent(max);
    snapshot.setLoadAvg(loadAvg);
    snapshot.setCpuCores(1);
    snapshot.setResponseNs(responseNs);
    return snapshot;
  }
}
