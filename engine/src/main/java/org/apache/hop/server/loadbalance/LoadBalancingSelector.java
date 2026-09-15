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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** Picks one eligible Hop Server using the configured algorithm. */
public final class LoadBalancingSelector {

  private LoadBalancingSelector() {}

  public static Optional<ServerHealthSnapshot> select(
      LoadBalancingAlgorithm algorithm, List<ServerHealthSnapshot> snapshots) {
    List<ServerHealthSnapshot> eligible = new ArrayList<>();
    if (snapshots != null) {
      for (ServerHealthSnapshot snapshot : snapshots) {
        if (snapshot != null && snapshot.isEligible()) {
          eligible.add(snapshot);
        }
      }
    }
    if (eligible.isEmpty()) {
      return Optional.empty();
    }
    LoadBalancingAlgorithm resolved =
        algorithm == null ? LoadBalancingAlgorithm.EVEN_LOAD : algorithm;
    Comparator<ServerHealthSnapshot> comparator =
        resolved == LoadBalancingAlgorithm.PACK ? packOrder() : evenLoadOrder();
    eligible.sort(comparator);
    return Optional.of(eligible.get(0));
  }

  static Comparator<ServerHealthSnapshot> evenLoadOrder() {
    return Comparator.comparingDouble(ServerHealthSnapshot::utilization)
        .thenComparingDouble(ServerHealthSnapshot::loadPerCore)
        .thenComparingLong(ServerHealthSnapshot::getResponseNs)
        .thenComparing(s -> s.getHopServerName() == null ? "" : s.getHopServerName());
  }

  static Comparator<ServerHealthSnapshot> packOrder() {
    return Comparator.comparingDouble(ServerHealthSnapshot::utilization)
        .reversed()
        .thenComparingLong(ServerHealthSnapshot::getResponseNs)
        .thenComparing(s -> s.getHopServerName() == null ? "" : s.getHopServerName());
  }
}
