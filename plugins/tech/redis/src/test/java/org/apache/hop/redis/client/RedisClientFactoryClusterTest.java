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

package org.apache.hop.redis.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class RedisClientFactoryClusterTest {

  @Test
  void countCoveredSlotsSumsAssignedSlots() {
    Partitions partitions = new Partitions();
    partitions.addPartition(nodeWithSlots(0, 5461, NodeFlag.UPSTREAM));
    partitions.addPartition(nodeWithSlots(5461, 10923, NodeFlag.UPSTREAM));
    partitions.addPartition(nodeWithSlots(10923, 16384, NodeFlag.UPSTREAM));
    assertEquals(16384, RedisClientFactory.countCoveredSlots(partitions));
  }

  @Test
  void countCoveredSlotsIgnoresFailedNodes() {
    Partitions partitions = new Partitions();
    partitions.addPartition(nodeWithSlots(0, 100, NodeFlag.FAIL));
    partitions.addPartition(nodeWithSlots(0, 50, NodeFlag.UPSTREAM));
    assertEquals(50, RedisClientFactory.countCoveredSlots(partitions));
  }

  @Test
  void summarizePartitionsHandlesNullAndEmpty() {
    assertEquals("(null)", RedisClientFactory.summarizePartitions(null));
    assertEquals("(none)", RedisClientFactory.summarizePartitions(new Partitions()));

    Partitions partitions = new Partitions();
    RedisClusterNode node = nodeWithSlots(0, 10, NodeFlag.UPSTREAM);
    node.setUri(RedisURI.create("redis://127.0.0.1:7000"));
    partitions.addPartition(node);
    String summary = RedisClientFactory.summarizePartitions(partitions);
    assertTrue(summary.contains("7000"));
    assertTrue(summary.contains("slots=10"));
  }

  @Test
  void parseNodesSupportsSemicolonAndWhitespace() {
    List<RedisClientFactory.HostPort> nodes = RedisClientFactory.parseNodes("a:1; b:2\nc:3", 6379);
    assertEquals(3, nodes.size());
    assertEquals("a", nodes.get(0).host());
    assertEquals(1, nodes.get(0).port());
    assertEquals("b", nodes.get(1).host());
    assertEquals(2, nodes.get(1).port());
    assertEquals("c", nodes.get(2).host());
    assertEquals(3, nodes.get(2).port());
  }

  private static RedisClusterNode nodeWithSlots(int fromInclusive, int toExclusive, NodeFlag flag) {
    RedisClusterNode node = new RedisClusterNode();
    List<Integer> slots = new ArrayList<>();
    IntStream.range(fromInclusive, toExclusive).forEach(slots::add);
    node.setSlots(slots);
    node.setFlags(EnumSet.of(flag));
    return node;
  }
}
