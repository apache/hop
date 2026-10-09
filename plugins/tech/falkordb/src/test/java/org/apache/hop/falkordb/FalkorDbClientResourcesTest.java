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

package org.apache.hop.falkordb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Transports;
import io.netty.channel.EventLoopGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class FalkorDbClientResourcesTest {

  @Test
  void testClientsShareTheResources() {
    RedisClient first = FalkorDbClientResources.createClient(RedisURI.create("localhost", 6379));
    RedisClient second = FalkorDbClientResources.createClient(RedisURI.create("localhost", 6380));
    try {
      assertSame(FalkorDbClientResources.get(), first.getResources());
      assertSame(first.getResources(), second.getResources());
    } finally {
      first.shutdown();
      second.shutdown();
    }
  }

  /** Shutting a client down, the last one included, leaves the shared resources running. */
  @Test
  void testShutdownLeavesTheResourcesRunning() throws Exception {
    ClientResources resources = FalkorDbClientResources.get();
    Class<? extends EventLoopGroup> type = Transports.eventLoopGroupClass();
    // The group a client gets when it connects
    EventLoopGroup group = resources.eventLoopGroupProvider().allocate(type);
    resources.eventLoopGroupProvider().release(group, 0, 1, TimeUnit.SECONDS).await();

    FalkorDbClientResources.createClient(RedisURI.create("localhost", 6379)).shutdown();

    assertFalse(resources.eventExecutorGroup().isShuttingDown());
    assertFalse(group.isShuttingDown());
    assertSame(group, resources.eventLoopGroupProvider().allocate(type));
    resources.eventLoopGroupProvider().release(group, 0, 1, TimeUnit.SECONDS).await();
  }

  @Test
  void testCreatedOnce() throws Exception {
    List<ClientResources> seen = new ArrayList<>();
    CountDownLatch start = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  start.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                ClientResources resources = FalkorDbClientResources.get();
                synchronized (seen) {
                  seen.add(resources);
                }
              });
      thread.start();
      threads.add(thread);
    }
    start.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    for (ClientResources resources : seen) {
      assertSame(seen.get(0), resources);
    }
  }
}
