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
package org.apache.hop.core.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.metrics.IMetricsSnapshot;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link MetricsRegistry} */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class MetricsRegistryTest {
  private MetricsRegistry metricsRegistry;
  private List<String> logIds;
  private final int threadCount = 100;
  private final int logChannelIdCount = 20;
  private CountDownLatch countDownLatch = null;

  @BeforeEach
  void setUp() {
    metricsRegistry = MetricsRegistry.getInstance();
    metricsRegistry.reset();
    logIds = new ArrayList<>(logChannelIdCount);
    for (int i = 1; i <= logChannelIdCount; i++) {
      logIds.add("logChannelId_" + i);
    }
    countDownLatch = new CountDownLatch(1);
  }

  @Test
  void testConcurrencySnap() throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      service.submit(new ConcurrentPutIfAbsent(logIds.get(i % 20)));
    }
    countDownLatch.countDown();
    service.awaitTermination(2000, TimeUnit.MILLISECONDS);
    int expectedQueueCount = Math.min(logChannelIdCount, threadCount);

    assertEquals(expectedQueueCount, metricsRegistry.getSnapshotLists().size());
  }

  private class ConcurrentPutIfAbsent implements Callable<Queue<IMetricsSnapshot>> {
    private final String id;

    ConcurrentPutIfAbsent(String id) {
      this.id = id;
    }

    @Override
    public Queue<IMetricsSnapshot> call() throws Exception {
      countDownLatch.await();
      return metricsRegistry.getSnapshotList(id);
    }
  }
}
