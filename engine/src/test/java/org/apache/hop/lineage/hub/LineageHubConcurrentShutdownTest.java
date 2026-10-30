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

package org.apache.hop.lineage.hub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.spi.ILineageSink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * When hop-run is stopped by a signal, its shutdown hook and its main thread both shut the hub
 * down. The second call may not cut off the delivery the first one is still doing.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LineageHubConcurrentShutdownTest {

  private static final int EVENTS = 5;
  private static final long DELIVERY_MS = 100;

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  @Test
  void secondShutdownWaitsForTheFirstToDeliverEverything() throws Exception {
    AtomicInteger delivered = new AtomicInteger();
    AtomicBoolean deliveredAfterSinkShutdown = new AtomicBoolean();
    AtomicBoolean sinkShutdown = new AtomicBoolean();
    ILineageSink slowSink =
        new ILineageSink() {
          @Override
          public void accept(List<LineageEvent> events) {
            // Spin rather than sleep: the hub interrupts its worker when it shuts down.
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DELIVERY_MS);
            while (System.nanoTime() < end) {
              Thread.onSpinWait();
            }
            if (sinkShutdown.get()) {
              deliveredAfterSinkShutdown.set(true);
            }
            delivered.addAndGet(events.size());
          }

          @Override
          public void shutdown() {
            sinkShutdown.set(true);
          }
        };
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 100, 1, 5000L, Set.of());
    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(slowSink));

    for (int i = 0; i < EVENTS; i++) {
      hub.emit(LineageEvent.of(LineageEventKind.RUN_LIFECYCLE, LineageContext.empty(), null));
    }

    Thread first = new Thread(hub::shutdown, "first-shutdown");
    first.start();
    // The first shutdown is delivering the queued events one by one.
    Thread.sleep(DELIVERY_MS + DELIVERY_MS / 2);
    hub.shutdown();
    first.join(20_000);

    assertEquals(EVENTS, delivered.get(), "Every queued event should reach the sink");
    assertFalse(
        deliveredAfterSinkShutdown.get(), "The sink was shut down while events were delivered");
  }
}
