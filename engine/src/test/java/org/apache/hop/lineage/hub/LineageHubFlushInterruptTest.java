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

package org.apache.hop.lineage.hub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.spi.ILineageSink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the reported LineageHub flush-interrupt bug. When the pipeline engine is
 * shutting down and interrupts the thread running the PipelineCompleted extension point, {@link
 * LineageHub#flush()} was blocked in {@code queue.put(request)} and surfaced the interrupt as a
 * full ERROR stack trace. The fix handles the interrupt gracefully (restore the flag, debug-level
 * log) and, crucially, the still-queued events are not lost: {@link LineageHub#shutdown()} drains
 * them.
 */
class LineageHubFlushInterruptTest {

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  @Test
  void flushInterruptedDuringShutdownIsGracefulAndLosesNoEvents() throws Exception {
    // Capacity 1 so the queue fills quickly; large linger so the worker parks in the sink.
    LineageConfiguration cfg = LineageConfiguration.forTesting(true, 1, 1, 30_000L, Set.of());

    Semaphore holdSink = new Semaphore(0);
    AtomicBoolean firstBatch = new AtomicBoolean(true);
    AtomicInteger persisted = new AtomicInteger();
    ILineageSink blocking =
        batch -> {
          // Park on the first event so a second event fills the single queue slot and the flush
          // then blocks in queue.put(); release on interrupt so shutdown can proceed.
          if (firstBatch.compareAndSet(true, false)) {
            try {
              holdSink.acquire();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          persisted.addAndGet(batch.size());
        };

    LineageHub hub = LineageHub.newIsolatedForTesting(cfg, List.of(blocking));

    // Event 1: worker pops it and parks in the sink. Event 2: fills the single queue slot.
    hub.emit(sampleEvent());
    Thread.sleep(150);
    hub.emit(sampleEvent());

    // The flusher plays the role of RemotePipelineEngine.fireExecutionFinishedListeners ->
    // LineageHubPipelineCompletedXp.flushQuietly -> flush(): queue.put(FlushRequest) blocks.
    AtomicBoolean interruptFlagRestored = new AtomicBoolean(false);
    int bufferBefore = HopLogStore.getAppender().getLastBufferLineNr();
    Thread flusher =
        new Thread(
            () -> {
              hub.flushQuietly();
              interruptFlagRestored.set(Thread.currentThread().isInterrupted());
            },
            "repro-pipeline-finish-thread");
    flusher.start();

    // Give the flusher time to reach the blocked queue.put(), then interrupt it the way the engine
    // shutdown does.
    Thread.sleep(300);
    flusher.interrupt();
    flusher.join(5_000);

    // The interrupted flush must restore the interrupt flag...
    assertTrue(
        interruptFlagRestored.get(), "flush() should restore the interrupt flag after catching");

    // ...and must NOT surface an ERROR stack trace for the interrupt.
    String loggedSinceFlush =
        HopLogStore.getAppender().getBuffer(null, true, bufferBefore).toString();
    assertFalse(
        loggedSinceFlush.contains("Interrupted while flushing lineage queue"),
        "interrupted flush must no longer be logged as an ERROR; was:\n" + loggedSinceFlush);
    assertFalse(
        loggedSinceFlush.contains("InterruptedException"),
        "interrupted flush must not dump an InterruptedException stack trace; was:\n"
            + loggedSinceFlush);

    // Now the process shuts down. The two emitted events were never lost by the failed flush and
    // are drained by shutdown() rather than dropped.
    holdSink.release(20);
    hub.shutdown();
    assertEquals(
        2, persisted.get(), "both emitted events must be delivered despite the interrupted flush");
  }

  private static LineageEvent sampleEvent() {
    return LineageEvent.of(LineageEventKind.RUN_LIFECYCLE, LineageContext.empty(), null);
  }
}
