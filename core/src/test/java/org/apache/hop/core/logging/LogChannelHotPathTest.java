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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hop.core.Const;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Measures the per-line cost of {@link LogChannel#logBasic(String)} on the row hot path. The number
 * is printed for the reviewer and asserted below a generous ceiling so a gross regression (for
 * example re-introducing an expensive formatting or synchronization step) fails the build instead
 * of silently slowing pipelines down. The measured value on a laptop is on the order of one to two
 * microseconds per line once the JIT has warmed up.
 */
class LogChannelHotPathTest {

  private static final int WARMUP_LINES = 200_000;

  private static final int MEASURED_LINES = 1_000_000;

  private static final long MAX_NANOS_PER_LINE = TimeUnit.MICROSECONDS.toNanos(10);

  private LogChannel channel;

  @BeforeAll
  static void initStore() {
    // Large enough to never prune the buffer during the run: LoggingBuffer retains lines with an
    // ArrayList remove(0), which is O(n) per append once the cap is hit. Keeping the cap above the
    // measured burst isolates LogChannel's own per-message cost from that retention behaviour.
    System.setProperty(Const.HOP_MAX_LOG_SIZE_IN_LINES, "2000000");
    System.setProperty(Const.HOP_MAX_LOG_TIMEOUT_IN_MINUTES, "0");
    HopLogStore.init(false, false);
  }

  @Test
  void logBasicOnWarmedUpChannelStaysFast() {
    channel = new LogChannel("hotpath-" + System.nanoTime());
    channel.setLogLevel(LogLevel.BASIC);
    HopLogBufferAppender.getInstance().setConsoleEnabled(false);

    for (int i = 0; i < WARMUP_LINES; i++) {
      channel.logBasic("warmup " + i);
    }

    long start = System.nanoTime();
    for (int i = 0; i < MEASURED_LINES; i++) {
      channel.logBasic("row " + i);
    }
    long elapsed = System.nanoTime() - start;

    double nanosPerLine = (double) elapsed / MEASURED_LINES;
    System.out.printf(
        "LogChannel.logBasic hot path: %.2f ns/line (%.0f ns for %d lines)%n",
        nanosPerLine, nanosPerLine, MEASURED_LINES);

    assertTrue(
        nanosPerLine < MAX_NANOS_PER_LINE,
        "expected logBasic below " + MAX_NANOS_PER_LINE + " ns/line, was " + nanosPerLine);
  }
}
