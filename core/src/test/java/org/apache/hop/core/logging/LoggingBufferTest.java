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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Unit test for {@link LoggingBuffer} */
class LoggingBufferTest {

  @Test
  void testRaceCondition() throws Exception {
    final int eventCount = 100;
    final LoggingBuffer buf = new LoggingBuffer(200);
    final AtomicBoolean done = new AtomicBoolean(false);

    final IHopLoggingEventListener lsnr =
        event -> {
          // stub
        };

    final HopLoggingEvent event = new HopLoggingEvent();
    final CountDownLatch latch = new CountDownLatch(1);

    Thread.UncaughtExceptionHandler errorHandler = (t, e) -> e.printStackTrace();

    Thread addListeners =
        new Thread(
            () -> {
              try {
                while (!done.get()) {
                  buf.addLoggingEventListener(lsnr);
                }
              } finally {
                latch.countDown();
              }
            },
            "Add Listeners Thread") {};

    Thread addEvents =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < eventCount; i++) {
                  buf.addLogggingEvent(event);
                }
                done.set(true);
              } finally {
                latch.countDown();
              }
            },
            "Add Events Thread") {};

    // add error handlers to pass exceptions outside the thread
    addListeners.setUncaughtExceptionHandler(errorHandler);
    addEvents.setUncaughtExceptionHandler(errorHandler);

    // start
    addListeners.start();
    addEvents.start();

    // wait both
    latch.await();

    // check
    assertTrue(done.get(), "Failed");
  }

  @Test
  void testRemoveBufferLinesBefore() {
    LoggingBuffer loggingBuffer = new LoggingBuffer(100);
    for (int i = 0; i < 40; i++) {
      HopLoggingEvent event = new HopLoggingEvent();
      event.setMessage(new LogMessage("test", LogLevel.BASIC));
      event.setTimeStamp(i);
      loggingBuffer.addLogggingEvent(event);
    }
    loggingBuffer.removeBufferLinesBefore(20);
    assertEquals(20, loggingBuffer.size());
  }

  /**
   * The maximum number of lines is kept while lines are added, dropping the oldest ones. This is
   * what the max_log_lines setting of a server is meant to limit.
   */
  @Test
  void maxNrLinesDropsTheOldestLines() {
    LoggingBuffer loggingBuffer = new LoggingBuffer(10);

    for (int i = 0; i < 25; i++) {
      loggingBuffer.addLogggingEvent(event("line " + i, i));
    }

    assertEquals(10, loggingBuffer.size(), "No more than the maximum number of lines is kept");
    List<String> kept = messagesOf(loggingBuffer);
    assertTrue(kept.contains("line 24"), "The newest line is kept");
    assertFalse(kept.contains("line 14"), "The oldest lines are dropped");
  }

  /** A maximum of zero lines means: no limit, which is what a server defaults to. */
  @Test
  void maxNrLinesOfZeroKeepsEveryLine() {
    LoggingBuffer loggingBuffer = new LoggingBuffer(0);

    for (int i = 0; i < 100; i++) {
      loggingBuffer.addLogggingEvent(event("line " + i, i));
    }

    assertEquals(100, loggingBuffer.size(), "Without a limit every line is kept");
  }

  /**
   * The two limits are independent of each other. Lines are dropped as soon as there are too many
   * of them, no matter how recent they are.
   */
  @Test
  void maxNrLinesIsReachedBeforeAnyLineTimesOut() {
    LoggingBuffer loggingBuffer = new LoggingBuffer(5);

    // Ten lines that all carry the same, recent, timestamp.
    long now = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      loggingBuffer.addLogggingEvent(event("line " + i, now));
    }

    assertEquals(5, loggingBuffer.size(), "The line limit applies to lines that did not time out");

    // Nothing times out yet, the lines are all recent.
    loggingBuffer.removeBufferLinesBefore(now - 60000L);
    assertEquals(5, loggingBuffer.size());
  }

  /** And the other way around: lines time out while there is still plenty of room in the buffer. */
  @Test
  void linesTimeOutBeforeMaxNrLinesIsReached() {
    LoggingBuffer loggingBuffer = new LoggingBuffer(1000);

    long now = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      loggingBuffer.addLogggingEvent(event("old line " + i, now - 120000L));
    }
    for (int i = 0; i < 10; i++) {
      loggingBuffer.addLogggingEvent(event("new line " + i, now));
    }
    assertEquals(20, loggingBuffer.size(), "The line limit is nowhere near reached");

    loggingBuffer.removeBufferLinesBefore(now - 60000L);

    assertEquals(10, loggingBuffer.size(), "Only the lines that timed out are removed");
    assertTrue(messagesOf(loggingBuffer).contains("new line 0"), "Recent lines are kept");
  }

  private static HopLoggingEvent event(String message, long timeStamp) {
    HopLoggingEvent event = new HopLoggingEvent();
    // The two argument constructor takes a subject, only this one takes a message.
    event.setMessage(new LogMessage(message, "a-log-channel", LogLevel.BASIC));
    event.setTimeStamp(timeStamp);
    return event;
  }

  /** The messages still in the buffer, to check which lines were kept and which were dropped. */
  private static List<String> messagesOf(LoggingBuffer loggingBuffer) {
    List<HopLoggingEvent> events =
        loggingBuffer.getLogBufferFromTo(
            (List<String>) null, true, 0, loggingBuffer.getLastBufferLineNr());
    return events.stream()
        .map(event -> ((LogMessage) event.getMessage()).getMessage())
        .collect(Collectors.toList());
  }

  @Test
  void testRemoveChannelFromBuffer() {
    String logChannelId = "1";
    String otherLogChannelId = "2";
    LoggingBuffer loggingBuffer = new LoggingBuffer(20);
    for (int i = 0; i < 10; i++) {
      HopLoggingEvent event = new HopLoggingEvent();
      event.setMessage(new LogMessage("testWithLogChannelId", logChannelId, LogLevel.BASIC));
      event.setTimeStamp(i);
      loggingBuffer.addLogggingEvent(event);
    }
    for (int i = 10; i < 17; i++) {
      HopLoggingEvent event = new HopLoggingEvent();
      event.setMessage(new LogMessage("testWithNoLogChannelId", LogLevel.BASIC));
      event.setTimeStamp(i);
      loggingBuffer.addLogggingEvent(event);
    }
    for (int i = 17; i < 20; i++) {
      HopLoggingEvent event = new HopLoggingEvent();
      event.setMessage(
          new LogMessage("testWithOtherLogChannelId", otherLogChannelId, LogLevel.BASIC));
      event.setTimeStamp(i);
      loggingBuffer.addLogggingEvent(event);
    }
    loggingBuffer.removeChannelFromBuffer(logChannelId);
    assertEquals(10, loggingBuffer.size());
  }
}
