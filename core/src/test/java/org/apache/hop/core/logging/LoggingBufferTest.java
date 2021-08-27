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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoggingBufferTest {

  @Test
  public void testRaceCondition() throws Exception {

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
    Assert.assertEquals("Failed", true, done.get());
  }

  @Test
  public void testRemoveBufferLinesBefore() {
    LoggingBuffer loggingBuffer = new LoggingBuffer(100);
    for (int i = 0; i < 40; i++) {
      HopLoggingEvent event = new HopLoggingEvent();
      event.setMessage(new LogMessage("test", LogLevel.BASIC));
      event.setTimeStamp(i);
      loggingBuffer.addLogggingEvent(event);
    }
    loggingBuffer.removeBufferLinesBefore(20);
    Assert.assertEquals(20, loggingBuffer.size());
  }

  @Test
  public void testRemoveChannelFromBuffer() {
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
    Assert.assertEquals(10, loggingBuffer.size());
  }
}
