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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.Const;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the real SLF4J/log4j2 pipeline: events emitted through a {@link LogChannel} must reach
 * Hop's in-memory buffer through the {@link HopLogBufferAppender} with the exact hop log level and
 * the pre-rendered stack trace preserved.
 */
class Slf4jLoggingPipelineTest {

  private LogChannel channel;

  @BeforeAll
  static void initStore() {
    System.setProperty(Const.HOP_MAX_LOG_SIZE_IN_LINES, "10000");
    System.setProperty(Const.HOP_MAX_LOG_TIMEOUT_IN_MINUTES, "0");
    HopLogStore.init(false, false);
    HopLogBufferAppender.getInstance().setConsoleEnabled(false);
  }

  @BeforeEach
  void setUpChannel() {
    channel = new LogChannel("pipeline-" + System.nanoTime());
    channel.setLogLevel(LogLevel.DETAILED);
    HopLogStore.getAppender().clear();
  }

  @AfterEach
  void tearDownChannel() {
    HopLogStore.discardLines(channel.getLogChannelId(), true);
  }

  @Test
  void detailedLevelSurvivesThroughThePipeline() {
    channel.logDetailed("message for fidelity");

    HopLoggingEvent event = readChannelEvent("message for fidelity");

    assertEquals(LogLevel.DETAILED, event.getLevel());
    assertEquals(LogLevel.DETAILED, ((LogMessage) event.getMessage()).getLevel());
  }

  @Test
  void rowLevelSurvivesThroughThePipeline() {
    channel.setLogLevel(LogLevel.ROWLEVEL);
    channel.logRowlevel("row payload");

    HopLoggingEvent event = readChannelEvent("row payload");

    assertEquals(LogLevel.ROWLEVEL, event.getLevel());
    assertEquals(LogLevel.ROWLEVEL, ((LogMessage) event.getMessage()).getLevel());
  }

  @Test
  void errorStackTraceSurvivesThroughThePipeline() {
    IllegalStateException failure = new IllegalStateException("kaboom");
    channel.logError("boom", failure);

    HopLoggingEvent event = readChannelEvent("boom");
    LogMessage logMessage = (LogMessage) event.getMessage();

    assertEquals(LogLevel.ERROR, event.getLevel());
    assertEquals(LogLevel.ERROR, logMessage.getLevel());
    assertNull(logMessage.getThrowable());
    assertNotNull(logMessage.getStackTrace());
    assertTrue(logMessage.getStackTrace().contains("IllegalStateException"));
    assertTrue(logMessage.getStackTrace().contains("kaboom"));
  }

  private HopLoggingEvent readChannelEvent(String message) {
    List<HopLoggingEvent> events =
        HopLogStore.getLogBufferFromTo(
            channel.getLogChannelId(), false, 0, HopLogStore.getLastBufferLineNr());
    return events.stream()
        .filter(event -> event.getMessage() instanceof LogMessage logMessage)
        .filter(event -> message.equals(((LogMessage) event.getMessage()).getMessage()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No buffered event with message '" + message + "'"));
  }
}
