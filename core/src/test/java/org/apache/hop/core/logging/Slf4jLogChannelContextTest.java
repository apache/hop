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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.Const;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the SLF4J/log4j2 event context: every record emitted through a {@link LogChannel} must
 * carry the hop channel id, the exact hop log level code and the caller on the log4j2 event, so a
 * custom appender (agent, collector, Kafka, ...) can group lines and restore Hop's seven levels
 * without parsing text.
 */
class Slf4jLogChannelContextTest {

  private static final String APPENDER_NAME = "hopTestCapture";

  private static LoggerContext loggerContext;

  private static Configuration configuration;

  private static LoggerConfig root;

  private static CapturingAppender capturingAppender;

  private LogChannel channel;

  @BeforeAll
  static void initStore() {
    System.setProperty(Const.HOP_MAX_LOG_SIZE_IN_LINES, "10000");
    System.setProperty(Const.HOP_MAX_LOG_TIMEOUT_IN_MINUTES, "0");
    HopLogStore.init(false, false);

    loggerContext = (LoggerContext) LogManager.getContext(false);
    configuration = loggerContext.getConfiguration();
    root = configuration.getRootLogger();
    capturingAppender = new CapturingAppender();
    capturingAppender.start();
    root.addAppender(capturingAppender, null, null);
    loggerContext.updateLoggers();
  }

  @BeforeEach
  void setUpChannel() {
    capturingAppender.clear();
    channel = new LogChannel("context-" + System.nanoTime());
    HopLogStore.getAppender().clear();
  }

  @AfterEach
  void tearDownChannel() {
    HopLogStore.discardLines(channel.getLogChannelId(), true);
  }

  @Test
  void detailedEventCarriesChannelLevelAndCaller() {
    channel.setLogLevel(LogLevel.DETAILED);
    channel.logDetailed("detailed text");

    LogEvent event = readEvent("detailed text");

    assertEquals(
        channel.getLogChannelId(), event.getContextData().getValue(LogChannel.MDC_CHANNEL));
    assertEquals(
        LogLevel.DETAILED.getCode(), event.getContextData().getValue(LogChannel.MDC_LEVEL));
    assertNotNull(event.getContextData().getValue(LogChannel.MDC_CALLER));
  }

  @Test
  void rowLevelCodeIsPreservedOnTheEvent() {
    channel.setLogLevel(LogLevel.ROWLEVEL);
    channel.logRowlevel("row text");

    LogEvent event = readEvent("row text");

    assertEquals(
        LogLevel.ROWLEVEL.getCode(), event.getContextData().getValue(LogChannel.MDC_LEVEL));
    assertEquals(
        org.apache.logging.log4j.Level.TRACE, event.getLevel(), "ROWLEVEL maps to SLF4J trace");
  }

  @Test
  void debugCodeIsPreservedOnTheEvent() {
    channel.setLogLevel(LogLevel.DEBUG);
    channel.logDebug("debug text");

    LogEvent event = readEvent("debug text");

    assertEquals(LogLevel.DEBUG.getCode(), event.getContextData().getValue(LogChannel.MDC_LEVEL));
    assertEquals(org.apache.logging.log4j.Level.DEBUG, event.getLevel());
  }

  @Test
  void errorCarriesTheThrowableThroughTheEvent() {
    IllegalStateException failure = new IllegalStateException("kaboom");
    channel.setLogLevel(LogLevel.ERROR);
    channel.logError("boom", failure);

    LogEvent event = readEvent("boom");

    assertEquals(LogLevel.ERROR.getCode(), event.getContextData().getValue(LogChannel.MDC_LEVEL));
    assertNotNull(event.getThrown(), "the throwable must reach the SLF4J event");
    assertEquals(IllegalStateException.class, event.getThrown().getClass());
  }

  private LogEvent readEvent(String message) {
    List<LogEvent> events = capturingAppender.events();
    for (LogEvent event : events) {
      String formatted = event.getMessage() == null ? "" : event.getMessage().getFormattedMessage();
      if (message.equals(formatted)) {
        return event;
      }
    }
    throw new AssertionError("No captured event with message '" + message + "'");
  }

  /** Captures every log4j2 event routed to the root logger, in the emitting thread. */
  private static final class CapturingAppender extends AbstractAppender {

    private final CopyOnWriteArrayList<LogEvent> events;

    private CapturingAppender() {
      super(APPENDER_NAME, null, null, true, null);
      this.events = new CopyOnWriteArrayList<>();
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    private List<LogEvent> events() {
      return events;
    }

    private void clear() {
      events.clear();
    }
  }
}
