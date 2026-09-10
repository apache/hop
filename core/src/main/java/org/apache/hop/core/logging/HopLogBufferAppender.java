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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.EnvUtil;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

/**
 * log4j2 appender that mirrors log4j2 events into Hop's in-memory {@link LoggingBuffer} and, by
 * default, writes them to the console. It is the single element carrying the previous Hop console
 * behavior: non-error lines go to stdout, error lines go to stderr (optionally ANSI colored through
 * {@code HOP_CONSOLE_COLORS}), formatted with {@link FixedWidthLogLayout}. Read-side consumers
 * (servlets, database log tables, metrics, the log browser, ...) keep working while {@link
 * LogChannel} emits through SLF4J.
 *
 * <p>The originating hop channel id is taken from the {@code hop.logChannelId} MDC key so lines can
 * be grouped per channel; lines emitted before any channel is associated are treated as general.
 */
@Plugin(name = "HopLogBuffer", category = "Core", elementType = "appender", printObject = true)
public class HopLogBufferAppender extends AbstractAppender {

  /** ANSI color codes for terminal output. */
  private static final String ANSI_RESET = "\u001B[0m";

  private static final String ANSI_RED = "\u001B[31m";

  public static final String NAME = "HopLogBuffer";

  private static final HopLogBufferAppender INSTANCE = new HopLogBufferAppender();

  private final FixedWidthLogLayout layout;

  private final AtomicBoolean consoleEnabled;

  private final boolean useColors;

  @PluginFactory
  public static HopLogBufferAppender createAppender(
      @PluginAttribute("name") final String name, @PluginElement("Filter") final Filter filter) {
    return INSTANCE;
  }

  public HopLogBufferAppender() {
    super(NAME, null, null, true, null);
    this.layout = new FixedWidthLogLayout(true);
    this.consoleEnabled = new AtomicBoolean(true);
    this.useColors = resolveColors();
  }

  /**
   * @return the singleton; the appender is registered programmatically at bootstrap.
   */
  public static HopLogBufferAppender getInstance() {
    return INSTANCE;
  }

  /** Enable or disable the console output. The in-memory buffer is always fed regardless. */
  public void setConsoleEnabled(boolean enabled) {
    consoleEnabled.set(enabled);
  }

  private static boolean resolveColors() {
    String colorConfig = EnvUtil.getSystemProperty(Const.HOP_CONSOLE_COLORS);
    if (colorConfig == null) {
      colorConfig = System.getenv(Const.HOP_CONSOLE_COLORS);
    }
    if (colorConfig == null) {
      colorConfig = "auto";
    }
    colorConfig = colorConfig.toLowerCase();
    if ("true".equals(colorConfig)) {
      return true;
    }
    if ("false".equals(colorConfig)) {
      return false;
    }
    return System.console() != null;
  }

  @Override
  public void append(LogEvent event) {
    String channelId = event.getContextData().getValue(LogChannel.MDC_CHANNEL);
    if (channelId == null) {
      channelId = "General";
    }
    LoggingBuffer appender = HopLogStore.getAppender();
    if (appender == null) {
      return;
    }
    LogLevel hopLevel = toHopLevel(event.getLevel());
    String message = event.getMessage() == null ? "" : event.getMessage().getFormattedMessage();
    LogMessage logMessage = new LogMessage(message, channelId, hopLevel, false);
    HopLoggingEvent loggingEvent = new HopLoggingEvent(logMessage, event.getTimeMillis(), hopLevel);
    appender.addLogggingEvent(loggingEvent);

    if (consoleEnabled.get()) {
      writeToConsole(loggingEvent, hopLevel);
    }
  }

  private void writeToConsole(HopLoggingEvent event, LogLevel hopLevel) {
    String text = layout.format(event);
    if (hopLevel == LogLevel.ERROR) {
      if (useColors) {
        text = ANSI_RED + text + ANSI_RESET;
      }
      HopLogStore.OriginalSystemErr.println(text);
      HopLogStore.OriginalSystemErr.flush();
    } else {
      HopLogStore.OriginalSystemOut.println(text);
      HopLogStore.OriginalSystemOut.flush();
    }
  }

  private static LogLevel toHopLevel(org.apache.logging.log4j.Level level) {
    if (level == null) {
      return LogLevel.BASIC;
    }
    if (level.isMoreSpecificThan(org.apache.logging.log4j.Level.ERROR)
        && level != org.apache.logging.log4j.Level.OFF) {
      return LogLevel.ERROR;
    }
    if (level == org.apache.logging.log4j.Level.WARN) {
      return LogLevel.MINIMAL;
    }
    if (level == org.apache.logging.log4j.Level.INFO) {
      return LogLevel.BASIC;
    }
    if (level == org.apache.logging.log4j.Level.DEBUG) {
      return LogLevel.DEBUG;
    }
    if (level == org.apache.logging.log4j.Level.TRACE) {
      return LogLevel.ROWLEVEL;
    }
    return LogLevel.BASIC;
  }
}
