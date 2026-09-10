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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.plugins.Plugin;

/**
 * log4j2 appender that mirrors log4j2 events into a VFS file, formatted with {@link HopLogLayout}.
 * It replaces the previous {@code LogChannelFileWriter} / {@code FileLoggingEventListener}
 * plumbing: the originating hop channel id is taken from the {@code hop.logChannelId} MDC key so
 * lines can be filtered per channel. When constructed with a non-null {@code logChannelId} only
 * events belonging to that channel (or one of its descendants in the {@link LoggingRegistry} tree)
 * are written; a null {@code logChannelId} writes every event.
 *
 * <p>Consumers create an appender, attach it to the root logger, run, then {@link #stop()} it to
 * detach and close the underlying stream.
 */
@Plugin(name = "HopFile", category = "Core", elementType = "appender", printObject = true)
public class HopFileAppender extends AbstractAppender {

  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private final String logChannelId;

  private final OutputStream outputStream;

  private final FileObject logFile;

  private final HopLogLayout layout;

  private volatile Throwable exception;

  private volatile boolean closed;

  /**
   * @param name unique appender name used to register it on the root logger
   * @param logChannelId channel to write, or {@code null} to write every channel
   * @param logFile destination file
   * @param append whether to append to an existing file
   */
  public HopFileAppender(
      final String name, final String logChannelId, final FileObject logFile, final boolean append)
      throws HopException {
    super(name, null, null, true, null);
    this.logChannelId = logChannelId;
    this.logFile = logFile;
    this.layout = new HopLogLayout(true);
    try {
      this.outputStream = HopVfs.getOutputStream(logFile, append);
    } catch (final IOException e) {
      throw new HopException("Unable to open log file '" + logFile + "'", e);
    }
  }

  /**
   * Opens {@code logFile} and returns a new appender with a unique name, ready to be attached.
   *
   * @param logChannelId channel to write, or {@code null} to write every channel
   * @param logFile destination file
   * @param append whether to append to an existing file
   */
  public static HopFileAppender create(
      final String logChannelId, final FileObject logFile, final boolean append)
      throws HopException {
    final String name = HopFileAppender.class.getSimpleName() + "-" + SEQUENCE.incrementAndGet();
    return new HopFileAppender(name, logChannelId, logFile, append);
  }

  /** Attach this appender to the root logger so it starts receiving events. */
  public void attach() {
    final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    final Configuration config = context.getConfiguration();
    final LoggerConfig root = config.getRootLogger();
    if (!root.getAppenders().containsKey(getName())) {
      root.addAppender(this, null, null);
      context.updateLoggers();
    }
  }

  /** Detach from the root logger and close the underlying stream. Safe to call multiple times. */
  public void stop() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
    }
    final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    final Configuration config = context.getConfiguration();
    final LoggerConfig root = config.getRootLogger();
    root.removeAppender(getName());
    context.updateLoggers();
    try {
      outputStream.flush();
      outputStream.close();
    } catch (final IOException e) {
      if (exception == null) {
        exception = e;
      }
    }
  }

  @Override
  public void append(final LogEvent event) {
    if (closed) {
      return;
    }
    String channelId = event.getContextData().getValue(LogChannel.MDC_CHANNEL);
    if (channelId == null) {
      channelId = "General";
    }
    if (logChannelId != null && !isChildChannel(channelId)) {
      return;
    }
    try {
      final LogLevel hopeLevel = toHopLevel(event.getLevel());
      final String message =
          event.getMessage() == null ? "" : event.getMessage().getFormattedMessage();
      final LogMessage logMessage = new LogMessage(message, channelId, hopeLevel, false);
      final HopLoggingEvent loggingEvent =
          new HopLoggingEvent(logMessage, event.getTimeMillis(), hopeLevel);
      outputStream.write(layout.format(loggingEvent).getBytes(Const.UTF_8));
      outputStream.write(Const.CR.getBytes(Const.UTF_8));
      outputStream.flush();
    } catch (final IOException e) {
      if (exception == null) {
        exception = e;
      }
    }
  }

  private boolean isChildChannel(final String channelId) {
    if (channelId == null) {
      return false;
    }
    final List<String> children = LoggingRegistry.getInstance().getLogChannelChildren(logChannelId);
    return children != null && children.contains(channelId);
  }

  private static LogLevel toHopLevel(final org.apache.logging.log4j.Level level) {
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

  /**
   * @return the destination file.
   */
  public FileObject getLogFile() {
    return logFile;
  }

  /**
   * @return the first exception raised while writing, or {@code null} if none occurred.
   */
  public Throwable getException() {
    return exception;
  }

  /**
   * @return the channel this appender filters on, or {@code null} for all channels.
   */
  public String getLogChannelId() {
    return logChannelId;
  }
}
