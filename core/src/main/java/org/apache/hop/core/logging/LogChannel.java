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

import java.util.Date;
import java.util.Map;
import java.util.Queue;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.metrics.IMetricsSnapshot;
import org.apache.hop.core.metrics.MetricsSnapshot;
import org.apache.hop.core.metrics.MetricsSnapshotType;
import org.apache.hop.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.CallerBoundaryAware;
import org.slf4j.spi.LoggingEventBuilder;

/**
 * The default Hop log channel, backed by SLF4J. Hop's in-memory {@link LoggingBuffer} is kept in
 * sync by the {@code HopLogBufferAppender} registered on the SLF4J/log4j2 backend so read-side
 * consumers (servlets, database log tables, metrics, log browsers, ...) keep working.
 *
 * <p>While a message is logged the hop context (the caller - the logger name / subject, the channel
 * id and the log level code) travels as key/value pairs on the emitted SLF4J event - not as mutable
 * thread-context (MDC) values - so backends can render the origin and restore Hop's 7 levels
 * exactly instead of approximating them from the 5 SLF4J levels. The {@link CallerBoundaryAware}
 * boundary is set to this facade so backends that capture file/line information resolve it to the
 * real hop call-site instead of this class. Capturing such location info stays opt-in on the
 * backend side (a location-aware appender) and costs nothing otherwise.
 */
public class LogChannel implements ILogChannel {

  /**
   * Key under which the hop caller (logger name / subject) is published on the event while a
   * message is logged.
   */
  public static final String MDC_CALLER = "hop.caller";

  /** Key under which the hop log channel id is published on the event while a message is logged. */
  public static final String MDC_CHANNEL = "hop.logChannelId";

  /**
   * Key under which the hop {@link LogLevel} code is published on the event while a message is
   * logged.
   */
  public static final String MDC_LEVEL = "hop.logLevel";

  public static ILogChannel GENERAL = new LogChannel("General");

  public static ILogChannel UI = new LogChannel("GUI");

  private final String logChannelId;

  private final Logger logger;

  /** The {@link #MDC_CALLER} value: the logger name, i.e. the hop subject that logged. */
  private final String caller;

  private LogLevel logLevel;

  private String containerObjectId;

  private boolean gatheringMetrics;

  private boolean forcingSeparateLogging;

  private static final MetricsRegistry metricsRegistry = MetricsRegistry.getInstance();

  private String filter;

  @Setter @Getter private boolean simplified;

  public LogChannel(Object subject) {
    this.logLevel = DefaultLogLevel.getLogLevel();
    this.logChannelId = LoggingRegistry.getInstance().registerLoggingSource(subject);
    this.logger = LoggerFactory.getLogger(loggerName(this.logChannelId));
    this.caller = this.logger.getName();
  }

  public LogChannel(Object subject, boolean gatheringMetrics) {
    this(subject);
    this.gatheringMetrics = gatheringMetrics;
  }

  public LogChannel(Object subject, ILoggingObject parentObject) {
    this(subject, parentObject, false, false);
  }

  public LogChannel(Object subject, ILoggingObject parentObject, boolean gatheringMetrics) {
    this(subject, parentObject, gatheringMetrics, false);
  }

  public LogChannel(
      Object subject,
      ILoggingObject parentObject,
      boolean gatheringMetrics,
      boolean forceNewLoggingEntry) {
    this.logChannelId =
        LoggingRegistry.getInstance().registerLoggingSource(subject, forceNewLoggingEntry);
    if (parentObject != null) {
      this.logLevel = parentObject.getLogLevel();
      this.containerObjectId = parentObject.getContainerId();
    } else {
      this.logLevel = DefaultLogLevel.getLogLevel();
      this.containerObjectId = null;
    }
    this.gatheringMetrics = gatheringMetrics;
    this.logger = LoggerFactory.getLogger(loggerName(this.logChannelId));
    this.caller = this.logger.getName();
  }

  private static String loggerName(String channelId) {
    ILoggingObject loggingObject = LoggingRegistry.getInstance().getLoggingObject(channelId);
    String detailed = LoggingObjectNameHelper.getDetailedSubject(loggingObject);
    if (Utils.isEmpty(detailed)) {
      return "org.apache.hop";
    }
    return "org.apache.hop." + detailed.replace('\\', '.');
  }

  @Override
  public String toString() {
    return logChannelId;
  }

  @Override
  public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * @param logMessage
   * @param channelLogLevel
   */
  public void println(ILogMessage logMessage, LogLevel channelLogLevel) {
    LogLevel logLevel = logMessage.getLevel();

    if (!logLevel.isVisible(channelLogLevel)) {
      return; // not for our eyes.
    }

    // Are the message filtered?
    //
    if (!logLevel.isError() && !Utils.isEmpty(filter) && !messageContainedInFilter(logMessage)) {
      return; // "filter" not found in row: don't show!
    }

    // Emit to the SLF4J backend. Hop's in-memory buffer is fed back through the
    // HopLogBufferAppender. The channel id, the caller (logger name) and the exact hop level travel
    // as key/value pairs on the event so backends can group lines per channel and restore the 7 hop
    // levels. The caller boundary tells location-aware backends where the real call-site starts.
    //
    LoggingEventBuilder builder = logToSf4j(logMessage, logLevel);
    if (builder == null) {
      return; // nothing to emit (e.g. NOTHING level)
    }
    if (builder instanceof CallerBoundaryAware boundaryAware) {
      boundaryAware.setCallerBoundary(LogChannel.class.getName());
    }
    builder.log();
  }

  public void println(ILogMessage message, Throwable e, LogLevel channelLogLevel) {
    if (message instanceof LogMessage logMessage) {
      if (e != null) {
        // Pre-render the trace (bounded memory), attach the throwable for SLF4J, and bump to ERROR.
        logMessage.setStackTrace(Const.getStackTracker(e));
        logMessage.setThrowable(e);
        logMessage.setLevel(LogLevel.ERROR);
      }
      println(message, channelLogLevel);
      // Release the throwable so the buffered event doesn't retain the exception graph.
      logMessage.setThrowable(null);
    } else {
      // Non-standard ILogMessage can't carry a throwable: fall back to the legacy two-event path.
      println(message, channelLogLevel);
      String stackTrace = Const.getStackTracker(e);
      LogMessage traceMessage =
          new LogMessage(stackTrace, message.getLogChannelId(), LogLevel.ERROR, simplified);
      println(traceMessage, channelLogLevel);
    }
  }

  public void logWithLevel(String s, LogLevel logMessageLevel) {
    if (logMessageLevel.isVisible(logLevel)) {
      println(new LogMessage(s, logChannelId, logMessageLevel, simplified), logLevel);
    }
  }

  public void logWithLevel(String s, Throwable e, LogLevel logMessageLevel) {
    if (logMessageLevel.isVisible(logLevel)) {
      println(new LogMessage(s, logChannelId, logMessageLevel, simplified), e, logLevel);
    }
  }

  public void logWithLevel(String s, LogLevel logMessageLevel, Object... arguments) {
    if (logMessageLevel.isVisible(logLevel)) {
      println(new LogMessage(s, logChannelId, arguments, logMessageLevel, simplified), logLevel);
    }
  }

  private boolean messageContainedInFilter(ILogMessage message) {
    return message.toString().indexOf(filter) >= 0;
  }

  private LoggingEventBuilder logToSf4j(ILogMessage message, LogLevel level) {
    String text = message.getMessage();
    LoggingEventBuilder builder;
    switch (level) {
      case ERROR:
        builder = logger.atError();
        break;
      case MINIMAL:
        builder = logger.atWarn();
        break;
      case BASIC, DETAILED:
        builder = logger.atInfo();
        break;
      case DEBUG:
        builder = logger.atDebug();
        break;
      case ROWLEVEL:
        builder = logger.atTrace();
        break;
      case NOTHING:
      default:
        return null;
    }
    builder.addKeyValue(MDC_CHANNEL, logChannelId);
    builder.addKeyValue(MDC_LEVEL, level.getCode());
    builder.addKeyValue(MDC_CALLER, caller);
    builder.setMessage(text);
    Throwable throwable = message.getThrowable();
    if (throwable != null) {
      builder.setCause(throwable);
    }
    return builder;
  }

  @Override
  public void logMinimal(String s) {
    logWithLevel(s, LogLevel.MINIMAL);
  }

  @Override
  public void logBasic(String s) {
    logWithLevel(s, LogLevel.BASIC);
  }

  @Override
  public void logError(String s) {
    logWithLevel(s, LogLevel.ERROR);
  }

  @Override
  public void logError(String s, Throwable e) {
    logWithLevel(s, e, LogLevel.ERROR);
  }

  @Override
  public void logBasic(String s, Object... arguments) {
    logWithLevel(s, LogLevel.BASIC, arguments);
  }

  @Override
  public void logDetailed(String s, Object... arguments) {
    logWithLevel(s, LogLevel.DETAILED, arguments);
  }

  @Override
  public void logError(String s, Object... arguments) {
    logWithLevel(s, LogLevel.ERROR, arguments);
  }

  @Override
  public void logDetailed(String s) {
    logWithLevel(s, LogLevel.DETAILED);
  }

  @Override
  public void logDebug(String s) {
    logWithLevel(s, LogLevel.DEBUG);
  }

  @Override
  public void logDebug(String message, Object... arguments) {
    logWithLevel(message, LogLevel.DEBUG, arguments);
  }

  @Override
  public void logRowlevel(String s) {
    logWithLevel(s, LogLevel.ROWLEVEL);
  }

  @Override
  public void logMinimal(String message, Object... arguments) {
    logWithLevel(message, LogLevel.MINIMAL, arguments);
  }

  @Override
  public void logRowlevel(String message, Object... arguments) {
    logWithLevel(message, LogLevel.ROWLEVEL, arguments);
  }

  @Override
  public boolean isBasic() {
    return logLevel.isBasic();
  }

  @Override
  public boolean isDebug() {
    return logLevel.isDebug();
  }

  @Override
  public boolean isDetailed() {
    try {
      return logLevel.isDetailed();
    } catch (NullPointerException ex) {
      return false;
    }
  }

  @Override
  public boolean isRowLevel() {
    return logLevel.isRowlevel();
  }

  @Override
  public boolean isError() {
    return logLevel.isError();
  }

  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  @Override
  public void setLogLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
  }

  /**
   * @return the containerObjectId
   */
  @Override
  public String getContainerObjectId() {
    return containerObjectId;
  }

  /**
   * @param containerObjectId the containerObjectId to set
   */
  @Override
  public void setContainerObjectId(String containerObjectId) {
    this.containerObjectId = containerObjectId;
  }

  /**
   * @return the gatheringMetrics
   */
  @Override
  public boolean isGatheringMetrics() {
    return gatheringMetrics;
  }

  /**
   * @param gatheringMetrics the gatheringMetrics to set
   */
  @Override
  public void setGatheringMetrics(boolean gatheringMetrics) {
    this.gatheringMetrics = gatheringMetrics;
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return forcingSeparateLogging;
  }

  @Override
  public void setForcingSeparateLogging(boolean forcingSeparateLogging) {
    this.forcingSeparateLogging = forcingSeparateLogging;
  }

  @Override
  public void snap(IMetrics metric, long... value) {
    snap(metric, null, value);
  }

  @Override
  public void snap(IMetrics metric, String subject, long... value) {
    if (!isGatheringMetrics()) {
      return;
    }

    String key = MetricsSnapshot.getKey(metric, subject);
    Map<String, IMetricsSnapshot> metricsMap = null;
    IMetricsSnapshot snapshot = null;
    Queue<IMetricsSnapshot> metricsList = null;
    switch (metric.getType()) {
      case MAX:
        // Calculate and store the maximum value for this metric
        //
        if (value.length != 1) {
          break; // ignore
        }

        metricsMap = metricsRegistry.getSnapshotMap(logChannelId);
        snapshot = metricsMap.get(key);
        if (snapshot != null) {
          if (value[0] > snapshot.getValue()) {
            snapshot.setValue(value[0]);
            snapshot.setDate(new Date());
          }
        } else {
          snapshot =
              new MetricsSnapshot(MetricsSnapshotType.MAX, metric, subject, value[0], logChannelId);
          metricsMap.put(key, snapshot);
        }

        break;
      case MIN:
        // Calculate and store the minimum value for this metric
        //
        if (value.length != 1) {
          break; // ignore
        }

        metricsMap = metricsRegistry.getSnapshotMap(logChannelId);
        snapshot = metricsMap.get(key);
        if (snapshot != null) {
          if (value[0] < snapshot.getValue()) {
            snapshot.setValue(value[0]);
            snapshot.setDate(new Date());
          }
        } else {
          snapshot =
              new MetricsSnapshot(MetricsSnapshotType.MIN, metric, subject, value[0], logChannelId);
          metricsMap.put(key, snapshot);
        }

        break;
      case SUM:
        metricsMap = metricsRegistry.getSnapshotMap(logChannelId);
        snapshot = metricsMap.get(key);
        if (snapshot != null) {
          snapshot.setValue(snapshot.getValue() + value[0]);
        } else {
          snapshot =
              new MetricsSnapshot(MetricsSnapshotType.SUM, metric, subject, value[0], logChannelId);
          metricsMap.put(key, snapshot);
        }

        break;
      case COUNT:
        metricsMap = metricsRegistry.getSnapshotMap(logChannelId);
        snapshot = metricsMap.get(key);
        if (snapshot != null) {
          snapshot.setValue(snapshot.getValue() + 1L);
        } else {
          snapshot =
              new MetricsSnapshot(MetricsSnapshotType.COUNT, metric, subject, 1L, logChannelId);
          metricsMap.put(key, snapshot);
        }

        break;
      case START:
        metricsList = metricsRegistry.getSnapshotList(logChannelId);
        snapshot =
            new MetricsSnapshot(MetricsSnapshotType.START, metric, subject, 1L, logChannelId);
        metricsList.add(snapshot);

        break;
      case STOP:
        metricsList = metricsRegistry.getSnapshotList(logChannelId);
        snapshot = new MetricsSnapshot(MetricsSnapshotType.STOP, metric, subject, 1L, logChannelId);
        metricsList.add(snapshot);

        break;
      default:
        break;
    }
  }

  @Override
  public String getFilter() {
    return filter;
  }

  @Override
  public void setFilter(String filter) {
    this.filter = filter;
  }
}
