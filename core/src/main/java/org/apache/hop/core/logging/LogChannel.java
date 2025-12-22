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

public class LogChannel implements ILogChannel {

  public static ILogChannel GENERAL = new LogChannel("General");

  public static ILogChannel UI = new LogChannel("GUI");

  private final String logChannelId;

  private LogLevel logLevel;

  private String containerObjectId;

  private boolean gatheringMetrics;

  private boolean forcingSeparateLogging;

  private static final MetricsRegistry metricsRegistry = MetricsRegistry.getInstance();

  private String filter;

  private LogChannelFileWriterBuffer fileWriter;

  @Setter @Getter private boolean simplified;

  public LogChannel(Object subject) {
    logLevel = DefaultLogLevel.getLogLevel();
    logChannelId = LoggingRegistry.getInstance().registerLoggingSource(subject);
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
    if (parentObject != null) {
      this.logLevel = parentObject.getLogLevel();
      this.containerObjectId = parentObject.getContainerId();
    } else {
      this.logLevel = DefaultLogLevel.getLogLevel();
      this.containerObjectId = null;
    }
    this.gatheringMetrics = gatheringMetrics;

    logChannelId =
        LoggingRegistry.getInstance().registerLoggingSource(subject, forceNewLoggingEntry);
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
    String subject = null;

    LogLevel logLevel = logMessage.getLevel();

    if (!logLevel.isVisible(channelLogLevel)) {
      return; // not for our eyes.
    }

    if (subject == null) {
      subject = "Hop";
    }

    // Are the message filtered?
    //
    if (!logLevel.isError()
        && !Utils.isEmpty(filter)
        && subject.indexOf(filter) < 0
        && logMessage.toString().indexOf(filter) < 0) {

      return; // "filter" not found in row: don't show!
    }

    // Let's not keep everything...
    //
    if (channelLogLevel.getLevel() >= logLevel.getLevel()) {
      HopLoggingEvent loggingEvent =
          new HopLoggingEvent(logMessage, System.currentTimeMillis(), logLevel);
      HopLogStore.getAppender().addLogggingEvent(loggingEvent);

      if (this.fileWriter == null) {
        this.fileWriter = LoggingRegistry.getInstance().getLogChannelFileWriterBuffer(logChannelId);
      }

      // add to buffer
      if (this.fileWriter != null) {
        this.fileWriter.addEvent(loggingEvent);
      }
    }
  }

  public void println(ILogMessage message, Throwable e, LogLevel channelLogLevel) {
    println(message, channelLogLevel);

    String stackTrace = Const.getStackTracker(e);
    LogMessage traceMessage =
        new LogMessage(stackTrace, message.getLogChannelId(), LogLevel.ERROR, simplified);
    println(traceMessage, channelLogLevel);
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
