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

import org.apache.hop.core.Const;
import org.apache.hop.core.metrics.MetricsSnapshot;
import org.apache.hop.core.metrics.IMetricsSnapshot;
import org.apache.hop.core.metrics.MetricsSnapshotType;
import org.apache.hop.core.util.Utils;

import java.util.Date;
import java.util.Map;
import java.util.Queue;

public class LogChannel implements ILogChannel {

  public static ILogChannel GENERAL = new LogChannel( "General" );

  public static ILogChannel UI = new LogChannel( "GUI" );

  private String logChannelId;

  private LogLevel logLevel;

  private String containerObjectId;

  private boolean gatheringMetrics;

  private boolean forcingSeparateLogging;

  private static MetricsRegistry metricsRegistry = MetricsRegistry.getInstance();

  private String filter;

  private LogChannelFileWriterBuffer fileWriter;

  private boolean simplified;

  public LogChannel( Object subject ) {
    logLevel = DefaultLogLevel.getLogLevel();
    logChannelId = LoggingRegistry.getInstance().registerLoggingSource( subject );
  }

  public LogChannel( Object subject, boolean gatheringMetrics ) {
    this( subject );
    this.gatheringMetrics = gatheringMetrics;
  }

  public LogChannel( Object subject, ILoggingObject parentObject ) {
    if ( parentObject != null ) {
      this.logLevel = parentObject.getLogLevel();
      this.containerObjectId = parentObject.getContainerId();
    } else {
      this.logLevel = DefaultLogLevel.getLogLevel();
      this.containerObjectId = null;
    }
    logChannelId = LoggingRegistry.getInstance().registerLoggingSource( subject );
  }

  public LogChannel( Object subject, ILoggingObject parentObject, boolean gatheringMetrics ) {
    this( subject, parentObject );
    this.gatheringMetrics = gatheringMetrics;
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
  public void println( ILogMessage logMessage, LogLevel channelLogLevel ) {
    String subject = null;

    LogLevel logLevel = logMessage.getLevel();

    if ( !logLevel.isVisible( channelLogLevel ) ) {
      return; // not for our eyes.
    }

    if ( subject == null ) {
      subject = "Hop";
    }

    // Are the message filtered?
    //
    if ( !logLevel.isError() && !Utils.isEmpty( filter ) ) {
      if ( subject.indexOf( filter ) < 0 && logMessage.toString().indexOf( filter ) < 0 ) {
        return; // "filter" not found in row: don't show!
      }
    }

    // Let's not keep everything...
    //
    if ( channelLogLevel.getLevel() >= logLevel.getLevel() ) {
      HopLoggingEvent loggingEvent = new HopLoggingEvent( logMessage, System.currentTimeMillis(), logLevel );
      HopLogStore.getAppender().addLogggingEvent( loggingEvent );

      if ( this.fileWriter == null ) {
        this.fileWriter = LoggingRegistry.getInstance().getLogChannelFileWriterBuffer( logChannelId );
      }

      // add to buffer
      if ( this.fileWriter != null ) {
        this.fileWriter.addEvent( loggingEvent );
      }
    }
  }

  public void println( ILogMessage message, Throwable e, LogLevel channelLogLevel ) {
    println( message, channelLogLevel );

    String stackTrace = Const.getStackTracker( e );
    LogMessage traceMessage = new LogMessage( stackTrace, message.getLogChannelId(), LogLevel.ERROR, simplified );
    println( traceMessage, channelLogLevel );
  }

  @Override
  public void logMinimal( String s ) {
    println( new LogMessage( s, logChannelId, LogLevel.MINIMAL, simplified ), logLevel );
  }

  @Override
  public void logBasic( String s ) {
    println( new LogMessage( s, logChannelId, LogLevel.BASIC, simplified ), logLevel );
  }

  @Override
  public void logError( String s ) {
    println( new LogMessage( s, logChannelId, LogLevel.ERROR, simplified ), logLevel );
  }

  @Override
  public void logError( String s, Throwable e ) {
    println( new LogMessage( s, logChannelId, LogLevel.ERROR, simplified ), e, logLevel );
  }

  @Override
  public void logBasic( String s, Object... arguments ) {
    println( new LogMessage( s, logChannelId, arguments, LogLevel.BASIC, simplified ), logLevel );
  }

  @Override
  public void logDetailed( String s, Object... arguments ) {
    println( new LogMessage( s, logChannelId, arguments, LogLevel.DETAILED, simplified ), logLevel );
  }

  @Override
  public void logError( String s, Object... arguments ) {
    println( new LogMessage( s, logChannelId, arguments, LogLevel.ERROR, simplified ), logLevel );
  }

  @Override
  public void logDetailed( String s ) {
    println( new LogMessage( s, logChannelId, LogLevel.DETAILED, simplified ), logLevel );
  }

  @Override
  public void logDebug( String s ) {
    println( new LogMessage( s, logChannelId, LogLevel.DEBUG, simplified ), logLevel );
  }

  @Override
  public void logDebug( String message, Object... arguments ) {
    println( new LogMessage( message, logChannelId, arguments, LogLevel.DEBUG, simplified ), logLevel );
  }

  @Override
  public void logRowlevel( String s ) {
    println( new LogMessage( s, logChannelId, LogLevel.ROWLEVEL, simplified ), logLevel );
  }

  @Override
  public void logMinimal( String message, Object... arguments ) {
    println( new LogMessage( message, logChannelId, arguments, LogLevel.MINIMAL, simplified ), logLevel );
  }

  @Override
  public void logRowlevel( String message, Object... arguments ) {
    println( new LogMessage( message, logChannelId, arguments, LogLevel.ROWLEVEL, simplified ), logLevel );
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
    } catch ( NullPointerException ex ) {
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
  public void setLogLevel( LogLevel logLevel ) {
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
  public void setContainerObjectId( String containerObjectId ) {
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
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    this.gatheringMetrics = gatheringMetrics;
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return forcingSeparateLogging;
  }

  @Override
  public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
    this.forcingSeparateLogging = forcingSeparateLogging;
  }

  @Override
  public void snap( IMetrics metric, long... value ) {
    snap( metric, null, value );
  }

  @Override
  public void snap( IMetrics metric, String subject, long... value ) {
    if ( !isGatheringMetrics() ) {
      return;
    }

    String key = MetricsSnapshot.getKey( metric, subject );
    Map<String, IMetricsSnapshot> metricsMap = null;
    IMetricsSnapshot snapshot = null;
    Queue<IMetricsSnapshot> metricsList = null;
    switch ( metric.getType() ) {
      case MAX:
        // Calculate and store the maximum value for this metric
        //
        if ( value.length != 1 ) {
          break; // ignore
        }

        metricsMap = metricsRegistry.getSnapshotMap( logChannelId );
        snapshot = metricsMap.get( key );
        if ( snapshot != null ) {
          if ( value[ 0 ] > snapshot.getValue() ) {
            snapshot.setValue( value[ 0 ] );
            snapshot.setDate( new Date() );
          }
        } else {
          snapshot = new MetricsSnapshot( MetricsSnapshotType.MAX, metric, subject, value[ 0 ], logChannelId );
          metricsMap.put( key, snapshot );
        }

        break;
      case MIN:
        // Calculate and store the minimum value for this metric
        //
        if ( value.length != 1 ) {
          break; // ignore
        }

        metricsMap = metricsRegistry.getSnapshotMap( logChannelId );
        snapshot = metricsMap.get( key );
        if ( snapshot != null ) {
          if ( value[ 0 ] < snapshot.getValue() ) {
            snapshot.setValue( value[ 0 ] );
            snapshot.setDate( new Date() );
          }
        } else {
          snapshot = new MetricsSnapshot( MetricsSnapshotType.MIN, metric, subject, value[ 0 ], logChannelId );
          metricsMap.put( key, snapshot );
        }

        break;
      case SUM:
        metricsMap = metricsRegistry.getSnapshotMap( logChannelId );
        snapshot = metricsMap.get( key );
        if ( snapshot != null ) {
          snapshot.setValue( snapshot.getValue() + value[ 0 ] );
        } else {
          snapshot = new MetricsSnapshot( MetricsSnapshotType.SUM, metric, subject, value[ 0 ], logChannelId );
          metricsMap.put( key, snapshot );
        }

        break;
      case COUNT:
        metricsMap = metricsRegistry.getSnapshotMap( logChannelId );
        snapshot = metricsMap.get( key );
        if ( snapshot != null ) {
          snapshot.setValue( snapshot.getValue() + 1L );
        } else {
          snapshot = new MetricsSnapshot( MetricsSnapshotType.COUNT, metric, subject, 1L, logChannelId );
          metricsMap.put( key, snapshot );
        }

        break;
      case START:
        metricsList = metricsRegistry.getSnapshotList( logChannelId );
        snapshot = new MetricsSnapshot( MetricsSnapshotType.START, metric, subject, 1L, logChannelId );
        metricsList.add( snapshot );

        break;
      case STOP:
        metricsList = metricsRegistry.getSnapshotList( logChannelId );
        snapshot = new MetricsSnapshot( MetricsSnapshotType.STOP, metric, subject, 1L, logChannelId );
        metricsList.add( snapshot );

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
  public void setFilter( String filter ) {
    this.filter = filter;
  }

  /**
   * Gets simplified
   *
   * @return value of simplified
   */
  public boolean isSimplified() {
    return simplified;
  }

  /**
   * @param simplified The simplified to set
   */
  public void setSimplified( boolean simplified ) {
    this.simplified = simplified;
  }
}
