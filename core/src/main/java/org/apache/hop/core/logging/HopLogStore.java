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
import org.apache.hop.core.util.EnvUtil;

import java.io.PrintStream;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class HopLogStore {

  public static PrintStream OriginalSystemOut = System.out;
  public static PrintStream OriginalSystemErr = System.err;

  private static HopLogStore store;

  private LoggingBuffer appender;

  private Timer logCleanerTimer;

  private static AtomicBoolean initialized = new AtomicBoolean( false );

  private static ILogChannelFactory logChannelFactory = new LogChannelFactory();

  public static ILogChannelFactory getLogChannelFactory() {
    return logChannelFactory;
  }

  public static void setLogChannelFactory( ILogChannelFactory logChannelFactory ) {
    HopLogStore.logChannelFactory = logChannelFactory;
  }

  /**
   * Create the central log store with optional limitation to the size
   *
   * @param maxSize              the maximum size
   * @param maxLogTimeoutMinutes The maximum time that a log line times out in Minutes.
   */
  private HopLogStore( int maxSize, int maxLogTimeoutMinutes, boolean redirectStdOut, boolean redirectStdErr ) {
    this.appender = new LoggingBuffer( maxSize );
    replaceLogCleaner( maxLogTimeoutMinutes );

    if ( redirectStdOut ) {
      System.setOut( new LoggingPrintStream( OriginalSystemOut ) );
    }

    if ( redirectStdErr ) {
      System.setErr( new LoggingPrintStream( OriginalSystemErr ) );
    }
  }

  public void replaceLogCleaner( final int maxLogTimeoutMinutes ) {
    if ( logCleanerTimer != null ) {
      logCleanerTimer.cancel();
    }
    logCleanerTimer = new Timer( true );

    TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {

        if ( maxLogTimeoutMinutes > 0 ) {
          long minTimeBoundary = new Date().getTime() - maxLogTimeoutMinutes * 60 * 1000;

          // Remove all the old lines.
          appender.removeBufferLinesBefore( minTimeBoundary );
        }
      }
    };

    // Clean out the rows every 10 seconds to get a nice steady purge operation...
    //
    logCleanerTimer.schedule( timerTask, 10000, 10000 );

  }

  /**
   * Initialize the central log store with optional limitation to the size and redirect of stdout and stderr
   *
   * @param maxSize              the maximum size
   * @param maxLogTimeoutMinutes the maximum time that a log line times out in hours
   * @param redirectStdOut       a boolean indicating whether to redirect stdout to the logging framework
   * @param redirectStdErr       a boolean indicating whether to redirect stderr to the logging framework
   */
  public static void init( int maxSize, int maxLogTimeoutMinutes, boolean redirectStdOut, boolean redirectStdErr ) {
    if ( maxSize > 0 || maxLogTimeoutMinutes > 0 ) {
      init0( maxSize, maxLogTimeoutMinutes, redirectStdOut, redirectStdErr );
    } else {
      init( redirectStdOut, redirectStdErr );
    }
  }

  public static void init() {
    init( EnvUtil.getSystemProperty( Const.HOP_REDIRECT_STDOUT, "N" ).equalsIgnoreCase( "Y" ), EnvUtil
      .getSystemProperty( Const.HOP_REDIRECT_STDERR, "N" ).equalsIgnoreCase( "Y" ) );
  }

  /**
   * Initialize the central log store with arguments specifying whether to redirect of stdout and stderr
   *
   * @param redirectStdOut a boolean indicating whether to redirect stdout to the logging framework
   * @param redirectStdErr a boolean indicating whether to redirect stderr to the logging framework
   */
  public static void init( boolean redirectStdOut, boolean redirectStdErr ) {
    int maxSize = Const.toInt( EnvUtil.getSystemProperty( Const.HOP_MAX_LOG_SIZE_IN_LINES ), Const.MAX_NR_LOG_LINES );
    int maxLogTimeoutMinutes =
      Const.toInt( EnvUtil.getSystemProperty( Const.HOP_MAX_LOG_TIMEOUT_IN_MINUTES ), Const.MAX_LOG_LINE_TIMEOUT_MINUTES );
    init0( maxSize, maxLogTimeoutMinutes, redirectStdOut, redirectStdErr );
  }

  /**
   * Initialize the central log store. If it has already been initialized the configuration will be updated.
   *
   * @param maxSize              the maximum size of the log buffer
   * @param maxLogTimeoutMinutes The maximum time that a log line times out in minutes
   */
  private static synchronized void init0( int maxSize, int maxLogTimeoutMinutes, boolean redirectStdOut,
                                          boolean redirectStdErr ) {
    if ( store != null ) {
      // CentralLogStore already initialized. Just update the values.
      store.appender.setMaxNrLines( maxSize );
      store.replaceLogCleaner( maxLogTimeoutMinutes );
    } else {
      store = new HopLogStore( maxSize, maxLogTimeoutMinutes, redirectStdOut, redirectStdErr );
    }
    initialized.set( true );
  }

  public static HopLogStore getInstance() {
    if ( store == null ) {
      throw new RuntimeException( "Central Log Store is not initialized!!!" );
    }
    return store;
  }

  /**
   * @return the number (sequence, 1..N) of the last log line. If no records are present in the buffer, 0 is returned.
   */
  public static int getLastBufferLineNr() {
    return getInstance().appender.getLastBufferLineNr();
  }

  /**
   * Get all the log lines pertaining to the specified parent log channel id (including all children)
   *
   * @param parentLogChannelId the parent log channel ID to grab
   * @param includeGeneral     include general log lines
   * @param from
   * @param to
   * @return the log lines found
   */
  public static List<HopLoggingEvent> getLogBufferFromTo( String parentLogChannelId, boolean includeGeneral,
                                                          int from, int to ) {
    return getInstance().appender.getLogBufferFromTo( parentLogChannelId, includeGeneral, from, to );
  }

  /**
   * Get all the log lines for the specified parent log channel id (including all children)
   *
   * @param channelId      channel IDs to grab
   * @param includeGeneral include general log lines
   * @param from
   * @param to
   * @return
   */
  public static List<HopLoggingEvent> getLogBufferFromTo( List<String> channelId, boolean includeGeneral,
                                                          int from, int to ) {
    return getInstance().appender.getLogBufferFromTo( channelId, includeGeneral, from, to );
  }

  /**
   * @return The appender that represents the central logging store. It is capable of giving back log rows in an
   * incremental fashion, etc.
   */
  public static LoggingBuffer getAppender() {
    return getInstance().appender;
  }

  /**
   * Discard all the lines for the specified log channel id AND all the children.
   *
   * @param parentLogChannelId the parent log channel id to be removed along with all its children.
   */
  public static void discardLines( String parentLogChannelId, boolean includeGeneralMessages ) {
    LoggingRegistry registry = LoggingRegistry.getInstance();
    MetricsRegistry metricsRegistry = MetricsRegistry.getInstance();
    List<String> ids = registry.getLogChannelChildren( parentLogChannelId );

    // Remove all the rows for these ids
    //
    LoggingBuffer bufferAppender = getInstance().appender;
    // int beforeSize = bufferAppender.size();
    for ( String id : ids ) {
      // Remove it from the central log buffer
      //
      bufferAppender.removeChannelFromBuffer( id );

      // Also remove the item from the registry.
      //
      registry.getMap().remove( id );
      metricsRegistry.getSnapshotLists().remove( id );
      metricsRegistry.getSnapshotMaps().remove( id );
    }

    // Now discard the general lines if this is required
    //
    if ( includeGeneralMessages ) {
      bufferAppender.removeGeneralMessages();
    }
  }

  public static boolean isInitialized() {
    return initialized.get();
  }

  public void reset() {
    if ( initialized.compareAndSet( true, false ) ) {
      appender = null;
      if ( logCleanerTimer != null ) {
        logCleanerTimer.cancel();
        logCleanerTimer = null;
      }
      store = null;
    }
  }
}
