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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.Metrics;
import org.apache.hop.i18n.BaseMessages;

import java.util.Date;
import java.util.List;

public class RunThread implements Runnable {

  private static final Class<?> PKG = BaseTransform.class; // For Translator

  private ITransform transform;
  private ILogChannel log;

  public RunThread( TransformMetaDataCombi combi ) {
    this.transform = combi.transform;

    // Sanity check just in case the provided meta or data wasn't used during the creation of the transform
    //
    this.transform.setMeta( combi.meta );
    this.transform.setData( combi.data );

    this.log = transform.getLogChannel();
  }

  public void run() {
    try {
      transform.setRunning( true );
      transform.setExecutionStartDate( new Date() );
      transform.getLogChannel().snap( Metrics.METRIC_TRANSFORM_EXECUTION_START );

      if ( log.isDetailed() ) {
        log.logDetailed( BaseMessages.getString( "System.Log.StartingToRun" ) );
      }

      // Wait
      while ( transform.processRow() ) {
        if ( transform.isStopped() ) {
          break;
        }
      }
    } catch ( Throwable t ) {
      try {
        // check for OOME
        if ( t instanceof OutOfMemoryError ) {
          // Handle this different with as less overhead as possible to get an error message in the log.
          // Otherwise it crashes likely with another OOME in Me$$ages.getString() and does not log
          // nor call the setErrors() and stopAll() below.
          log.logError( "UnexpectedError: ", t );
        } else {
          t.printStackTrace();
          log.logError( BaseMessages.getString( "System.Log.UnexpectedError" ), t );
        }

        String logChannelId = log.getLogChannelId();
        ILoggingObject loggingObject = LoggingRegistry.getInstance().getLoggingObject( logChannelId );
        String parentLogChannelId = loggingObject.getParent().getLogChannelId();
        List<String> logChannelChildren = LoggingRegistry.getInstance().getLogChannelChildren( parentLogChannelId );
        int childIndex = Const.indexOfString( log.getLogChannelId(), logChannelChildren );
        if ( log.isDebug() ) {
          log.logDebug( "child index = " + childIndex + ", logging object : " + loggingObject.toString() + " parent=" + parentLogChannelId );
        }
        HopLogStore.getAppender().getBuffer( "2bcc6b3f-c660-4a8b-8b17-89e8cbd5b29b", false );
        // baseTransform.logError(Const.getStackTracker(t));
      } catch ( OutOfMemoryError e ) {
        e.printStackTrace();
      } finally {
        transform.setErrors( 1 );
        transform.stopAll();
      }
    } finally {
      transform.dispose();
      transform.setExecutionEndDate( new Date() );
      // If the transform was stopped it never flagged the last row
      if (transform.getLastRowWrittenDate()==null) {
        transform.setLastRowWrittenDate( transform.getExecutionEndDate() );
      }
      transform.getLogChannel().snap( Metrics.METRIC_TRANSFORM_EXECUTION_STOP );
      try {
        long li = transform.getLinesInput();
        long lo = transform.getLinesOutput();
        long lr = transform.getLinesRead();
        long lw = transform.getLinesWritten();
        long lu = transform.getLinesUpdated();
        long lj = transform.getLinesRejected();
        long e = transform.getErrors();
        if ( li > 0 || lo > 0 || lr > 0 || lw > 0 || lu > 0 || lj > 0 || e > 0 ) {
          log.logBasic( BaseMessages.getString( PKG, "BaseTransform.Log.SummaryInfo", String.valueOf( li ),
            String.valueOf( lo ), String.valueOf( lr ), String.valueOf( lw ),
            String.valueOf( lu ), String.valueOf( e + lj ) ) );
        } else {
          log.logDetailed( BaseMessages.getString( PKG, "BaseTransform.Log.SummaryInfo", String.valueOf( li ),
            String.valueOf( lo ), String.valueOf( lr ), String.valueOf( lw ),
            String.valueOf( lu ), String.valueOf( e + lj ) ) );
        }
      } catch ( Throwable t ) {
        //
        // it's likely an OOME, so we don't want to introduce overhead by using BaseMessages.getString(), see above
        //
        log.logError( "UnexpectedError: " + Const.getStackTracker( t ) );
      } finally {
        transform.markStop();
      }
    }
  }
}
