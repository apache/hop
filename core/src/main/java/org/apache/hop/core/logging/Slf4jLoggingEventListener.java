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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hop.core.logging.LoggingObjectType.DATABASE;
import static org.apache.hop.core.logging.LoggingObjectType.WORKFLOW;
import static org.apache.hop.core.logging.LoggingObjectType.ACTION;
import static org.apache.hop.core.logging.LoggingObjectType.TRANSFORM;
import static org.apache.hop.core.logging.LoggingObjectType.PIPELINE;

public class Slf4jLoggingEventListener implements IHopLoggingEventListener {

  @VisibleForTesting Logger pipelineLogger = LoggerFactory.getLogger( "org.apache.hop.pipeline.Pipeline" );

  @VisibleForTesting Logger jobLogger = LoggerFactory.getLogger( "org.apache.hop.workflow.Workflow" );

  @VisibleForTesting Logger diLogger = LoggerFactory.getLogger( "org.apache.hop" );

  @VisibleForTesting Function<String, ILoggingObject> logObjProvider =
      objId -> LoggingRegistry.getInstance().getLoggingObject( objId );

  private static final String SEPARATOR = "/";

  public Slf4jLoggingEventListener() {
  }


  @Override
  public void eventAdded( HopLoggingEvent event ) {
    Object messageObject = event.getMessage();
    checkNotNull( messageObject, "Expected log message to be defined." );
    if ( messageObject instanceof LogMessage ) {
      LogMessage message = (LogMessage) messageObject;
      ILoggingObject loggingObject = logObjProvider.apply( message.getLogChannelId() );

      if ( loggingObject == null ) {
        // this can happen if logObject has been discarded while log events are still in flight.
        logToLogger( diLogger, message.getLevel(),
          message.getSubject() + " " + message.getMessage() );
      } else if ( loggingObject.getObjectType() == PIPELINE || loggingObject.getObjectType() == TRANSFORM || loggingObject.getObjectType() == DATABASE ) {
        logToLogger( pipelineLogger, message.getLevel(), loggingObject, message );
      } else if ( loggingObject.getObjectType() == WORKFLOW || loggingObject.getObjectType() == ACTION ) {
        logToLogger( jobLogger, message.getLevel(), loggingObject, message );
      }
    }
  }

  private void logToLogger( Logger logger, LogLevel logLevel, ILoggingObject loggingObject,
                            LogMessage message ) {
    logToLogger( logger, logLevel,
      "[" + getDetailedSubject( loggingObject ) + "]  " + message.getMessage() );
  }

  private void logToLogger( Logger logger, LogLevel logLevel, String message ) {
    switch ( logLevel ) {
      case NOTHING:
        break;
      case ERROR:
        logger.error( message );
        break;
      case MINIMAL:
        logger.warn( message );
        break;
      case BASIC:
      case DETAILED:
        logger.info( message );
        break;
      case DEBUG:
        logger.debug( message );
        break;
      case ROWLEVEL:
        logger.trace( message );
        break;
      default:
        break;
    }
  }

  private String getDetailedSubject( ILoggingObject loggingObject ) {
    LinkedList<String> subjects = new LinkedList<>();
    while ( loggingObject != null ) {
      if ( loggingObject.getObjectType() == PIPELINE || loggingObject.getObjectType() == WORKFLOW ) {
        String filename = loggingObject.getFilename();
        if ( filename != null && filename.length() > 0 ) {
          subjects.add( filename );
        }
      }
      loggingObject = loggingObject.getParent();
    }
    if ( subjects.size() > 0 ) {
      return subjects.size() > 1 ? formatDetailedSubject( subjects ) : subjects.get( 0 );
    } else {
      return "";
    }
  }

  private String formatDetailedSubject( LinkedList<String> subjects ) {
    StringBuilder string = new StringBuilder();
    for ( Iterator<String> it = subjects.descendingIterator(); it.hasNext(); ) {
      string.append( it.next() );
      if ( it.hasNext() ) {
        string.append( "  " );
      }
    }
    return string.toString();
  }
}
