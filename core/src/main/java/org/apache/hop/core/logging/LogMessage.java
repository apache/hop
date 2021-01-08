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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.StringUtil;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class LogMessage implements ILogMessage {
  private String logChannelId;
  private String message;
  private String subject;
  private Object[] arguments;
  private LogLevel level;
  private String copy;
  private boolean simplified;

  /**
   * Backward compatibility : no registry used, just log the subject as part of the message
   *
   */
  public LogMessage( String subject, LogLevel level ) {
    this.subject = subject;
    this.level = level;
    this.message = null;
    this.logChannelId = null;
  }

  public LogMessage( String message, String logChannelId, LogLevel level ) {
    this(message, logChannelId, null, level, false);
  }

  public LogMessage( String message, String logChannelId, LogLevel level, boolean simplified ) {
    this(message, logChannelId, null, level, simplified);
  }

  public LogMessage( String message, String logChannelId, Object[] arguments, LogLevel level ) {
    this(message, logChannelId, arguments, level, false);
  }

  public LogMessage( String message, String logChannelId, Object[] arguments, LogLevel level, boolean simplified ) {
    this.message = message;
    this.logChannelId = logChannelId;
    this.arguments = arguments;
    this.level = level;
    this.simplified = simplified;
    lookupSubject();
  }

  private void lookupSubject() {
    // Derive the subject from the registry
    //
    ILoggingObject loggingObject = LoggingRegistry.getInstance().getLoggingObject( logChannelId );
    boolean detailedLogTurnOn = "Y".equals( EnvUtil.getSystemProperty( Const.HOP_LOG_MARK_MAPPINGS ) ) ? true : false;
    if ( loggingObject != null ) {
      if ( !detailedLogTurnOn ) {
        subject = loggingObject.getObjectName();
      } else {
        subject = getDetailedSubject( loggingObject );
      }
      copy = loggingObject.getObjectCopy();
    }
  }

  /**
   * @param loggingObject
   * @return
   */
  private String getDetailedSubject( ILoggingObject loggingObject ) {

    List<String> subjects = getSubjectTree( loggingObject );
    return subjects.size() > 1 ? formatDetailedSubject( subjects ) : subjects.get( 0 );
  }

  /**
   * @param loggingObject
   */
  private List<String> getSubjectTree( ILoggingObject loggingObject ) {
    List<String> subjects = new ArrayList<>();
    while ( loggingObject != null ) {
      subjects.add( loggingObject.getObjectName() );
      loggingObject = loggingObject.getParent();
    }
    return subjects;
  }

  private String formatDetailedSubject( List<String> subjects ) {

    StringBuilder string = new StringBuilder();

    int currentTransform = 0;
    int rootTransform = subjects.size() - 1;

    for ( int i = rootTransform - 1; i > currentTransform; i-- ) {
      string.append( "[" ).append( subjects.get( i ) ).append( "]" ).append( "." );
    }
    string.append( subjects.get( currentTransform ) );
    return string.toString();
  }

  @Override
  @Deprecated
  public String toString() {
    if (simplified) {
      return getMessage();
    } else {
      if ( StringUtils.isBlank( message ) ) {
        return subject;
      } else if ( StringUtils.isBlank( subject ) ) {
        return getMessage();
      }
      return String.format( "%s - %s", subject, getMessage() );
    }
  }

  @Override
  public LogLevel getLevel() {
    return level;
  }

  /**
   * @return The formatted message.
   */
  @Override
  public String getMessage() {
    String formatted = message;
    if ( arguments != null ) {
      // get all "tokens" enclosed by curly brackets within the message
      final List<String> tokens = new ArrayList<>();
      StringUtil.getUsedVariables( formatted, "{", "}", tokens, true );
      // perform MessageFormat.format( ... ) on each token, if we get an exception, we'll know that we have a
      // segment that isn't parsable by MessageFormat, likely a pdi variable name (${foo}) - in this case, we need to
      // escape the curly brackets in the message, so that MessageFormat does not complain
      for ( final String token : tokens ) {
        try {
          MessageFormat.format( "{" + token + "}", arguments );
        } catch ( final IllegalArgumentException iar ) {
          formatted = formatted.replaceAll( "\\{" + token + "\\}", "\\'{'" + token + "\\'}'" );
        }
      }
      // now that we have escaped curly brackets in all invalid tokens, we can attempt to format the entire message
      formatted = MessageFormat.format( formatted, arguments );
    }
    return formatted;
  }

  /**
   * @return the subject
   */
  @Override
  public String getSubject() {
    return subject;
  }

  /**
   * @return the logChannelId
   */
  @Override
  public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * @return the arguments
   */
  @Override
  public Object[] getArguments() {
    return arguments;
  }

  public boolean isError() {
    return level.isError();
  }

  @Override
  public String getCopy() {
    return copy;
  }

  /**
   * Gets simplified
   *
   * @return value of simplified
   */
  @Override public boolean isSimplified() {
    return simplified;
  }

  /**
   * @param simplified The simplified to set
   */
  public void setSimplified( boolean simplified ) {
    this.simplified = simplified;
  }
}
