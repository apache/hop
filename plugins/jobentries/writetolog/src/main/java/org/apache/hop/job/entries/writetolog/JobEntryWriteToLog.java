/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.job.entries.writetolog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Date;

/**
 * Job entry type to output message to the job log.
 *
 * @author Samatar
 * @since 08-08-2007
 */

@JobEntry(
  id = "WRITE_TO_LOG",
  i18nPackageName = "org.apache.hop.job.entries.writetolog",
  name = "JobEntryWriteToLog.Name",
  description = "JobEntryWriteToLog.Description",
  image = "WriteToLog.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Utility"
)
public class JobEntryWriteToLog extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryWriteToLog.class; // for i18n purposes, needed by Translator2!!

  /**
   * The log level with which the message should be logged.
   *
   * @deprecated Use {@link JobEntryWriteToLog#getEntryLogLevel()} and
   * {@link JobEntryWriteToLog#setEntryLogLevel(LogLevel)} instead.
   */
  @Deprecated
  public LogLevel entryLogLevel;
  private String logsubject;
  private String logmessage;

  public JobEntryWriteToLog( String n ) {
    super( n, "" );
    logmessage = null;
    logsubject = null;
  }

  public JobEntryWriteToLog() {
    this( "" );
  }

  @Override
  public Object clone() {
    JobEntryWriteToLog je = (JobEntryWriteToLog) super.clone();
    return je;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logmessage", logmessage ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "loglevel", ( getEntryLogLevel() == null ) ? null : getEntryLogLevel().getCode() ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logsubject", logsubject ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      logmessage = XMLHandler.getTagValue( entrynode, "logmessage" );
      entryLogLevel = LogLevel.getLogLevelForCode( XMLHandler.getTagValue( entrynode, "loglevel" ) );
      logsubject = XMLHandler.getTagValue( entrynode, "logsubject" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "WriteToLog.Error.UnableToLoadFromXML.Label" ), e );

    }
  }

  private class LogWriterObject implements LoggingObjectInterface {

    private LogChannelInterface writerLog;

    private LogLevel logLevel;
    private LoggingObjectInterface parent;
    private String subject;
    private String containerObjectId;

    public LogWriterObject( String subject, LoggingObjectInterface parent, LogLevel logLevel ) {
      this.subject = subject;
      this.parent = parent;
      this.logLevel = logLevel;
      this.writerLog = new LogChannel( this, parent );
      this.containerObjectId = writerLog.getContainerObjectId();
    }

    @Override
    public String getFilename() {
      return null;
    }

    @Override
    public String getLogChannelId() {
      return writerLog.getLogChannelId();
    }

    @Override
    public String getObjectCopy() {
      return null;
    }

    @Override
    public String getObjectName() {
      return subject;
    }

    @Override
    public LoggingObjectType getObjectType() {
      return LoggingObjectType.STEP;
    }

    @Override
    public LoggingObjectInterface getParent() {
      return parent;
    }

    public LogChannelInterface getLogChannel() {
      return writerLog;
    }

    @Override
    public LogLevel getLogLevel() {
      return logLevel;
    }

    /**
     * @return the execution container object id
     */
    @Override
    public String getContainerObjectId() {
      return containerObjectId;
    }

    /**
     * Stub
     */
    @Override
    public Date getRegistrationDate() {
      return null;
    }

    @Override
    public boolean isGatheringMetrics() {
      return log.isGatheringMetrics();
    }

    @Override
    public void setGatheringMetrics( boolean gatheringMetrics ) {
      log.setGatheringMetrics( gatheringMetrics );
    }

    @Override
    public boolean isForcingSeparateLogging() {
      return log.isForcingSeparateLogging();
    }

    @Override
    public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
      log.setForcingSeparateLogging( forcingSeparateLogging );
    }
  }

  LogChannelInterface createLogChannel() {
    LogWriterObject logWriterObject = new LogWriterObject( getRealLogSubject(), this, parentJob.getLogLevel() );
    return logWriterObject.getLogChannel();
  }

  /**
   * Output message to job log.
   */
  public boolean evaluate( Result result ) {
    LogChannelInterface logChannel = createLogChannel();
    String message = getRealLogMessage();

    // Filter out empty messages and those that are not visible with the job's log level
    if ( Utils.isEmpty( message ) || !getEntryLogLevel().isVisible( logChannel.getLogLevel() ) ) {
      return true;
    }

    try {
      switch ( getEntryLogLevel() ) {
        case ERROR:
          logChannel.logError( message + Const.CR );
          break;
        case MINIMAL:
          logChannel.logMinimal( message + Const.CR );
          break;
        case BASIC:
          logChannel.logBasic( message + Const.CR );
          break;
        case DETAILED:
          logChannel.logDetailed( message + Const.CR );
          break;
        case DEBUG:
          logChannel.logDebug( message + Const.CR );
          break;
        case ROWLEVEL:
          logChannel.logRowlevel( message + Const.CR );
          break;
        default: // NOTHING
          break;
      }

      return true;
    } catch ( Exception e ) {
      result.setNrErrors( 1 );
      log.logError( BaseMessages.getString( PKG, "WriteToLog.Error.Label" ), BaseMessages.getString(
        PKG, "WriteToLog.Error.Description" )
        + " : " + e.toString() );
      return false;
    }

  }

  /**
   * Execute this job entry and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param prev_result The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute( Result prev_result, int nr ) {
    prev_result.setResult( evaluate( prev_result ) );
    return prev_result;
  }

  @Override
  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous jobentry.
    return false;
  }

  @Override
  public boolean evaluates() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  public String getRealLogMessage() {
    return Const.NVL( environmentSubstitute( getLogMessage() ), "" );

  }

  public String getRealLogSubject() {
    return Const.NVL( environmentSubstitute( getLogSubject() ), "" );
  }

  public String getLogMessage() {
    if ( logmessage == null ) {
      logmessage = "";
    }
    return logmessage;

  }

  public String getLogSubject() {
    if ( logsubject == null ) {
      logsubject = "";
    }
    return logsubject;

  }

  public void setLogMessage( String s ) {
    logmessage = s;
  }

  public void setLogSubject( String logsubjectin ) {
    logsubject = logsubjectin;
  }

  public LogLevel getEntryLogLevel() {
    return entryLogLevel;
  }

  public void setEntryLogLevel( LogLevel in ) {
    this.entryLogLevel = in;
  }
}
