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

package org.apache.hop.workflow.actions.writetolog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.w3c.dom.Node;

import java.util.Date;

/**
 * Action type to output message to the workflow log.
 *
 * @author Samatar
 * @since 08-08-2007
 */

@Action(
  id = "WRITE_TO_LOG",
  name = "i18n::ActionWriteToLog.Name",
  description = "i18n::ActionWriteToLog.Description",
  image = "WriteToLog.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/writetolog.html"
)
public class ActionWriteToLog extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionWriteToLog.class; // For Translator

  /**
   * The log level with which the message should be logged.
   *
   * @deprecated Use {@link ActionWriteToLog#getEntryLogLevel()} and
   * {@link ActionWriteToLog#setEntryLogLevel(LogLevel)} instead.
   */
  @Deprecated
  public LogLevel entryLogLevel;
  private String logsubject;
  private String logmessage;

  public ActionWriteToLog( String n ) {
    super( n, "" );
    logmessage = null;
    logsubject = null;
  }

  public ActionWriteToLog() {
    this( "" );
  }

  @Override
  public Object clone() {
    ActionWriteToLog je = (ActionWriteToLog) super.clone();
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "logmessage", logmessage ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "loglevel", ( getEntryLogLevel() == null ) ? null : getEntryLogLevel().getCode() ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "logsubject", logsubject ) );

    return retval.toString();
  }

  @Override
  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      logmessage = XmlHandler.getTagValue( entrynode, "logmessage" );
      entryLogLevel = LogLevel.getLogLevelForCode( XmlHandler.getTagValue( entrynode, "loglevel" ) );
      logsubject = XmlHandler.getTagValue( entrynode, "logsubject" );
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "WriteToLog.Error.UnableToLoadFromXML.Label" ), e );

    }
  }

  private class LogWriterObject implements ILoggingObject {

    private ILogChannel writerLog;

    private LogLevel logLevel;
    private ILoggingObject parent;
    private String subject;
    private String containerObjectId;

    public LogWriterObject( String subject, ILoggingObject parent, LogLevel logLevel ) {
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
      return LoggingObjectType.TRANSFORM;
    }

    @Override
    public ILoggingObject getParent() {
      return parent;
    }

    public ILogChannel getLogChannel() {
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
    public String getContainerId() {
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

  ILogChannel createLogChannel() {
    LogWriterObject logWriterObject = new LogWriterObject( getRealLogSubject(), this, parentWorkflow.getLogLevel() );
    return logWriterObject.getLogChannel();
  }

  /**
   * Output message to workflow log.
   */
  public boolean evaluate( Result result ) {
    ILogChannel logChannel = createLogChannel();
    String message = getRealLogMessage();

    // Filter out empty messages and those that are not visible with the workflow's log level
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
   * Execute this action and return the result. In this case it means, just set the result boolean in the Result
   * class.
   *
   * @param prevResult The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute( Result prevResult, int nr ) {
    prevResult.setResult( evaluate( prevResult ) );
    return prevResult;
  }

  @Override
  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous action.
    return false;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  public String getRealLogMessage() {
    return Const.NVL( resolve( getLogMessage() ), "" );

  }

  public String getRealLogSubject() {
    return Const.NVL( resolve( getLogSubject() ), "" );
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
