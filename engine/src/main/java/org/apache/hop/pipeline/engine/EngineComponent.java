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

package org.apache.hop.pipeline.engine;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;

import java.util.Date;
import java.util.Objects;

public class EngineComponent implements IEngineComponent {

  private static final Class<?> PKG = BaseTransform.class; // For Translator

  private String name;
  private int copyNr;
  private String logChannelId;
  private ILogChannel logChannel;
  private LogLevel logLevel;
  private String logText;
  private boolean running;
  private boolean selected;
  private long errors;
  private long linesRead;
  private long linesWritten;
  private long linesInput;
  private long linesOutput;
  private long linesRejected;
  private long linesUpdated;
  private long executionDuration;
  private long inputBufferSize;
  private long outputBufferSize;
  private boolean stopped;
  private boolean paused;
  private ComponentExecutionStatus status;
  private Date initStartDate;
  private Date executionStartDate;
  private Date firstRowReadDate;
  private Date lastRowWrittenDate;
  private Date executionEndDate;

  public EngineComponent() {
  }

  public EngineComponent( String name, int copyNr ) {
    this();
    this.name = name;
    this.copyNr = copyNr;
    this.logChannel = LogChannel.GENERAL;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    EngineComponent that = (EngineComponent) o;
    return copyNr == that.copyNr &&
      Objects.equals( name, that.name );
  }

  @Override public int hashCode() {
    return Objects.hash( name, copyNr );
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  @Override public int getCopyNr() {
    return copyNr;
  }

  /**
   * @param copyNr The copyNr to set
   */
  public void setCopyNr( int copyNr ) {
    this.copyNr = copyNr;
  }

  /**
   * Gets logChannel
   *
   * @return value of logChannel
   */
  @Override public ILogChannel getLogChannel() {
    return logChannel;
  }

  /**
   * @param logChannel The logChannel to set
   */
  public void setLogChannel( ILogChannel logChannel ) {
    this.logChannel = logChannel;
  }

  /**
   * Gets logChannelId
   *
   * @return value of logChannelId
   */
  @Override public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * @param logChannelId The logChannelId to set
   */
  public void setLogChannelId( String logChannelId ) {
    this.logChannelId = logChannelId;
  }

  /**
   * Gets logLevel
   *
   * @return value of logLevel
   */
  @Override public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * @param logLevel The logLevel to set
   */
  @Override public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  /**
   * Gets logText
   *
   * @return value of logText
   */
  @Override public String getLogText() {
    return logText;
  }

  /**
   * @param logText The logText to set
   */
  public void setLogText( String logText ) {
    this.logText = logText;
  }

  /**
   * Gets running
   *
   * @return value of running
   */
  @Override public boolean isRunning() {
    return running;
  }

  /**
   * @param running The running to set
   */
  public void setRunning( boolean running ) {
    this.running = running;
  }

  /**
   * Gets selected
   *
   * @return value of selected
   */
  @Override public boolean isSelected() {
    return selected;
  }

  /**
   * @param selected The selected to set
   */
  public void setSelected( boolean selected ) {
    this.selected = selected;
  }

  /**
   * Gets errors
   *
   * @return value of errors
   */
  @Override public long getErrors() {
    return errors;
  }

  @Override public void addRowListener( IRowListener rowListener ) {
    throw new RuntimeException( "Adding a row listener to this transform is not possible as it's not part of a running engine" );
  }

  @Override public void removeRowListener( IRowListener rowListener ) {
    throw new RuntimeException( "Removing a row listener to this transform is not possible as it's not part of a running engine" );
  }

  /**
   * @param errors The errors to set
   */
  public void setErrors( long errors ) {
    this.errors = errors;
  }

  /**
   * Gets linesRead
   *
   * @return value of linesRead
   */
  @Override public long getLinesRead() {
    return linesRead;
  }

  /**
   * @param linesRead The linesRead to set
   */
  public void setLinesRead( long linesRead ) {
    this.linesRead = linesRead;
  }

  /**
   * Gets linesWritten
   *
   * @return value of linesWritten
   */
  @Override public long getLinesWritten() {
    return linesWritten;
  }

  /**
   * @param linesWritten The linesWritten to set
   */
  public void setLinesWritten( long linesWritten ) {
    this.linesWritten = linesWritten;
  }

  /**
   * Gets linesInput
   *
   * @return value of linesInput
   */
  @Override public long getLinesInput() {
    return linesInput;
  }

  /**
   * @param linesInput The linesInput to set
   */
  public void setLinesInput( long linesInput ) {
    this.linesInput = linesInput;
  }

  /**
   * Gets linesOutput
   *
   * @return value of linesOutput
   */
  @Override public long getLinesOutput() {
    return linesOutput;
  }

  /**
   * @param linesOutput The linesOutput to set
   */
  public void setLinesOutput( long linesOutput ) {
    this.linesOutput = linesOutput;
  }

  /**
   * Gets linesRejected
   *
   * @return value of linesRejected
   */
  @Override public long getLinesRejected() {
    return linesRejected;
  }

  /**
   * @param linesRejected The linesRejected to set
   */
  public void setLinesRejected( long linesRejected ) {
    this.linesRejected = linesRejected;
  }

  /**
   * Gets linesUpdated
   *
   * @return value of linesUpdated
   */
  @Override public long getLinesUpdated() {
    return linesUpdated;
  }

  /**
   * @param linesUpdated The linesUpdated to set
   */
  public void setLinesUpdated( long linesUpdated ) {
    this.linesUpdated = linesUpdated;
  }

  /**
   * Gets statusDescription
   *
   * @return value of statusDescription
   */
  @Override public String getStatusDescription() {
    return status == null ? null : status.getDescription();
  }


  /**
   * Gets executionDuration
   *
   * @return value of executionDuration
   */
  @Override public long getExecutionDuration() {
    return executionDuration;
  }

  /**
   * @param executionDuration The executionDuration to set
   */
  public void setExecutionDuration( long executionDuration ) {
    this.executionDuration = executionDuration;
  }

  /**
   * Gets inputBufferSize
   *
   * @return value of inputBufferSize
   */
  @Override public long getInputBufferSize() {
    return inputBufferSize;
  }

  /**
   * @param inputBufferSize The inputBufferSize to set
   */
  public void setInputBufferSize( long inputBufferSize ) {
    this.inputBufferSize = inputBufferSize;
  }

  /**
   * Gets outputBufferSize
   *
   * @return value of outputBufferSize
   */
  @Override public long getOutputBufferSize() {
    return outputBufferSize;
  }

  /**
   * @param outputBufferSize The outputBufferSize to set
   */
  public void setOutputBufferSize( long outputBufferSize ) {
    this.outputBufferSize = outputBufferSize;
  }

  /**
   * Gets stopped
   *
   * @return value of stopped
   */
  @Override public boolean isStopped() {
    return stopped;
  }

  /**
   * @param stopped The stopped to set
   */
  public void setStopped( boolean stopped ) {
    this.stopped = stopped;
  }

  /**
   * Gets paused
   *
   * @return value of paused
   */
  @Override public boolean isPaused() {
    return paused;
  }

  /**
   * @param paused The paused to set
   */
  public void setPaused( boolean paused ) {
    this.paused = paused;
  }

  /**
   * Gets status
   *
   * @return value of status
   */
  @Override public ComponentExecutionStatus getStatus() {
    return status;
  }

  /**
   * @param status The status to set
   */
  public void setStatus( ComponentExecutionStatus status ) {
    this.status = status;
  }

  /**
   * Gets initStartDate
   *
   * @return value of initStartDate
   */
  @Override public Date getInitStartDate() {
    return initStartDate;
  }

  /**
   * @param initStartDate The initStartDate to set
   */
  @Override public void setInitStartDate( Date initStartDate ) {
    this.initStartDate = initStartDate;
  }

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  @Override public Date getExecutionStartDate() {
    return executionStartDate;
  }

  /**
   * @param executionStartDate The executionStartDate to set
   */
  @Override public void setExecutionStartDate( Date executionStartDate ) {
    this.executionStartDate = executionStartDate;
  }

  /**
   * Gets firstRowReadDate
   *
   * @return value of firstRowReadDate
   */
  @Override public Date getFirstRowReadDate() {
    return firstRowReadDate;
  }

  /**
   * @param firstRowReadDate The firstRowReadDate to set
   */
  @Override public void setFirstRowReadDate( Date firstRowReadDate ) {
    this.firstRowReadDate = firstRowReadDate;
  }

  /**
   * Gets lastRowWrittenDate
   *
   * @return value of lastRowWrittenDate
   */
  @Override public Date getLastRowWrittenDate() {
    return lastRowWrittenDate;
  }

  /**
   * @param lastRowWrittenDate The lastRowWrittenDate to set
   */
  @Override public void setLastRowWrittenDate( Date lastRowWrittenDate ) {
    this.lastRowWrittenDate = lastRowWrittenDate;
  }

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  @Override public Date getExecutionEndDate() {
    return executionEndDate;
  }

  /**
   * @param executionEndDate The executionEndDate to set
   */
  @Override public void setExecutionEndDate( Date executionEndDate ) {
    this.executionEndDate = executionEndDate;
  }

  public enum ComponentExecutionStatus {


    /**
     * The status empty.
     */
    STATUS_EMPTY( BaseMessages.getString( PKG, "BaseTransform.status.Empty" ) ),

    /**
     * The status init.
     */
    STATUS_INIT( BaseMessages.getString( PKG, "BaseTransform.status.Init" ) ),

    /**
     * The status running.
     */
    STATUS_RUNNING( BaseMessages.getString( PKG, "BaseTransform.status.Running" ) ),

    /**
     * The status idle.
     */
    STATUS_IDLE( BaseMessages.getString( PKG, "BaseTransform.status.Idle" ) ),

    /**
     * The status finished.
     */
    STATUS_FINISHED( BaseMessages.getString( PKG, "BaseTransform.status.Finished" ) ),

    /**
     * The status stopped.
     */
    STATUS_STOPPED( BaseMessages.getString( PKG, "BaseTransform.status.Stopped" ) ),

    /**
     * The status disposed.
     */
    STATUS_DISPOSED( BaseMessages.getString( PKG, "BaseTransform.status.Disposed" ) ),

    /**
     * The status halted.
     */
    STATUS_HALTED( BaseMessages.getString( PKG, "BaseTransform.status.Halted" ) ),

    /**
     * The status paused.
     */
    STATUS_PAUSED( BaseMessages.getString( PKG, "BaseTransform.status.Paused" ) ),

    /**
     * The status halting.
     */
    STATUS_HALTING( BaseMessages.getString( PKG, "BaseTransform.status.Halting" ) );


    /**
     * The description.
     */
    private String description;

    /**
     * Instantiates a new transform execution status.
     *
     * @param description the description
     */
    private ComponentExecutionStatus( String description ) {
      this.description = description;
    }

    /**
     * Gets the description.
     *
     * @return the description
     */
    public String getDescription() {
      return description;
    }



    /*
     * (non-Javadoc)
     *
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
      return description;
    }

    public static ComponentExecutionStatus getStatusFromDescription( String description ) {
      for ( ComponentExecutionStatus status : values() ) {
        if ( status.description.equalsIgnoreCase( description ) ) {
          return status;
        }
      }
      return STATUS_EMPTY;
    }
  }
}
