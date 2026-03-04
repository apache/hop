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

import java.util.Date;
import java.util.Objects;
import lombok.Setter;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;

public class EngineComponent implements IEngineComponent {

  private static final Class<?> PKG = BaseTransform.class;

  @Setter private String name;
  @Setter private int copyNr;
  @Setter private String logChannelId;
  @Setter private ILogChannel logChannel;
  private LogLevel logLevel;
  @Setter private String logText;
  @Setter private boolean running;
  @Setter private boolean selected;
  @Setter private long errors;
  @Setter private long linesRead;
  @Setter private long linesWritten;
  @Setter private long linesInput;
  @Setter private long linesOutput;
  @Setter private long linesRejected;
  @Setter private long linesUpdated;
  @Setter private long executionDuration;
  @Setter private long inputBufferSize;
  @Setter private long outputBufferSize;
  @Setter private boolean stopped;
  @Setter private boolean paused;
  @Setter private ComponentExecutionStatus status;
  private Date initStartDate;
  private Date executionStartDate;
  private Date firstRowReadDate;
  private Date lastRowWrittenDate;
  private Date executionEndDate;

  public EngineComponent() {}

  public EngineComponent(String name, int copyNr) {
    this();
    this.name = name;
    this.copyNr = copyNr;
    this.logChannel = LogChannel.GENERAL;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EngineComponent that = (EngineComponent) o;
    return copyNr == that.copyNr && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, copyNr);
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  @Override
  public int getCopyNr() {
    return copyNr;
  }

  /**
   * Gets logChannel
   *
   * @return value of logChannel
   */
  @Override
  public ILogChannel getLogChannel() {
    return logChannel;
  }

  /**
   * Gets logChannelId
   *
   * @return value of logChannelId
   */
  @Override
  public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * Gets logLevel
   *
   * @return value of logLevel
   */
  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * @param logLevel The logLevel to set
   */
  @Override
  public void setLogLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
  }

  /**
   * Gets logText
   *
   * @return value of logText
   */
  @Override
  public String getLogText() {
    return logText;
  }

  /**
   * Gets running
   *
   * @return value of running
   */
  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * Gets selected
   *
   * @return value of selected
   */
  @Override
  public boolean isSelected() {
    return selected;
  }

  /**
   * Gets errors
   *
   * @return value of errors
   */
  @Override
  public long getErrors() {
    return errors;
  }

  @Override
  public void addRowListener(IRowListener rowListener) {
    throw new RuntimeException(
        "Adding a row listener to this transform is not possible as it's not part of a running engine");
  }

  @Override
  public void removeRowListener(IRowListener rowListener) {
    throw new RuntimeException(
        "Removing a row listener to this transform is not possible as it's not part of a running engine");
  }

  /**
   * Gets linesRead
   *
   * @return value of linesRead
   */
  @Override
  public long getLinesRead() {
    return linesRead;
  }

  /**
   * Gets linesWritten
   *
   * @return value of linesWritten
   */
  @Override
  public long getLinesWritten() {
    return linesWritten;
  }

  /**
   * Gets linesInput
   *
   * @return value of linesInput
   */
  @Override
  public long getLinesInput() {
    return linesInput;
  }

  /**
   * Gets linesOutput
   *
   * @return value of linesOutput
   */
  @Override
  public long getLinesOutput() {
    return linesOutput;
  }

  /**
   * Gets linesRejected
   *
   * @return value of linesRejected
   */
  @Override
  public long getLinesRejected() {
    return linesRejected;
  }

  /**
   * Gets data volume (row-based estimate). EngineComponent does not track this; the value is
   * provided by the actual transform (e.g. BaseTransform).
   *
   * @return null for EngineComponent; real value from ITransform.getDataVolume()
   */
  @Override
  public Long getDataVolume() {
    return null;
  }

  /**
   * Gets data volume in (bytes from InputStream). EngineComponent does not track this; only input
   * transforms that use an actual InputStream provide a value.
   *
   * @return null for EngineComponent
   */
  @Override
  public Long getDataVolumeIn() {
    return null;
  }

  /**
   * Gets data volume out (bytes from OutputStream). EngineComponent does not track this; only
   * output transforms that use an actual OutputStream provide a value.
   *
   * @return null for EngineComponent
   */
  @Override
  public Long getDataVolumeOut() {
    return null;
  }

  /**
   * Gets linesUpdated
   *
   * @return value of linesUpdated
   */
  @Override
  public long getLinesUpdated() {
    return linesUpdated;
  }

  /**
   * Gets statusDescription
   *
   * @return value of statusDescription
   */
  @Override
  public String getStatusDescription() {
    return status == null ? null : status.getDescription();
  }

  /**
   * Gets executionDuration
   *
   * @return value of executionDuration
   */
  @Override
  public long getExecutionDuration() {
    return executionDuration;
  }

  /**
   * Gets inputBufferSize
   *
   * @return value of inputBufferSize
   */
  @Override
  public long getInputBufferSize() {
    return inputBufferSize;
  }

  /**
   * Gets outputBufferSize
   *
   * @return value of outputBufferSize
   */
  @Override
  public long getOutputBufferSize() {
    return outputBufferSize;
  }

  /**
   * Gets stopped
   *
   * @return value of stopped
   */
  @Override
  public boolean isStopped() {
    return stopped;
  }

  /**
   * Gets paused
   *
   * @return value of paused
   */
  @Override
  public boolean isPaused() {
    return paused;
  }

  /**
   * Gets status
   *
   * @return value of status
   */
  @Override
  public ComponentExecutionStatus getStatus() {
    return status;
  }

  /**
   * Gets initStartDate
   *
   * @return value of initStartDate
   */
  @Override
  public Date getInitStartDate() {
    return initStartDate;
  }

  /**
   * @param initStartDate The initStartDate to set
   */
  @Override
  public void setInitStartDate(Date initStartDate) {
    this.initStartDate = initStartDate;
  }

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  @Override
  public Date getExecutionStartDate() {
    return executionStartDate;
  }

  /**
   * @param executionStartDate The executionStartDate to set
   */
  @Override
  public void setExecutionStartDate(Date executionStartDate) {
    this.executionStartDate = executionStartDate;
  }

  /**
   * Gets firstRowReadDate
   *
   * @return value of firstRowReadDate
   */
  @Override
  public Date getFirstRowReadDate() {
    return firstRowReadDate;
  }

  /**
   * @param firstRowReadDate The firstRowReadDate to set
   */
  @Override
  public void setFirstRowReadDate(Date firstRowReadDate) {
    this.firstRowReadDate = firstRowReadDate;
  }

  /**
   * Gets lastRowWrittenDate
   *
   * @return value of lastRowWrittenDate
   */
  @Override
  public Date getLastRowWrittenDate() {
    return lastRowWrittenDate;
  }

  /**
   * @param lastRowWrittenDate The lastRowWrittenDate to set
   */
  @Override
  public void setLastRowWrittenDate(Date lastRowWrittenDate) {
    this.lastRowWrittenDate = lastRowWrittenDate;
  }

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  @Override
  public Date getExecutionEndDate() {
    return executionEndDate;
  }

  /**
   * @param executionEndDate The executionEndDate to set
   */
  @Override
  public void setExecutionEndDate(Date executionEndDate) {
    this.executionEndDate = executionEndDate;
  }

  public enum ComponentExecutionStatus {

    /** The status empty. */
    STATUS_EMPTY(BaseMessages.getString(PKG, "BaseTransform.status.Empty")),

    /** The status init. */
    STATUS_INIT(BaseMessages.getString(PKG, "BaseTransform.status.Init")),

    /** The status running. */
    STATUS_RUNNING(BaseMessages.getString(PKG, "BaseTransform.status.Running")),

    /** The status idle. */
    STATUS_IDLE(BaseMessages.getString(PKG, "BaseTransform.status.Idle")),

    /** The status finished. */
    STATUS_FINISHED(BaseMessages.getString(PKG, "BaseTransform.status.Finished")),

    /** The status stopped. */
    STATUS_STOPPED(BaseMessages.getString(PKG, "BaseTransform.status.Stopped")),

    /** The status disposed. */
    STATUS_DISPOSED(BaseMessages.getString(PKG, "BaseTransform.status.Disposed")),

    /** The status halted. */
    STATUS_HALTED(BaseMessages.getString(PKG, "BaseTransform.status.Halted")),

    /** The status paused. */
    STATUS_PAUSED(BaseMessages.getString(PKG, "BaseTransform.status.Paused")),

    /** The status halting. */
    STATUS_HALTING(BaseMessages.getString(PKG, "BaseTransform.status.Halting"));

    /** The description. */
    private String description;

    /**
     * Instantiates a new transform execution status.
     *
     * @param description the description
     */
    private ComponentExecutionStatus(String description) {
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

    public static ComponentExecutionStatus getStatusFromDescription(String description) {
      for (ComponentExecutionStatus status : values()) {
        if (status.description.equalsIgnoreCase(description)) {
          return status;
        }
      }
      return STATUS_EMPTY;
    }
  }
}
