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
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.IRowListener;

import java.util.Date;

/**
 * An identifiable component of an execution engine {@link IPipelineEngine}
 * In a pipeline engine this would be a transform
 */
public interface IEngineComponent {

  /**
   * @return The component name
   */
  String getName();

  /**
   * @return The copy number (0 of higher for parallel runs)
   */
  int getCopyNr();

  LogLevel getLogLevel();

  void setLogLevel(LogLevel logLevel);

  ILogChannel getLogChannel();

  /**
   * @return The log channel ID or null if there is no separate log channel.
   */
  String getLogChannelId();

  /**
   * Retrieve the logging text of this component in the engine
   *
   * @return logging text
   */
  String getLogText();

  /**
   * @return true if this component is running/active
   */
  boolean isRunning();

  /**
   * @return true if the component is selected in the user interface
   */
  boolean isSelected();

  /**
   * @return True if the component is stopped
   */
  boolean isStopped();

  /**
   * @return True if the component is paused
   */
  boolean isPaused();

  /**
   * @return The number of errors in this component
   */
  long getErrors();

  long getLinesRead();
  long getLinesWritten();
  long getLinesInput();
  long getLinesOutput();
  long getLinesRejected();
  long getLinesUpdated();

  String getStatusDescription();

  long getExecutionDuration();

  long getInputBufferSize();

  long getOutputBufferSize();

  /**
   * Add a rowlistener to the transform allowing you to inspect (or manipulate, be careful) the rows coming in or exiting the
   * transform.
   *
   * @param rowListener the rowlistener to add
   */
  void addRowListener( IRowListener rowListener );

  void removeRowListener( IRowListener rowListener );

  /**
   * Get the execution status of the component
   * @return
   */
  EngineComponent.ComponentExecutionStatus getStatus();


  /**
   * Gets initStartDate
   *
   * @return value of initStartDate
   */
  Date getInitStartDate();

  /**
   * @param initStartDate The initStartDate to set
   */
  void setInitStartDate( Date initStartDate );

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  Date getExecutionStartDate();

  /**
   * @param executionStartDate The executionStartDate to set
   */
  void setExecutionStartDate( Date executionStartDate );

  /**
   * Gets firstRowReadDate
   *
   * @return value of firstRowReadDate
   */
  Date getFirstRowReadDate();

  /**
   * @param firstRowReadDate The firstRowReadDate to set
   */
  void setFirstRowReadDate( Date firstRowReadDate );

  /**
   * Gets lastRowWrittenDate
   *
   * @return value of lastRowWrittenDate
   */
  Date getLastRowWrittenDate();

  /**
   * @param lastRowWrittenDate The lastRowWrittenDate to set
   */
  void setLastRowWrittenDate( Date lastRowWrittenDate );

  /**
   * Gets executionEndDate
   *
   * @return value of executionEndDate
   */
  Date getExecutionEndDate();

  /**
   * @param executionEndDate The executionEndDate to set
   */
  void setExecutionEndDate( Date executionEndDate );

}
