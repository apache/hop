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

package org.apache.hop.pipeline.performance;

import java.util.Date;

public class PerformanceSnapShot {

  private int seqNr;
  private Date date;
  private String parentName;
  private String componentName;
  private int copyNr;
  private long totalLinesRead;
  private long totalLinesWritten;
  private long totalLinesInput;
  private long totalLinesOutput;
  private long totalLinesUpdated;
  private long totalLinesRejected;
  private long totalErrors;

  private long timeDifference;
  private long linesRead;
  private long linesWritten;
  private long linesInput;
  private long linesOutput;
  private long linesUpdated;
  private long linesRejected;
  private long errors;

  private long inputBufferSize;
  private long outputBufferSize;

  /**
   * @param date
   * @param componentName
   * @param copyNr
   * @param totalLinesRead
   * @param totalLinesWritten
   * @param totalLinesInput
   * @param totalLinesOutput
   * @param totalLinesUpdated
   * @param totalLinesRejected
   * @param totalErrors
   */
  public PerformanceSnapShot( int seqNr, Date date, String parentName, String componentName,
                              int copyNr, long totalLinesRead, long totalLinesWritten, long totalLinesInput, long totalLinesOutput,
                              long totalLinesUpdated, long totalLinesRejected, long totalErrors ) {
    this.seqNr = seqNr;
    this.date = date;
    this.parentName = parentName;
    this.componentName = componentName;
    this.copyNr = copyNr;
    this.totalLinesRead = totalLinesRead;
    this.totalLinesWritten = totalLinesWritten;
    this.totalLinesInput = totalLinesInput;
    this.totalLinesOutput = totalLinesOutput;
    this.totalLinesUpdated = totalLinesUpdated;
    this.totalLinesRejected = totalLinesRejected;
    this.totalErrors = totalErrors;
  }

  public void diff( PerformanceSnapShot previous, long inputBufferSize, long outputBufferSize ) {
    this.inputBufferSize = inputBufferSize;
    this.outputBufferSize = outputBufferSize;

    if ( previous == null ) {
      timeDifference = 0;
      linesRead = totalLinesRead;
      linesWritten = totalLinesWritten;
      linesInput = totalLinesInput;
      linesOutput = totalLinesOutput;
      linesUpdated = totalLinesUpdated;
      linesRejected = totalLinesRejected;
      errors = totalErrors;
    } else {
      timeDifference = date.getTime() - previous.date.getTime();
      linesRead = totalLinesRead - previous.totalLinesRead;
      linesWritten = totalLinesWritten - previous.totalLinesWritten;
      linesInput = totalLinesInput - previous.totalLinesInput;
      linesOutput = totalLinesOutput - previous.totalLinesOutput;
      linesUpdated = totalLinesUpdated - previous.totalLinesUpdated;
      linesRejected = totalLinesRejected - previous.totalLinesRejected;
      errors = totalErrors - previous.totalErrors;
    }
  }

  /**
   * @return the date
   */
  public Date getDate() {
    return date;
  }

  /**
   * @param date the date to set
   */
  public void setDate( Date date ) {
    this.date = date;
  }

  /**
   * @return the transformName
   */
  public String getComponentName() {
    return componentName;
  }

  /**
   * @param componentName the transformName to set
   */
  public void setComponentName( String componentName ) {
    this.componentName = componentName;
  }

  /**
   * @return the transform copy nr
   */
  public int getCopyNr() {
    return copyNr;
  }

  /**
   * @param copyNr the transform copy nr to set
   */
  public void setCopyNr( int copyNr ) {
    this.copyNr = copyNr;
  }

  /**
   * @return the totalLinesRead
   */
  public long getTotalLinesRead() {
    return totalLinesRead;
  }

  /**
   * @param totalLinesRead the totalLinesRead to set
   */
  public void setTotalLinesRead( long totalLinesRead ) {
    this.totalLinesRead = totalLinesRead;
  }

  /**
   * @return the totalLinesWritten
   */
  public long getTotalLinesWritten() {
    return totalLinesWritten;
  }

  /**
   * @param totalLinesWritten the totalLinesWritten to set
   */
  public void setTotalLinesWritten( long totalLinesWritten ) {
    this.totalLinesWritten = totalLinesWritten;
  }

  /**
   * @return the totalLinesInput
   */
  public long getTotalLinesInput() {
    return totalLinesInput;
  }

  /**
   * @param totalLinesInput the totalLinesInput to set
   */
  public void setTotalLinesInput( long totalLinesInput ) {
    this.totalLinesInput = totalLinesInput;
  }

  /**
   * @return the totalLinesOutput
   */
  public long getTotalLinesOutput() {
    return totalLinesOutput;
  }

  /**
   * @param totalLinesOutput the totalLinesOutput to set
   */
  public void setTotalLinesOutput( long totalLinesOutput ) {
    this.totalLinesOutput = totalLinesOutput;
  }

  /**
   * @return the totalLinesUpdated
   */
  public long getTotalLinesUpdated() {
    return totalLinesUpdated;
  }

  /**
   * @param totalLinesUpdated the totalLinesUpdated to set
   */
  public void setTotalLinesUpdated( long totalLinesUpdated ) {
    this.totalLinesUpdated = totalLinesUpdated;
  }

  /**
   * @return the totalLinesRejected
   */
  public long getTotalLinesRejected() {
    return totalLinesRejected;
  }

  /**
   * @param totalLinesRejected the totalLinesRejected to set
   */
  public void setTotalLinesRejected( long totalLinesRejected ) {
    this.totalLinesRejected = totalLinesRejected;
  }

  /**
   * @return the totalErrors
   */
  public long getTotalErrors() {
    return totalErrors;
  }

  /**
   * @param totalErrors the totalErrors to set
   */
  public void setTotalErrors( long totalErrors ) {
    this.totalErrors = totalErrors;
  }

  /**
   * @return the timeDifference
   */
  public long getTimeDifference() {
    return timeDifference;
  }

  /**
   * @param timeDifference the timeDifference to set
   */
  public void setTimeDifference( long timeDifference ) {
    this.timeDifference = timeDifference;
  }

  /**
   * @return the linesRead
   */
  public long getLinesRead() {
    return linesRead;
  }

  /**
   * @param linesRead the linesRead to set
   */
  public void setLinesRead( long linesRead ) {
    this.linesRead = linesRead;
  }

  /**
   * @return the linesWritten
   */
  public long getLinesWritten() {
    return linesWritten;
  }

  /**
   * @param linesWritten the linesWritten to set
   */
  public void setLinesWritten( long linesWritten ) {
    this.linesWritten = linesWritten;
  }

  /**
   * @return the linesInput
   */
  public long getLinesInput() {
    return linesInput;
  }

  /**
   * @param linesInput the linesInput to set
   */
  public void setLinesInput( long linesInput ) {
    this.linesInput = linesInput;
  }

  /**
   * @return the linesOutput
   */
  public long getLinesOutput() {
    return linesOutput;
  }

  /**
   * @param linesOutput the linesOutput to set
   */
  public void setLinesOutput( long linesOutput ) {
    this.linesOutput = linesOutput;
  }

  /**
   * @return the linesUpdated
   */
  public long getLinesUpdated() {
    return linesUpdated;
  }

  /**
   * @param linesUpdated the linesUpdated to set
   */
  public void setLinesUpdated( long linesUpdated ) {
    this.linesUpdated = linesUpdated;
  }

  /**
   * @return the linesRejected
   */
  public long getLinesRejected() {
    return linesRejected;
  }

  /**
   * @param linesRejected the linesRejected to set
   */
  public void setLinesRejected( long linesRejected ) {
    this.linesRejected = linesRejected;
  }

  /**
   * @return the errors
   */
  public long getErrors() {
    return errors;
  }

  /**
   * @param errors the errors to set
   */
  public void setErrors( long errors ) {
    this.errors = errors;
  }

  /**
   * @return the inputBufferSize
   */
  public long getInputBufferSize() {
    return inputBufferSize;
  }

  /**
   * @param inputBufferSize the inputBufferSize to set
   */
  public void setInputBufferSize( long inputBufferSize ) {
    this.inputBufferSize = inputBufferSize;
  }

  /**
   * @return the outputBufferSize
   */
  public long getOutputBufferSize() {
    return outputBufferSize;
  }

  /**
   * @param outputBufferSize the outputBufferSize to set
   */
  public void setOutputBufferSize( long outputBufferSize ) {
    this.outputBufferSize = outputBufferSize;
  }

  /**
   * @return the seqNr
   */
  public int getSeqNr() {
    return seqNr;
  }

  /**
   * @param seqNr the seqNr to set
   */
  public void setSeqNr( int seqNr ) {
    this.seqNr = seqNr;
  }

  /**
   * @return the pipelineName
   */
  public String getParentName() {
    return parentName;
  }

  /**
   * @param parentName the pipelineName to set
   */
  public void setParentName( String parentName ) {
    this.parentName = parentName;
  }

}
