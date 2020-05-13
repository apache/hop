package org.apache.hop.beam.core;

import java.io.Serializable;

public class Failure implements Serializable {

  private String failingClass;
  private String stackTrace;
  private String sourceData;

  public Failure() {
  }

  public Failure( String failingClass, String stackTrace, String sourceData ) {
    this.failingClass = failingClass;
    this.stackTrace = stackTrace;
    this.sourceData = sourceData;
  }

  /**
   * Gets failingClass
   *
   * @return value of failingClass
   */
  public String getFailingClass() {
    return failingClass;
  }

  /**
   * @param failingClass The failingClass to set
   */
  public void setFailingClass( String failingClass ) {
    this.failingClass = failingClass;
  }

  /**
   * Gets stackTrace
   *
   * @return value of stackTrace
   */
  public String getStackTrace() {
    return stackTrace;
  }

  /**
   * @param stackTrace The stackTrace to set
   */
  public void setStackTrace( String stackTrace ) {
    this.stackTrace = stackTrace;
  }

  /**
   * Gets sourceData
   *
   * @return value of sourceData
   */
  public String getSourceData() {
    return sourceData;
  }

  /**
   * @param sourceData The sourceData to set
   */
  public void setSourceData( String sourceData ) {
    this.sourceData = sourceData;
  }
}
