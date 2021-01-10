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
