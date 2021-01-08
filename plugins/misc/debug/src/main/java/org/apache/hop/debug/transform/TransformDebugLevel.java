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

package org.apache.hop.debug.transform;

import org.apache.hop.core.Condition;
import org.apache.hop.core.logging.LogLevel;

public class TransformDebugLevel implements Cloneable {
  private LogLevel logLevel;
  private int startRow;
  private int endRow;
  private Condition condition;

  public TransformDebugLevel() {
    condition = new Condition();
    logLevel = LogLevel.DEBUG;
    startRow = -1;
    endRow = -1;
  }

  public TransformDebugLevel( LogLevel logLevel ) {
    this();
    this.logLevel = logLevel;
  }

  public TransformDebugLevel( LogLevel logLevel, int startRow, int endRow, Condition condition ) {
    this( logLevel );
    this.startRow = startRow;
    this.endRow = endRow;
    this.condition = condition;
  }

  @Override public String toString() {
    String s = logLevel.toString();
    if ( startRow >= 0 ) {
      s += ", start row=" + startRow;
    }
    if ( endRow >= 0 ) {
      s += ", end row=" + endRow;
    }
    if ( condition != null && !condition.isEmpty() ) {
      s += ", condition=" + condition.toString();
    }
    return s;
  }

  @Override public TransformDebugLevel clone() {
    return new TransformDebugLevel( logLevel, startRow, endRow, (Condition) condition.clone() );
  }

  /**
   * Gets logLevel
   *
   * @return value of logLevel
   */
  public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * @param logLevel The logLevel to set
   */
  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  /**
   * Gets startRow
   *
   * @return value of startRow
   */
  public int getStartRow() {
    return startRow;
  }

  /**
   * @param startRow The startRow to set
   */
  public void setStartRow( int startRow ) {
    this.startRow = startRow;
  }

  /**
   * Gets endRow
   *
   * @return value of endRow
   */
  public int getEndRow() {
    return endRow;
  }

  /**
   * @param endRow The endRow to set
   */
  public void setEndRow( int endRow ) {
    this.endRow = endRow;
  }

  /**
   * Gets condition
   *
   * @return value of condition
   */
  public Condition getCondition() {
    return condition;
  }

  /**
   * @param condition The condition to set
   */
  public void setCondition( Condition condition ) {
    this.condition = condition;
  }
}
