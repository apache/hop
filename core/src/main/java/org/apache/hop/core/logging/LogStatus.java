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

/**
 * This enumeration describes the logging status in a logging table for pipelines and workflows.
 *
 * @author matt
 */
public enum LogStatus {

  START( "start" ), END( "end" ), STOP( "stop" ), ERROR( "error" ), RUNNING( "running" ), PAUSED( "paused" );

  private String status;

  private LogStatus( String status ) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return status;
  }

  public boolean equals( LogStatus logStatus ) {
    return status.equalsIgnoreCase( logStatus.status );
  }

  /**
   * Find the LogStatus based on the string description of the status.
   *
   * @param status the status string to search for
   * @return the LogStatus or null if none is found
   */
  public static LogStatus findStatus( String status ) {
    for ( LogStatus logStatus : values() ) {
      if ( logStatus.status.equalsIgnoreCase( status ) ) {
        return logStatus;
      }
    }
    return null;
  }
}
