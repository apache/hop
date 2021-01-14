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

public class HopLoggingEvent {

  private Object message;

  public long timeStamp;

  private LogLevel level;

  public HopLoggingEvent() {
    this( null, System.currentTimeMillis(), LogLevel.BASIC );
  }

  public HopLoggingEvent( Object message, long timeStamp, LogLevel level ) {
    super();
    this.message = message;
    this.timeStamp = timeStamp;
    this.level = level;
  }

  public Object getMessage() {
    return message;
  }

  public void setMessage( Object message ) {
    this.message = message;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp( long timeStamp ) {
    this.timeStamp = timeStamp;
  }

  public LogLevel getLevel() {
    return level;
  }

  public void setLevel( LogLevel level ) {
    this.level = level;
  }

}
