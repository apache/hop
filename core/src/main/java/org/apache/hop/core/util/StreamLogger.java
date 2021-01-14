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

package org.apache.hop.core.util;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamLogger implements Runnable {
  private InputStream is;

  private String type;

  private ILogChannel log;

  private Boolean errorStream;

  private String lastLine;

  public StreamLogger( ILogChannel log, InputStream is, String type ) {
    this( log, is, type, false );
  }

  public StreamLogger( ILogChannel log, InputStream is, String type, Boolean errorStream ) {
    this.log = log;
    this.is = is;
    this.type = type;
    this.errorStream = errorStream;
  }

  @Override
  public void run() {
    try {
      InputStreamReader isr = new InputStreamReader( is );
      BufferedReader br = new BufferedReader( isr );
      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        lastLine = line;
        if ( errorStream ) {
          log.logError( type + " " + line );
        } else {
          log.logBasic( type + " " + line );
        }

      }
    } catch ( IOException ioe ) {
      log.logError( type + " " + Const.getStackTracker( ioe ) );
    }

  }

  public String getLastLine() {
    return lastLine;
  }

}
