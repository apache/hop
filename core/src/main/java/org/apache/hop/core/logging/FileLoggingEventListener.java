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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;

import java.io.OutputStream;
import java.util.List;

public class FileLoggingEventListener implements IHopLoggingEventListener {

  private String filename;
  private FileObject file;

  public FileObject getFile() {
    return file;
  }

  private OutputStream outputStream;
  private HopLogLayout layout;

  private HopException exception;
  private String logChannelId;

  /**
   * Log all log lines to the specified file
   *
   * @param filename
   * @param append
   * @throws HopException
   */
  public FileLoggingEventListener( String filename, boolean append ) throws HopException {
    this( null, filename, append );
  }

  /**
   * Log only lines belonging to the specified log channel ID or one of it's children (grandchildren) to the specified
   * file.
   *
   * @param logChannelId
   * @param filename
   * @param append
   * @throws HopException
   */
  public FileLoggingEventListener( String logChannelId, String filename, boolean append ) throws HopException {
    this.logChannelId = logChannelId;
    this.filename = filename;
    this.layout = new HopLogLayout( true );
    this.exception = null;

    file = HopVfs.getFileObject( filename );
    outputStream = null;
    try {
      outputStream = HopVfs.getOutputStream( file, append );
    } catch ( Exception e ) {
      throw new HopException(
        "Unable to create a logging event listener to write to file '" + filename + "'", e );
    }
  }

  @Override
  public void eventAdded( HopLoggingEvent event ) {

    try {
      Object messageObject = event.getMessage();
      if ( messageObject instanceof LogMessage ) {
        boolean logToFile = false;

        if ( logChannelId == null ) {
          logToFile = true;
        } else {
          LogMessage message = (LogMessage) messageObject;
          // This should be fast enough cause cached.
          List<String> logChannelChildren = LoggingRegistry.getInstance().getLogChannelChildren( logChannelId );
          // This could be non-optimal, consider keeping the list sorted in the logging registry
          logToFile = Const.indexOfString( message.getLogChannelId(), logChannelChildren ) >= 0;
        }

        if ( logToFile ) {
          String logText = layout.format( event );
          outputStream.write( logText.getBytes() );
          outputStream.write( Const.CR.getBytes() );
          outputStream.flush();
        }
      }
    } catch ( Exception e ) {
      exception = new HopException( "Unable to write to logging event to file '" + filename + "'", e );
    }
  }

  public void close() throws HopException {
    try {
      if ( outputStream != null ) {
        outputStream.close();
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to close output of file '" + filename + "'", e );
    }
  }

  public HopException getException() {
    return exception;
  }

  public void setException( HopException exception ) {
    this.exception = exception;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  public void setOutputStream( OutputStream outputStream ) {
    this.outputStream = outputStream;
  }
}
