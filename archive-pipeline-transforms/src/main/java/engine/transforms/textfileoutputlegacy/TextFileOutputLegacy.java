/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.textfileoutputlegacy;


import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.StreamLogger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.textfileoutput.TextFileOutput;

import java.io.IOException;

/**
 * This is deprecated version with capability run as command.
 *
 * @deprecated use {@link org.apache.hop.pipeline.transforms.textfileoutput.TextFileOutput} instead.
 */
@Deprecated
public class TextFileOutputLegacy extends TextFileOutput {

  public TextFileOutputLegacy( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                               Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  protected boolean writeRowTo( Object[] row ) throws HopException {
    if ( ( (TextFileOutputLegacyMeta) meta ).isFileAsCommand() ) {
      return writeRowToCommand( row );
    } else {
      return super.writeRowTo( row );
    }
  }

  private boolean writeRowToCommand( Object[] row ) throws HopException {
    if ( row != null ) {
      if ( data.writer == null ) {
        initCommandStreamWriter( environmentSubstitute( meta.getFileName() ) );
      }
      first = false;
      writeRow( data.outputRowMeta, row );
      putRow( data.outputRowMeta, row ); // in case we want it to go further...

      if ( checkFeedback( getLinesOutput() ) ) {
        logBasic( "linenr " + getLinesOutput() );
      }
      return true;
    } else {
      if ( ( data.writer == null ) && !Utils.isEmpty( meta.getEndedLine() ) ) {
        initCommandStreamWriter( environmentSubstitute( meta.getFileName() ) );
        initBinaryDataFields();
      }
      writeEndedLine();
      closeCommand();
      setOutputDone();
      return false;
    }
  }


  private boolean closeCommand() {
    boolean retval;

    try {
      if ( data.writer != null ) {
        data.writer.flush();

        // If writing a ZIP or GZIP file not from a command, do not close the writer or else
        // the closing of the ZipOutputStream below will throw an "already closed" exception.
        // Rather than checking for compression types, it is easier to check for cmdProc != null
        // because if that check fails, we know we will get into the ZIP/GZIP processing below.
        if ( log.isDebug() ) {
          logDebug( "Closing output stream" );
        }
        data.writer.close();
        if ( log.isDebug() ) {
          logDebug( "Closed output stream" );
        }
      }
      data.writer = null;
      if ( log.isDebug() ) {
        logDebug( "Ending running external command" );
      }

      if ( data.cmdProc != null ) {
        int procStatus = data.cmdProc.waitFor();
        // close the streams
        // otherwise you get "Too many open files, java.io.IOException" after a lot of iterations
        try {
          data.cmdProc.getErrorStream().close();
          data.cmdProc.getOutputStream().flush();
          data.cmdProc.getOutputStream().close();
          data.cmdProc.getInputStream().close();
        } catch ( IOException e ) {
          if ( log.isDetailed() ) {
            logDetailed( "Warning: Error closing streams: " + e.getMessage() );
          }
        }
        data.cmdProc = null;
        if ( log.isBasic() && procStatus != 0 ) {
          logBasic( "Command exit status: " + procStatus );
        }
      }

      retval = true;
    } catch ( Exception e ) {
      logError( "Exception trying to close file: " + e.toString() );
      setErrors( 1 );
      //Clean resources
      data.writer = null;
      retval = false;
    }

    return retval;
  }

  @Override
  protected void initOutput() throws HopException {
    if ( ( (TextFileOutputLegacyMeta) meta ).isFileAsCommand() ) {
      initCommandStreamWriter( environmentSubstitute( meta.getFileName() ) );
    } else {
      super.initOutput();
    }
  }

  @Override
  protected void close() throws IOException {
    if ( ( (TextFileOutputLegacyMeta) meta ).isFileAsCommand() ) {
      closeCommand();
    } else {
      super.close();
    }
  }

  private void initCommandStreamWriter( String cmdstr ) throws HopException {
    data.writer = null;
    try {
      if ( log.isDebug() ) {
        logDebug( "Spawning external process" );
      }
      if ( data.cmdProc != null ) {
        logError( "Previous command not correctly terminated" );
        setErrors( 1 );
      }
      if ( Const.getOS().equals( "Windows 95" ) ) {
        cmdstr = "command.com /C " + cmdstr;
      } else {
        if ( Const.getOS().startsWith( "Windows" ) ) {
          cmdstr = "cmd.exe /C " + cmdstr;
        }
      }
      if ( isDetailed() ) {
        logDetailed( "Starting: " + cmdstr );
      }
      Runtime runtime = Runtime.getRuntime();
      data.cmdProc = runtime.exec( cmdstr, EnvUtil.getEnvironmentVariablesForRuntimeExec() );
      data.writer = data.cmdProc.getOutputStream();

      StreamLogger stdoutLogger = new StreamLogger( log, data.cmdProc.getInputStream(), "(stdout)" );
      StreamLogger stderrLogger = new StreamLogger( log, data.cmdProc.getErrorStream(), "(stderr)" );
      new Thread( stdoutLogger ).start();
      new Thread( stderrLogger ).start();
    } catch ( Exception e ) {
      throw new HopException( "Error opening new file : " + e.toString() );
    }
  }
}

