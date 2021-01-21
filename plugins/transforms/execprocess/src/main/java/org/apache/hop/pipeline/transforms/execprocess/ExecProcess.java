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

package org.apache.hop.pipeline.transforms.execprocess;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Execute a process *
 *
 * @author Samatar
 * @since 03-11-2008
 */

public class ExecProcess extends BaseTransform<ExecProcessMeta, ExecProcessData> implements ITransform<ExecProcessMeta, ExecProcessData> {

  private static final Class<?> PKG = ExecProcessMeta.class; // For Translator

  public ExecProcess( TransformMeta transformMeta, ExecProcessMeta meta, ExecProcessData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.NrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = data.previousRowMeta;
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Check is process field is provided
      if ( Utils.isEmpty( meta.getProcessField() ) ) {
        logError( BaseMessages.getString( PKG, "ExecProcess.Error.ProcessFieldMissing" ) );
        throw new HopException( BaseMessages.getString( PKG, "ExecProcess.Error.ProcessFieldMissing" ) );
      }

      // cache the position of the field
      if ( data.indexOfProcess < 0 ) {
        data.indexOfProcess = data.previousRowMeta.indexOfValue( meta.getProcessField() );
        if ( data.indexOfProcess < 0 ) {
          // The field is unreachable !
          logError( BaseMessages.getString( PKG, "ExecProcess.Exception.CouldnotFindField" )
            + "[" + meta.getProcessField() + "]" );
          throw new HopException( BaseMessages.getString( PKG, "ExecProcess.Exception.CouldnotFindField", meta
            .getProcessField() ) );
        }
      }
      if ( meta.isArgumentsInFields() ) {
        if ( data.indexOfArguments == null ) {
          data.indexOfArguments = new int[ meta.getArgumentFieldNames().length ];
          for ( int i = 0; i < data.indexOfArguments.length; i++ ) {
            String fieldName = meta.getArgumentFieldNames()[ i ];
            data.indexOfArguments[ i ] = data.previousRowMeta.indexOfValue( fieldName );
            if ( data.indexOfArguments[ i ] < 0 ) {
              logError( BaseMessages.getString( PKG, "ExecProcess.Exception.CouldnotFindField" )
                + "[" + fieldName + "]" );
              throw new HopException(
                BaseMessages.getString( PKG, "ExecProcess.Exception.CouldnotFindField", fieldName ) );
            }
          }
        }
      }
    } // End If first

    Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    for ( int i = 0; i < data.NrPrevFields; i++ ) {
      outputRow[ i ] = r[ i ];
    }
    // get process to execute
    String processString = data.previousRowMeta.getString( r, data.indexOfProcess );

    ProcessResult processResult = new ProcessResult();

    try {
      if ( Utils.isEmpty( processString ) ) {
        throw new HopException( BaseMessages.getString( PKG, "ExecProcess.ProcessEmpty" ) );
      }

      // execute and return result
      if ( meta.isArgumentsInFields() ) {
        List<String> cmdArray = new ArrayList<>();
        cmdArray.add( processString );

        for ( int i = 0; i < data.indexOfArguments.length; i++ ) {
          // Runtime.exec will fail on null array elements
          // Convert to an empty string if value is null
          String argString = data.previousRowMeta.getString( r, data.indexOfArguments[ i ] );
          cmdArray.add( Const.NVL( argString, "" ) );
        }

        execProcess( cmdArray.toArray( new String[ 0 ] ), processResult );
      } else {
        execProcess( processString, processResult );
      }

      if ( meta.isFailWhenNotSuccess() ) {
        if ( processResult.getExistStatus() != 0 ) {
          String errorString = processResult.getErrorStream();
          if ( Utils.isEmpty( errorString ) ) {
            errorString = processResult.getOutputStream();
          }
          throw new HopException( errorString );
        }
      }

      // Add result field to input stream
      int rowIndex = data.NrPrevFields;
      outputRow[ rowIndex++ ] = processResult.getOutputStream();

      // Add result field to input stream
      outputRow[ rowIndex++ ] = processResult.getErrorStream();

      // Add result field to input stream
      outputRow[ rowIndex++ ] = processResult.getExistStatus();

      // add new values to the row.
      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "ExecProcess.LineNumber", getLinesRead()
          + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( HopException e ) {

      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "ExecProcess.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "ExecProcess001" );
      }
    }

    return true;
  }

  private void execProcess( String process, ProcessResult processresult ) throws HopException {
    execProcess( new String[] { process }, processresult );
  }

  private void execProcess( String[] process, ProcessResult processresult ) throws HopException {

    Process p = null;
    try {
      String errorMsg = null;
      // execute process
      try {
        if ( !meta.isArgumentsInFields() ) {
          p = data.runtime.exec( process[ 0 ] );
        } else {
          p = data.runtime.exec( process );
        }
      } catch ( Exception e ) {
        errorMsg = e.getMessage();
      }
      if ( p == null ) {
        processresult.setErrorStream( errorMsg );
      } else {
        // get output stream
        processresult.setOutputStream( getOutputString( new BufferedReader( new InputStreamReader( p
          .getInputStream() ) ) ) );

        // get error message
        processresult.setErrorStream( getOutputString( new BufferedReader( new InputStreamReader( p
          .getErrorStream() ) ) ) );

        // Wait until end
        p.waitFor();

        // get exit status
        processresult.setExistStatus( p.exitValue() );
      }
    } catch ( IOException ioe ) {
      throw new HopException( "IO exception while running the process " + Arrays.toString( process ) + "!", ioe );
    } catch ( InterruptedException ie ) {
      throw new HopException( "Interrupted exception while running the process " + Arrays.toString( process ) + "!", ie );
    } catch ( Exception e ) {
      throw new HopException( e );
    } finally {
      if ( p != null ) {
        p.destroy();
      }
    }
  }

  private String getOutputString( BufferedReader b ) throws IOException {
    StringBuilder retvalBuff = new StringBuilder();
    String line;
    String delim = meta.getOutputLineDelimiter();
    if ( delim == null ) {
      delim = "";
    } else {
      delim = resolve( delim );
    }

    while ( ( line = b.readLine() ) != null ) {
      if ( retvalBuff.length() > 0 ) {
        retvalBuff.append( delim );
      }
      retvalBuff.append( line );
    }
    return retvalBuff.toString();
  }

  @Override
  public boolean init(){

    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "ExecProcess.Error.ResultFieldMissing" ) );
        return false;
      }
      data.runtime = Runtime.getRuntime();
      return true;
    }
    return false;
  }

}
