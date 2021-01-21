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

package org.apache.hop.pipeline.transforms.salesforcedelete;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransform;

/**
 * Read data from Salesforce module, convert them to rows and writes these to one or more output streams.
 *
 * @author jstairs,Samatar
 * @since 10-06-2007
 */
public class SalesforceDelete extends SalesforceTransform<SalesforceDeleteMeta,SalesforceDeleteData> implements ITransform<SalesforceDeleteMeta,SalesforceDeleteData> {
  private static Class<?> PKG = SalesforceDeleteMeta.class; // For Translator

  public SalesforceDelete( TransformMeta transformMeta, SalesforceDeleteMeta meta, SalesforceDeleteData data, int copyNr,
                           PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    // get one row ... This does some basic initialization of the objects, including loading the info coming in
    Object[] outputRowData = getRow();

    if ( outputRowData == null ) {
      if ( data.iBufferPos > 0 ) {
        flushBuffers();
      }
      setOutputDone();
      return false;
    }

    // If we haven't looked at a row before then do some basic setup.
    if ( first ) {
      first = false;

      data.deleteId = new String[meta.getBatchSizeInt()];
      data.outputBuffer = new Object[meta.getBatchSizeInt()][];

      // Create the output row meta-data
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Check deleteKeyField
      String realFieldName = resolve( meta.getDeleteField() );
      if ( Utils.isEmpty( realFieldName ) ) {
        throw new HopException( BaseMessages.getString( PKG, "SalesforceDelete.Error.DeleteKeyFieldMissing" ) );
      }

      // return the index of the field in the input stream
      data.indexOfKeyField = getInputRowMeta().indexOfValue( realFieldName );
      if ( data.indexOfKeyField < 0 ) {
        // the field is unreachable!
        throw new HopException( BaseMessages.getString(
          PKG, "SalesforceDelete.Error.CanNotFindFDeleteKeyField", realFieldName ) );
      }
    }

    try {
      writeToSalesForce( outputRowData );
    } catch ( Exception e ) {
      throw new HopTransformException( BaseMessages.getString( PKG, "SalesforceDelete.log.Exception" ), e );
    }
    return true;
  }

  private void writeToSalesForce( Object[] rowData ) throws HopException {
    try {

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "SalesforceDelete.Log.WriteToSalesforce", data.iBufferPos, meta
          .getBatchSizeInt() ) );
      }

      // if there is room in the buffer
      if ( data.iBufferPos < meta.getBatchSizeInt() ) {

        // Load the buffer array
        data.deleteId[data.iBufferPos] = getInputRowMeta().getString( rowData, data.indexOfKeyField );
        data.outputBuffer[data.iBufferPos] = rowData;
        data.iBufferPos++;
      }

      if ( data.iBufferPos >= meta.getBatchSizeInt() ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "SalesforceDelete.Log.CallingFlush" ) );
        }
        flushBuffers();
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "SalesforceDelete.Error.WriteToSalesforce", e
        .getMessage() ) );
    }
  }

  private void flushBuffers() throws HopException {

    try {
      if ( data.deleteId.length > data.iBufferPos ) {
        String[] smallBuffer = new String[data.iBufferPos];
        System.arraycopy( data.deleteId, 0, smallBuffer, 0, data.iBufferPos );
        data.deleteId = smallBuffer;
      }
      // delete the object(s) by sending the array to the web service
      data.deleteResult = data.connection.delete( data.deleteId );
      int nr = data.deleteResult.length;
      for ( int j = 0; j < nr; j++ ) {
        if ( data.deleteResult[j].isSuccess() ) {

          putRow( data.outputRowMeta, data.outputBuffer[j] ); // copy row to output rowset(s);
          incrementLinesOutput();

          if ( checkFeedback( getLinesInput() ) ) {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SalesforceDelete.log.LineRow", String
                .valueOf( getLinesInput() ) ) );
            }
          }

        } else {
          // there were errors during the create call, go through the
          // errors
          // array and write them to the screen

          if ( !getTransformMeta().isDoingErrorHandling() ) {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SalesforceDelete.Found.Error" ) );
            }

            com.sforce.soap.partner.Error err = data.deleteResult[j].getErrors()[0];
            throw new HopException( BaseMessages
              .getString( PKG, "SalesforceDelete.Error.FlushBuffer", new Integer( j ), err.getStatusCode(), err
                .getMessage() ) );

          }
          String errorMessage = "";
          int nrErrors = data.deleteResult[j].getErrors().length;
          for ( int i = 0; i < nrErrors; i++ ) {
            // get the next error
            com.sforce.soap.partner.Error err = data.deleteResult[j].getErrors()[i];
            errorMessage +=
              BaseMessages.getString( PKG, "SalesforceDelete.Error.FlushBuffer", new Integer( j ), err
                .getStatusCode(), err.getMessage() );
          }
          // Simply add this row to the error row
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "SalesforceDelete.PassingRowToErrorTransform" ) );
          }
          putError( getInputRowMeta(), data.outputBuffer[j], 1, errorMessage, null, "SalesforceDelete001" );
        }

      }

      // reset the buffers
      data.deleteId = new String[meta.getBatchSizeInt()];
      data.outputBuffer = new Object[meta.getBatchSizeInt()][];
      data.iBufferPos = 0;

    } catch ( Exception e ) {
      if ( !getTransformMeta().isDoingErrorHandling() ) {
        throw new HopException( BaseMessages
          .getString( PKG, "SalesforceDelete.FailedToDeleted", e.getMessage() ) );
      }
      // Simply add this row to the error row
      if ( log.isDebug() ) {
        logDebug( "Passing row to error transform" );
      }

      for ( int i = 0; i < data.iBufferPos; i++ ) {
        putError( data.inputRowMeta, data.outputBuffer[i], 1, e.getMessage(), null, "SalesforceDelete002" );
      }
    } finally {
      if ( data.deleteResult != null ) {
        data.deleteResult = null;
      }
    }

  }

  @Override
  public boolean init( ) {

    if ( super.init( ) ) {

      try {
        // set timeout
        data.connection.setTimeOut( Const.toInt( resolve( meta.getTimeout() ), 0 ) );
        // Do we use compression?
        data.connection.setUsingCompression( meta.isCompression() );
        // Do we rollback all changes on error
        data.connection.setRollbackAllChangesOnError( meta.isRollbackAllChangesOnError() );

        // Now connect ...
        data.connection.connect();

        return true;
      } catch ( HopException ke ) {
        logError( BaseMessages.getString( PKG, "SalesforceDelete.Log.ErrorOccurredDuringTransformInitialize" )
          + ke.getMessage() );
        return false;
      }
    }
    return false;
  }

  @Override
  public void dispose() {
    if ( data.outputBuffer != null ) {
      data.outputBuffer = null;
    }
    if ( data.deleteId != null ) {
      data.deleteId = null;
    }
    super.dispose();
  }

}
