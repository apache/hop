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

package org.apache.hop.pipeline.transforms.salesforceupdate;

import com.google.common.annotations.VisibleForTesting;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransform;
import org.apache.hop.pipeline.transforms.salesforceutils.SalesforceUtils;

import java.util.ArrayList;

/**
 * Read data from Salesforce module, convert them to rows and writes these to one or more output streams.
 *
 * @author jstairs, Samatar
 * @since 10-06-2007
 */
public class SalesforceUpdate extends SalesforceTransform<SalesforceUpdateMeta, SalesforceUpdateData> {
  private static Class<?> PKG = SalesforceUpdateMeta.class; // For Translator

  public SalesforceUpdate( TransformMeta transformMeta, SalesforceUpdateMeta meta, SalesforceUpdateData data, int copyNr, PipelineMeta pipelineMeta,
                           Pipeline pipeline ) {
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

      data.sfBuffer = new SObject[ meta.getBatchSizeInt() ];
      data.outputBuffer = new Object[ meta.getBatchSizeInt() ][];

      // get total fields in the grid
      data.nrFields = meta.getUpdateLookup().length;

      // Check if field list is filled
      if ( data.nrFields == 0 ) {
        throw new HopException( BaseMessages.getString( PKG,
          "SalesforceUpdateDialog.FieldsMissing.DialogMessage" ) );
      }

      // Create the output row meta-data
      data.inputRowMeta = getInputRowMeta().clone();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Build the mapping of input position to field name
      data.fieldnrs = new int[ meta.getUpdateStream().length ];
      for ( int i = 0; i < meta.getUpdateStream().length; i++ ) {
        data.fieldnrs[ i ] = getInputRowMeta().indexOfValue( meta.getUpdateStream()[ i ] );
        if ( data.fieldnrs[ i ] < 0 ) {
          throw new HopException( "Field [" + meta.getUpdateStream()[ i ]
            + "] couldn't be found in the input stream!" );
        }
      }
    }

    try {
      writeToSalesForce( outputRowData );

    } catch ( Exception e ) {
      throw new HopTransformException( BaseMessages.getString( PKG, "SalesforceUpdate.log.Exception" ), e );
    }
    return true;
  }

  @VisibleForTesting
  void writeToSalesForce( Object[] rowData ) throws HopException {
    try {

      if ( log.isDetailed() ) {
        logDetailed( "Called writeToSalesForce with " + data.iBufferPos + " out of " + meta.getBatchSizeInt() );
      }

      // if there is room in the buffer
      if ( data.iBufferPos < meta.getBatchSizeInt() ) {
        // Reserve for empty fields
        ArrayList<String> fieldsToNull = new ArrayList<>();
        ArrayList<XmlObject> updatefields = new ArrayList<>();

        // Add fields to update
        for ( int i = 0; i < data.nrFields; i++ ) {
          boolean valueIsNull = data.inputRowMeta.isNull( rowData, data.fieldnrs[ i ] );
          if ( valueIsNull ) {
            // The value is null
            // We need to keep track of this field
            fieldsToNull.add( SalesforceUtils.getFieldToNullName( log, meta.getUpdateLookup()[ i ], meta
              .getUseExternalId()[ i ] ) );
          } else {
            IValueMeta valueMeta = data.inputRowMeta.getValueMeta( data.fieldnrs[ i ] );
            Object value = rowData[ data.fieldnrs[ i ] ];
            Object normalObject = normalizeValue( valueMeta, value );
            updatefields.add( SalesforceConnection.createMessageElement( meta.getUpdateLookup()[ i ],
              normalObject, meta.getUseExternalId()[ i ] ) );
          }
        }

        // build the SObject
        SObject sobjPass = new SObject();
        sobjPass.setType( data.connection.getModule() );
        if ( updatefields.size() > 0 ) {
          for ( XmlObject element : updatefields ) {
            sobjPass.setSObjectField( element.getName().getLocalPart(), element.getValue() );
          }
        }
        if ( fieldsToNull.size() > 0 ) {
          // Set Null to fields
          sobjPass.setFieldsToNull( fieldsToNull.toArray( new String[ fieldsToNull.size() ] ) );
        }

        // Load the buffer array
        data.sfBuffer[ data.iBufferPos ] = sobjPass;
        data.outputBuffer[ data.iBufferPos ] = rowData;
        data.iBufferPos++;
      }

      if ( data.iBufferPos >= meta.getBatchSizeInt() ) {
        if ( log.isDetailed() ) {
          logDetailed( "Calling flush buffer from writeToSalesForce" );
        }
        flushBuffers();
      }
    } catch ( Exception e ) {
      throw new HopException( "\nFailed in writeToSalesForce: " + e.getMessage() );
    }
  }

  private void flushBuffers() throws HopException {

    try {
      if ( data.sfBuffer.length > data.iBufferPos ) {
        SObject[] smallBuffer = new SObject[ data.iBufferPos ];
        System.arraycopy( data.sfBuffer, 0, smallBuffer, 0, data.iBufferPos );
        data.sfBuffer = smallBuffer;
      }
      // update the object(s) by sending the array to the web service
      data.saveResult = data.connection.update( data.sfBuffer );
      int nr = data.saveResult.length;
      for ( int j = 0; j < nr; j++ ) {
        if ( data.saveResult[ j ].isSuccess() ) {
          // Row was updated
          String id = data.saveResult[ j ].getId();
          if ( log.isDetailed() ) {
            logDetailed( "Row updated with id: " + id );
          }

          // write out the row with the SalesForce ID
          Object[] newRow = RowDataUtil.resizeArray( data.outputBuffer[ j ], data.outputRowMeta.size() );

          if ( log.isDetailed() ) {
            logDetailed( "The new row has an id value of : " + newRow[ 0 ] );
          }

          putRow( data.outputRowMeta, newRow ); // copy row to output rowset(s);
          incrementLinesUpdated();

          if ( checkFeedback( getLinesInput() ) ) {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SalesforceUpdate.log.LineRow", "" + getLinesInput() ) );
            }
          }

        } else {
          // there were errors during the create call, go through the
          // errors
          // array and write them to the screen

          if ( !getTransformMeta().isDoingErrorHandling() ) {
            if ( log.isDetailed() ) {
              logDetailed( "Found error from SalesForce and raising the exception" );
            }

            // Only send the first error
            //
            com.sforce.soap.partner.Error err = data.saveResult[ j ].getErrors()[ 0 ];
            throw new HopException( BaseMessages.getString( PKG, "SalesforceUpdate.Error.FlushBuffer",
              new Integer( j ), err.getStatusCode(), err.getMessage() ) );
          }

          String errorMessage = "";
          for ( int i = 0; i < data.saveResult[ j ].getErrors().length; i++ ) {
            // get the next error
            com.sforce.soap.partner.Error err = data.saveResult[ j ].getErrors()[ i ];
            errorMessage +=
              BaseMessages.getString( PKG, "SalesforceUpdate.Error.FlushBuffer", new Integer( j ), err
                .getStatusCode(), err.getMessage() );
          }

          // Simply add this row to the error row
          if ( log.isDebug() ) {
            logDebug( "Passing row to error transform" );
          }

          putError( getInputRowMeta(), data.outputBuffer[ j ], 1, errorMessage, null, "SalesforceUpdate001" );

        }

      }

      // reset the buffers
      data.sfBuffer = new SObject[ meta.getBatchSizeInt() ];
      data.outputBuffer = new Object[ meta.getBatchSizeInt() ][];
      data.iBufferPos = 0;

    } catch ( Exception e ) {
      if ( !getTransformMeta().isDoingErrorHandling() ) {
        throw new HopException( "\nFailed to update object, error message was: \n" + e.getMessage() );
      }

      // Simply add this row to the error row
      if ( log.isDebug() ) {
        logDebug( "Passing row to error transform" );
      }

      for ( int i = 0; i < data.iBufferPos; i++ ) {
        putError( data.inputRowMeta, data.outputBuffer[ i ], 1, e.getMessage(), null, "SalesforceUpdate002" );
      }

    } finally {
      if ( data.saveResult != null ) {
        data.saveResult = null;
      }
    }

  }

  @Override
  public boolean init() {
    if ( super.init() ) {
      try {
        // Do we need to rollback all changes on error
        data.connection.setRollbackAllChangesOnError( meta.isRollbackAllChangesOnError() );

        // Now connect ...
        data.connection.connect();

        return true;
      } catch ( HopException ke ) {
        logError( BaseMessages.getString( PKG, "SalesforceUpdate.Log.ErrorOccurredDuringTransformInitialize" ) + ke
          .getMessage() );
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
    if ( data.sfBuffer != null ) {
      data.sfBuffer = null;
    }
    super.dispose();
  }

}
