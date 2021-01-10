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

package org.apache.hop.pipeline.transforms.salesforceinsert;

import com.google.common.annotations.VisibleForTesting;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
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
public class SalesforceInsert extends SalesforceTransform<SalesforceInsertMeta, SalesforceInsertData> {
  private static Class<?> PKG = SalesforceInsertMeta.class; // For Translator


  public SalesforceInsert( TransformMeta transformMeta, SalesforceInsertMeta meta, SalesforceInsertData data, int copyNr,
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

      data.sfBuffer = new SObject[ meta.getBatchSizeInt() ];
      data.outputBuffer = new Object[ meta.getBatchSizeInt() ][];

      // get total fields in the grid
      data.nrFields = meta.getUpdateLookup().length;

      // Check if field list is filled
      if ( data.nrFields == 0 ) {
        throw new HopException( BaseMessages.getString(
          PKG, "SalesforceInsertDialog.FieldsMissing.DialogMessage" ) );
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
          throw new HopException( BaseMessages.getString( PKG, "SalesforceInsert.CanNotFindField", meta
            .getUpdateStream()[ i ] ) );
        }
      }
    }

    try {
      writeToSalesForce( outputRowData );

    } catch ( Exception e ) {
      throw new HopTransformException( BaseMessages.getString( PKG, "SalesforceInsert.log.Exception", e ) );
    }
    return true;
  }

  @VisibleForTesting
  void writeToSalesForce( Object[] rowData ) throws HopException {
    try {

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "SalesforceInsert.WriteToSalesforce", data.iBufferPos, meta
          .getBatchSizeInt() ) );
      }

      // if there is room in the buffer
      if ( data.iBufferPos < meta.getBatchSizeInt() ) {
        ArrayList<XmlObject> insertfields = new ArrayList<>();
        // Reserve for empty fields
        ArrayList<String> fieldsToNull = new ArrayList<>();

        // Add fields to insert
        for ( int i = 0; i < data.nrFields; i++ ) {
          IValueMeta valueMeta = data.inputRowMeta.getValueMeta( data.fieldnrs[ i ] );
          Object value = rowData[ data.fieldnrs[ i ] ];

          if ( valueMeta.isNull( value ) ) {
            // The value is null
            // We need to keep track of this field
            fieldsToNull.add( SalesforceUtils.getFieldToNullName( log, meta.getUpdateLookup()[ i ], meta
              .getUseExternalId()[ i ] ) );
          } else {
            Object normalObject = normalizeValue( valueMeta, value );
            insertfields.add( SalesforceConnection.createMessageElement(
              meta.getUpdateLookup()[ i ], normalObject, meta.getUseExternalId()[ i ] ) );
          }
        }

        // build the SObject
        SObject sobjPass = new SObject();
        sobjPass.setType( data.connection.getModule() );

        if ( insertfields.size() > 0 ) {
          for ( XmlObject element : insertfields ) {
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
          logDetailed( BaseMessages.getString( PKG, "SalesforceInsert.CallingFlushBuffer" ) );
        }
        flushBuffers();
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "SalesforceInsert.Error", e.getMessage() ) );
    }
  }

  private void flushBuffers() throws HopException {
    try {
      // create the object(s) by sending the array to the web service
      data.saveResult = data.connection.insert( data.sfBuffer );
      for ( int j = 0; j < data.saveResult.length; j++ ) {
        if ( data.saveResult[ j ].isSuccess() ) {
          // Row was inserted
          String id = data.saveResult[ j ].getId();
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "SalesforceInsert.RowInserted", id ) );
          }

          // write out the row with the SalesForce ID
          Object[] newRow = RowDataUtil.resizeArray( data.outputBuffer[ j ], data.outputRowMeta.size() );

          if ( data.realSalesforceFieldName != null ) {
            int newIndex = getInputRowMeta().size();
            newRow[ newIndex++ ] = id;
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "SalesforceInsert.NewRow", newRow[ 0 ] ) );
          }

          putRow( data.outputRowMeta, newRow ); // copy row to output rowset(s);
          incrementLinesOutput();

          if ( checkFeedback( getLinesInput() ) ) {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "SalesforceInsert.log.LineRow", getLinesInput() ) );
            }
          }

        } else {
          // there were errors during the create call, go through the
          // errors
          // array and write them to the screen

          if ( !getTransformMeta().isDoingErrorHandling() ) {

            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString( PKG, "SalesforceInsert.ErrorFound" ) );
            }

            // Only show the first error
            //
            com.sforce.soap.partner.Error err = data.saveResult[ j ].getErrors()[ 0 ];
            throw new HopException( BaseMessages
              .getString( PKG, "SalesforceInsert.Error.FlushBuffer", new Integer( j ), err.getStatusCode(), err
                .getMessage() ) );
          }

          String errorMessage = "";
          for ( int i = 0; i < data.saveResult[ j ].getErrors().length; i++ ) {
            // get the next error
            com.sforce.soap.partner.Error err = data.saveResult[ j ].getErrors()[ i ];
            errorMessage +=
              BaseMessages.getString( PKG, "SalesforceInsert.Error.FlushBuffer", new Integer( j ), err
                .getStatusCode(), err.getMessage() );
          }

          // Simply add this row to the error row
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "SalesforceInsert.PassingRowToErrorTransform" ) );
          }
          putError( getInputRowMeta(), data.outputBuffer[ j ], 1, errorMessage, null, "SalesforceInsert001" );

        }

      }

      // reset the buffers
      data.sfBuffer = new SObject[ meta.getBatchSizeInt() ];
      data.outputBuffer = new Object[ meta.getBatchSizeInt() ][];
      data.iBufferPos = 0;

    } catch ( Exception e ) {
      if ( !getTransformMeta().isDoingErrorHandling() ) {
        throw new HopException( BaseMessages.getString( PKG, "SalesforceInsert.FailedToInsertObject", e
          .getMessage() ) );
      }
      // Simply add this row to the error row
      if ( log.isDebug() ) {
        logDebug( "Passing row to error transform" );
      }

      for ( int i = 0; i < data.iBufferPos; i++ ) {
        putError( data.inputRowMeta, data.outputBuffer[ i ], 1, e.getMessage(), null, "SalesforceInsert002" );
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
        String salesfoceIdFieldname = resolve( meta.getSalesforceIDFieldName() );
        if ( !Utils.isEmpty( salesfoceIdFieldname ) ) {
          data.realSalesforceFieldName = salesfoceIdFieldname;
        }

        // Do we need to rollback all changes on error?
        data.connection.setRollbackAllChangesOnError( meta.isRollbackAllChangesOnError() );

        // Now connect ...
        data.connection.connect();
      } catch ( HopException ke ) {
        logError( BaseMessages.getString( PKG, "SalesforceInsert.Log.ErrorOccurredDuringTransformInitialize" )
          + ke.getMessage() );
        return false;
      }
      return true;
    } else {
      return false;
    }
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
