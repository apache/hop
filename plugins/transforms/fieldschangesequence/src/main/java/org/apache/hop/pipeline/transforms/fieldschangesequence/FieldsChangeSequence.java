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

package org.apache.hop.pipeline.transforms.fieldschangesequence;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Add sequence to each input row.
 *
 * @author Samatar
 * @since 30-06-2008
 */

public class FieldsChangeSequence extends BaseTransform<FieldsChangeSequenceMeta, FieldsChangeSequenceData> implements ITransform<FieldsChangeSequenceMeta, FieldsChangeSequenceData> {

  private static final Class<?> PKG = FieldsChangeSequenceMeta.class; // For Translator

  public FieldsChangeSequence( TransformMeta transformMeta, FieldsChangeSequenceMeta meta, FieldsChangeSequenceData data, int copyNr,
                               PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      // get the RowMeta
      data.previousMeta = getInputRowMeta().clone();
      data.nextIndexField = data.previousMeta.size();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      if ( meta.getFieldName() == null || meta.getFieldName().length > 0 ) {
        data.fieldnr = meta.getFieldName().length;
        data.fieldnrs = new int[ data.fieldnr ];
        data.previousValues = new Object[ data.fieldnr ];
        data.fieldnrsMeta = new IValueMeta[ data.fieldnr ];
        for ( int i = 0; i < data.fieldnr; i++ ) {
          data.fieldnrs[ i ] = data.previousMeta.indexOfValue( meta.getFieldName()[ i ] );
          if ( data.fieldnrs[ i ] < 0 ) {
            logError( BaseMessages.getString(
              PKG, "FieldsChangeSequence.Log.CanNotFindField", meta.getFieldName()[ i ] ) );
            throw new HopException( BaseMessages.getString(
              PKG, "FieldsChangeSequence.Log.CanNotFindField", meta.getFieldName()[ i ] ) );
          }
          data.fieldnrsMeta[ i ] = data.previousMeta.getValueMeta( data.fieldnrs[ i ] );
        }
      } else {
        data.fieldnr = data.previousMeta.size();
        data.fieldnrs = new int[ data.fieldnr ];
        data.previousValues = new Object[ data.fieldnr ];
        data.fieldnrsMeta = new IValueMeta[ data.fieldnr ];
        for ( int i = 0; i < data.previousMeta.size(); i++ ) {
          data.fieldnrs[ i ] = i;
          data.fieldnrsMeta[ i ] = data.previousMeta.getValueMeta( i );
        }
      }

      data.startAt = Const.toInt( resolve( meta.getStart() ), 1 );
      data.incrementBy = Const.toInt( resolve( meta.getIncrement() ), 1 );
      data.seq = data.startAt;
    } // end if first

    try {
      boolean change = false;

      // Loop through fields
      for ( int i = 0; i < data.fieldnr; i++ ) {
        if ( !first ) {
          if ( data.fieldnrsMeta[ i ].compare( data.previousValues[ i ], r[ data.fieldnrs[ i ] ] ) != 0 ) {
            change = true;
          }
        }
        data.previousValues[ i ] = r[ data.fieldnrs[ i ] ];
      }
      if ( first ) {
        first = false;
      }

      if ( change ) {
        data.seq = data.startAt;
      }

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "FieldsChangeSequence.Log.ReadRow" )
          + getLinesRead() + " : " + getInputRowMeta().getString( r ) );
      }

      // reserve room and add value!
      Object[] outputRowData = RowDataUtil.addValueData( r, data.nextIndexField, data.seq );

      putRow( data.outputRowMeta, outputRowData ); // copy row to possible alternate rowset(s).

      data.seq += data.incrementBy;

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "FieldsChangeSequence.Log.WriteRow" )
          + getLinesWritten() + " : " + getInputRowMeta().getString( r ) );
      }

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "FieldsChangeSequence.Log.LineNumber" ) + getLinesRead() );
        }
      }

    } catch ( Exception e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "FieldsChangeSequence.ErrorInTransformRunning" ) + e.getMessage() );
        logError( Const.getStackTracker( e ) );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "FieldsChangeSequence001" );
      }
    }
    return true;
  }

  @Override
  public void dispose(){
    data.previousValues = null;
    data.fieldnrs = null;
    super.dispose();
  }
}
