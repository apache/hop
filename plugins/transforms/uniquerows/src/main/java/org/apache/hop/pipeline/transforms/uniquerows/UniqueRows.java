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

package org.apache.hop.pipeline.transforms.uniquerows;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Removes the same consequetive rows from the input stream(s).
 *
 */
public class UniqueRows extends BaseTransform<UniqueRowsMeta, UniqueRowsData> implements ITransform<UniqueRowsMeta, UniqueRowsData> {
  private static final Class<?> PKG = UniqueRowsMeta.class; // For Translator

  public UniqueRows( TransformMeta transformMeta, UniqueRowsMeta meta, UniqueRowsData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row!
    if ( r == null ) { // no more input to be expected...

      // Don't forget the last set of rows...
      if ( data.previous != null ) {
        Object[] outputRow = addCounter( data.outputRowMeta, data.previous, data.counter );
        putRow( data.outputRowMeta, outputRow );
      }
      setOutputDone();
      return false;
    }

    if ( first ) {
      // Don't set first to false here like we normally do, because it is being checked outside the
      // if(first) block to determine whether to send the row as a duplicate.
      data.inputRowMeta = getInputRowMeta().clone();
      data.compareRowMeta = getInputRowMeta().clone();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      data.previous = data.inputRowMeta.cloneRow( r ); // copy the row

      // ICache lookup of fields
      data.fieldnrs = new int[ meta.getCompareFields().length ];

      for ( int i = 0; i < meta.getCompareFields().length; i++ ) {
        data.fieldnrs[ i ] = getInputRowMeta().indexOfValue( meta.getCompareFields()[ i ] );
        if ( data.fieldnrs[ i ] < 0 ) {
          logError( BaseMessages.getString(
            PKG, "UniqueRows.Log.CouldNotFindFieldInRow", meta.getCompareFields()[ i ] ) );
          setErrors( 1 );
          stopAll();
          return false;
        }
        // Change the case insensitive flag...
        //
        data.compareRowMeta.getValueMeta( data.fieldnrs[ i ] ).setCaseInsensitive( meta.getCaseInsensitive()[ i ] );

        if ( data.sendDuplicateRows ) {
          data.compareFields =
            data.compareFields == null ? meta.getCompareFields()[ i ] : data.compareFields
              + "," + meta.getCompareFields()[ i ];
        }
      }
      if ( data.sendDuplicateRows && !Utils.isEmpty( meta.getErrorDescription() ) ) {
        data.realErrorDescription = resolve( meta.getErrorDescription() );
      }
    }

    // Emptied in a previous batch in single threading mode.
    //
    if ( data.previous == null ) {
      data.previous = data.inputRowMeta.cloneRow( r );
    }

    boolean isEqual = false;

    if ( meta.getCompareFields() == null || meta.getCompareFields().length == 0 ) {
      // Compare the complete row...
      isEqual = data.outputRowMeta.compare( r, data.previous ) == 0;
    } else {
      isEqual = data.outputRowMeta.compare( r, data.previous, data.fieldnrs ) == 0;
    }
    if ( !isEqual ) {
      Object[] outputRow = addCounter( data.outputRowMeta, data.previous, data.counter );
      putRow( data.outputRowMeta, outputRow ); // copy row to possible alternate
      // rowset(s).
      data.previous = data.inputRowMeta.cloneRow( r );
      data.counter = 1;
    } else {
      data.counter++;
      if ( data.sendDuplicateRows && !first ) {
        putError( getInputRowMeta(), r, 1, data.realErrorDescription, Utils.isEmpty( data.compareFields )
          ? null : data.compareFields, "UNR001" );
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "UniqueRows.Log.LineNumber" ) + getLinesRead() );
      }
    }
    first = false;
    return true;
  }

  private Object[] addCounter( IRowMeta outputRowMeta, Object[] r, long count ) {
    if ( meta.isCountRows() ) {
      Object[] outputRow = RowDataUtil.addValueData( r, outputRowMeta.size() - 1, new Long( count ) );

      return outputRow;
    } else {
      return r; // nothing to do
    }
  }

  public boolean init() {

    if ( super.init() ) {
      // Add init code here.
      data.sendDuplicateRows = getTransformMeta().getTransformErrorMeta() != null && meta.supportsErrorHandling();
      return true;
    }
    return false;
  }

  @Override
  public void batchComplete() throws HopException {
    // If there's a previous row, output it at the end of the batch...
    //
    if ( data.previous != null ) {
      Object[] outputRow = addCounter( data.outputRowMeta, data.previous, data.counter );
      putRow( data.outputRowMeta, outputRow );
      data.previous = null;
    }
  }
}
