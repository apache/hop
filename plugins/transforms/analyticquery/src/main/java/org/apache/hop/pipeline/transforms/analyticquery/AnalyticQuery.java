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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Performs analytic queries (LEAD/LAG, etc) based on a group
 *
 * @author ngoodman
 * @since 27-jan-2009
 */
public class AnalyticQuery extends BaseTransform<AnalyticQueryMeta, AnalyticQueryData> implements ITransform<AnalyticQueryMeta, AnalyticQueryData> {

  private static final Class<?> PKG = AnalyticQuery.class; // For Translator

  public AnalyticQuery( TransformMeta transformMeta, AnalyticQueryMeta meta, AnalyticQueryData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row!

    if ( first ) {
      // What is the output looking like?
      //
      data.inputRowMeta = getInputRowMeta();

      // In case we have 0 input rows, we still want to send out a single row aggregate
      // However... the problem then is that we don't know the layout from receiving it from the previous transform over the
      // row set.
      // So we need to calculated based on the metadata...
      //
      if ( data.inputRowMeta == null ) {
        data.inputRowMeta = getPipelineMeta().getPrevTransformFields( this, getTransformMeta() );
      }

      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      data.groupnrs = new int[ meta.getGroupField().length ];
      for ( int i = 0; i < meta.getGroupField().length; i++ ) {
        data.groupnrs[ i ] = data.inputRowMeta.indexOfValue( meta.getGroupField()[ i ] );
        if ( data.groupnrs[ i ] < 0 ) {
          logError( BaseMessages.getString(
            PKG, "AnalyticQuery.Log.GroupFieldCouldNotFound", meta.getGroupField()[ i ] ) );
          setErrors( 1 );
          stopAll();
          return false;
        }
      }

      // Setup of "window size" and "queue_size"
      int maxOffset = 0;
      for ( int i = 0; i < meta.getNumberOfFields(); i++ ) {
        if ( meta.getValueField()[ i ] > maxOffset ) {
          maxOffset = meta.getValueField()[ i ];
        }
      }
      data.window_size = maxOffset;
      data.queue_size = ( maxOffset * 2 ) + 1;

      // After we've processed the metadata we're all set
      first = false;

    }

    /* If our row is null we're done, clear the queue and end otherwise process the row */
    if ( r == null ) {
      clearQueue();
      setOutputDone();
      return false;
    } else {
      /* First with every group change AND the first row */
      if ( !sameGroup( this.data.previous, r ) ) {
        clearQueue();
        resetGroup();
      }
      /* Add this row to the end of the queue */
      data.data.add( r );
      /* Push the extra records off the end of the queue */
      while ( data.data.size() > data.queue_size ) {
        data.data.poll();
      }

      data.previous = r.clone();

      processQueue();
    }

    if ( log.isBasic() && checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "LineNr", getLinesRead() ) );
    }

    return true;
  }

  public void processQueue() throws HopTransformException {

    // If we've filled up our queue for processing
    if ( data.data.size() == data.queue_size ) {
      // Bring current cursor "up to current"
      if ( data.queue_cursor <= data.window_size ) {
        while ( data.queue_cursor <= data.window_size ) {
          processQueueObjectAt( data.queue_cursor + 1 );
          data.queue_cursor++;
        }
      } else {
        processQueueObjectAt( data.window_size + 1 );
      }
    }
  }

  public void clearQueue() throws HopTransformException {

    if ( data.data == null ) {
      return;
    }

    int numberOfRows = data.data.size();

    for ( int i = data.queue_cursor; i < numberOfRows; i++ ) {
      processQueueObjectAt( i + 1 );
    }

  }

  public void processQueueObjectAt( int i ) throws HopTransformException {
    int index = i - 1;
    Object[] rows = data.data.toArray();

    Object[] fields = new Object[ meta.getNumberOfFields() ];
    for ( int j = 0; j < meta.getNumberOfFields(); j++ ) {
      // field_index is the location inside a row of the subject of this
      // ie, ORDERTOTAL might be the subject ofthis field lag or lead
      // so we determine that ORDERTOTAL's index in the row
      int fieldIndex = data.inputRowMeta.indexOfValue( meta.getSubjectField()[ j ] );
      int rowIndex = 0;
      switch ( meta.getAggregateType()[ j ] ) {
        case AnalyticQueryMeta.TYPE_FUNCT_LAG:
          rowIndex = index - meta.getValueField()[ j ];
          break;
        case AnalyticQueryMeta.TYPE_FUNCT_LEAD:
          rowIndex = index + meta.getValueField()[ j ];
          break;
        default:
          break;
      }
      if ( rowIndex < rows.length && rowIndex >= 0 ) {
        Object[] singleRow = (Object[]) rows[ rowIndex ];
        if ( singleRow != null && singleRow[ fieldIndex ] != null ) {
          fields[ j ] = ( (Object[]) rows[ rowIndex ] )[ fieldIndex ];
        } else {
          // set default
          fields[ j ] = null;
        }
      } else {
        // set default
        fields[ j ] = null;
      }
    }

    Object[] newRow = RowDataUtil.addRowData( (Object[]) rows[ index ], data.inputRowMeta.size(), fields );

    putRow( data.outputRowMeta, newRow );

  }

  public void resetGroup() {
    data.data = new ConcurrentLinkedQueue<>();
    data.queue_cursor = 0;
  }

  // Is the row r of the same group as previous?
  private boolean sameGroup( Object[] previous, Object[] r ) throws HopValueException {
    if ( ( r == null && previous != null ) || ( previous == null && r != null ) ) {
      return false;
    } else {
      return data.inputRowMeta.compare( previous, r, data.groupnrs ) == 0;
    }

  }

  @Override
  public boolean init() {
    if ( super.init() ) {
      return true;
    } else {
      return false;
    }

  }

}
