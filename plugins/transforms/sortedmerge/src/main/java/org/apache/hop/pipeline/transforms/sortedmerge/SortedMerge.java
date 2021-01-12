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

package org.apache.hop.pipeline.transforms.sortedmerge;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Do nothing. Pass all input data to the next transforms.
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class SortedMerge extends BaseTransform<SortedMergeMeta, SortedMergeData> implements ITransform<SortedMergeMeta, SortedMergeData> {
  private static final Class<?> PKG = SortedMergeMeta.class; // For Translator

  public SortedMerge(TransformMeta transformMeta, SortedMergeMeta meta, SortedMergeData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * We read from all streams in the partition merge mode For that we need at least one row on all input rowsets... If
   * we don't have a row, we wait for one.
   * <p>
   * TODO: keep the inputRowSets() list sorted and go from there. That should dramatically improve speed as you only
   * need half as many comparisons.
   *
   * @return the next row
   */
  private synchronized Object[] getRowSorted() throws HopException {
    if ( first ) {
      first = false;

      // Verify that socket connections to all the remote input transforms are opened
      // before we start to read/write ...
      //
      //openRemoteInputTransformSocketsOnce();

      // Read one row from all rowsets...
      //
      data.sortedBuffer = new ArrayList<>();
      data.rowMeta = null;

      // PDI-1212:
      // If one of the inputRowSets holds a null row (the input yields
      // 0 rows), then the null rowSet is removed from the InputRowSet buffer.. (BaseTransform.getRowFrom())
      // which throws this loop off by one (the next set never gets processed).
      // Instead of modifying BaseTransform, I figure reversing the loop here would
      // effect change in less areas. If the reverse loop causes a problem, please
      List<IRowSet> inputRowSets = getInputRowSets();
      for ( int i = inputRowSets.size() - 1; i >= 0 && !isStopped(); i-- ) {

        IRowSet rowSet = inputRowSets.get( i );
        Object[] row = getRowFrom( rowSet );
        if ( row != null ) {
          // Add this row to the sortedBuffer...
          // Which is not yet sorted, we'll get to that later.
          //
          data.sortedBuffer.add( new RowSetRow( rowSet, rowSet.getRowMeta(), row ) );
          if ( data.rowMeta == null ) {
            data.rowMeta = rowSet.getRowMeta().clone();
          }

          // What fields do we compare on and in what order?

          // Better cache the location of the partitioning column
          // First time operation only
          //
          if ( data.fieldIndices == null ) {
            // Get the indexes of the specified sort fields...
            data.fieldIndices = new int[ meta.getFieldName().length ];
            for ( int f = 0; f < data.fieldIndices.length; f++ ) {
              data.fieldIndices[ f ] = data.rowMeta.indexOfValue( meta.getFieldName()[ f ] );
              if ( data.fieldIndices[ f ] < 0 ) {
                throw new HopTransformException( "Unable to find fieldname ["
                  + meta.getFieldName()[ f ] + "] in row : " + data.rowMeta );
              }

              data.rowMeta.getValueMeta( data.fieldIndices[ f ] ).setSortedDescending( !meta.getAscending()[ f ] );
            }
          }
        }

        data.comparator = ( o1, o2 ) -> {
          try {
            return o1.getRowMeta().compare( o1.getRowData(), o2.getRowData(), data.fieldIndices );
          } catch ( HopValueException e ) {
            return 0; // TODO see if we should fire off alarms over here... Perhaps throw a RuntimeException.
          }
        };

        // Now sort the sortedBuffer for the first time.
        //
        Collections.sort( data.sortedBuffer, data.comparator );
      }
    }

    // If our sorted buffer is empty, it means we're done...
    //
    if ( data.sortedBuffer.isEmpty() ) {
      return null;
    }

    // now that we have all rows sorted, all we need to do is find out what the smallest row is.
    // The smallest row is the first in our case...
    //
    RowSetRow smallestRow = data.sortedBuffer.get( 0 );
    data.sortedBuffer.remove( 0 );
    Object[] outputRowData = smallestRow.getRowData();

    // We read another row from the row set where the smallest row came from.
    // That we we exhaust all row sets.
    //
    Object[] extraRow = getRowFrom( smallestRow.getRowSet() );

    // Add it to the sorted buffer in the right position...
    //
    if ( extraRow != null ) {
      // Add this one to the sortedBuffer
      //
      RowSetRow add = new RowSetRow( smallestRow.getRowSet(), smallestRow.getRowSet().getRowMeta(), extraRow );
      int index = Collections.binarySearch( data.sortedBuffer, add, data.comparator );
      if ( index < 0 ) {
        data.sortedBuffer.add( -index - 1, add );
      } else {
        data.sortedBuffer.add( index, add );
      }
    }

    // This concludes the regular program...
    //

    // optionally perform safe mode checking to prevent problems.
    //
    if ( getPipeline().isSafeModeEnabled() ) {
      // for checking we need to get data and meta
      //
      safeModeChecking( smallestRow.getRowMeta() );
    }

    return outputRowData;
  }

  public boolean processRow() throws HopException {

    Object[] row = getRowSorted(); // get row, sorted
    if ( row == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    putRow( data.rowMeta, row ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "SortedMerge.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      // data.rowComparator = new RowComparator();

      // Add init code here.
      return true;
    }
    return false;
  }

}
