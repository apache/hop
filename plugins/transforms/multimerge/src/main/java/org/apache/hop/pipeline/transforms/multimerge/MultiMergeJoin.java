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

package org.apache.hop.pipeline.transforms.multimerge;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merge rows from 2 sorted streams and output joined rows with matched key fields. Use this instead of hash join is
 * both your input streams are too big to fit in memory. Note that both the inputs must be sorted on the join key.
 * <p>
 * This is a first prototype implementation that only handles two streams and inner join. It also always outputs all
 * values from both streams. Ideally, we should: 1) Support any number of incoming streams 2) Allow user to choose the
 * join type (inner, outer) for each stream 3) Allow user to choose which fields to push to next transform 4) Have multiple
 * output ports as follows: a) Containing matched records b) Unmatched records for each input port 5) Support incoming
 * rows to be sorted either on ascending or descending order. The currently implementation only supports ascending
 *
 * @author Biswapesh
 * @since 24-nov-2006
 */

public class MultiMergeJoin extends BaseTransform<MultiMergeJoinMeta,MultiMergeJoinData> implements ITransform<MultiMergeJoinMeta,MultiMergeJoinData> {
  private static final Class<?> PKG = MultiMergeJoinMeta.class; // For Translator

  public MultiMergeJoin(TransformMeta transformMeta, MultiMergeJoinMeta meta, MultiMergeJoinData data, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private boolean processFirstRow( MultiMergeJoinMeta smi, MultiMergeJoinData sdi ) throws HopException {

    PipelineMeta pipelineMeta = getPipelineMeta();
    PipelineHopMeta pipelineHopMeta;

    ITransformIOMeta transformIOMeta = meta.getTransformIOMeta();
    List<IStream> infoStreams = transformIOMeta.getInfoStreams();
    IStream stream;
    TransformMeta toTransformMeta = meta.getParentTransformMeta();
    TransformMeta fromTransformMeta;

    ArrayList<String> inputTransformNameList = new ArrayList<>();
    String[] inputTransformNames = meta.getInputTransforms();
    String inputTransformName;

    for ( int i = 0; i < infoStreams.size(); i++ ) {
      inputTransformName = inputTransformNames[ i ];
      stream = infoStreams.get( i );
      fromTransformMeta = stream.getTransformMeta();
      if ( fromTransformMeta == null ) {
        //should not arrive here, shoud typically have been caught by init.
        throw new HopException(
          BaseMessages.getString( PKG, "MultiMergeJoin.Log.UnableToFindReferenceStream", inputTransformName ) );
      }
      //check the hop
      pipelineHopMeta = pipelineMeta.findPipelineHop( fromTransformMeta, toTransformMeta, true );
      //there is no hop: this is unexpected.
      if ( pipelineHopMeta == null ) {
        //should not arrive here, shoud typically have been caught by init.
        throw new HopException(
          BaseMessages.getString( PKG, "MultiMergeJoin.Log.UnableToFindReferenceStream", inputTransformName ) );
      } else if ( pipelineHopMeta.isEnabled() ) {
        inputTransformNameList.add( inputTransformName );
      } else {
        logDetailed( BaseMessages.getString( PKG, "MultiMergeJoin.Log.IgnoringTransform", inputTransformName ) );
      }
    }

    int streamSize = inputTransformNameList.size();
    if ( streamSize == 0 ) {
      return false;
    }

    String keyField;
    String[] keyFields;

    data.rowSets = new IRowSet[ streamSize ];
    IRowSet rowSet;
    Object[] row;
    data.rows = new Object[ streamSize ][];
    data.metas = new IRowMeta[ streamSize ];
    data.rowLengths = new int[ streamSize ];
    MultiMergeJoinData.QueueComparator comparator = new MultiMergeJoinData.QueueComparator( data );
    data.queue = new PriorityQueue<>( streamSize, comparator );
    data.results = new ArrayList<>( streamSize );
    MultiMergeJoinData.QueueEntry queueEntry;
    data.queueEntries = new MultiMergeJoinData.QueueEntry[ streamSize ];
    data.drainIndices = new int[ streamSize ];
    data.keyNrs = new int[ streamSize ][];
    data.dummy = new Object[ streamSize ][];

    IRowMeta rowMeta;
    data.outputRowMeta = new RowMeta();
    for ( int i = 0, j = 0; i < inputTransformNames.length; i++ ) {
      inputTransformName = inputTransformNames[ i ];
      if ( !inputTransformNameList.contains( inputTransformName ) ) {
        //ignore transform with disabled hop.
        continue;
      }

      queueEntry = new MultiMergeJoinData.QueueEntry();
      queueEntry.index = j;
      data.queueEntries[ j ] = queueEntry;

      data.results.add( new ArrayList<>() );

      rowSet = findInputRowSet( inputTransformName );
      if ( rowSet == null ) {
        throw new HopException( BaseMessages.getString(
          PKG, "MultiMergeJoin.Exception.UnableToFindSpecifiedTransform", inputTransformName ) );
      }
      data.rowSets[ j ] = rowSet;

      row = getRowFrom( rowSet );
      data.rows[ j ] = row;
      if ( row == null ) {
        rowMeta = getPipelineMeta().getTransformFields( this, inputTransformName );
        data.metas[ j ] = rowMeta;
      } else {
        queueEntry.row = row;
        rowMeta = rowSet.getRowMeta();

        keyField = meta.getKeyFields()[ i ];
        String[] keyFieldParts = keyField.split( "," );
        String keyFieldPart;
        data.keyNrs[ j ] = new int[ keyFieldParts.length ];
        for ( int k = 0; k < keyFieldParts.length; k++ ) {
          keyFieldPart = keyFieldParts[ k ];
          data.keyNrs[ j ][ k ] = rowMeta.indexOfValue( keyFieldPart );
          if ( data.keyNrs[ j ][ k ] < 0 ) {
            String message =
              BaseMessages.getString( PKG, "MultiMergeJoin.Exception.UnableToFindFieldInReferenceStream", keyFieldPart, inputTransformName );
            logError( message );
            throw new HopTransformException( message );
          }
        }
        data.metas[ j ] = rowMeta;
        data.queue.add( data.queueEntries[ j ] );
      }
      data.outputRowMeta.mergeRowMeta( rowMeta.clone() );
      data.rowLengths[ j ] = rowMeta.size();
      data.dummy[ j ] = RowDataUtil.allocateRowData( rowMeta.size() );
      j++;
    }
    return true;
  }

  public boolean processRow() throws HopException {

    if ( first ) {
      if ( !processFirstRow( meta, data ) ) {
        setOutputDone();
        return false;
      }
      first = false;
    }

    if ( log.isRowLevel() ) {
      String metaString =
        BaseMessages
          .getString( PKG, "MultiMergeJoin.Log.DataInfo", data.metas[ 0 ].getString( data.rows[ 0 ] ) + "" );
      for ( int i = 1; i < data.metas.length; i++ ) {
        metaString += data.metas[ i ].getString( data.rows[ i ] );
      }
      logRowlevel( metaString );
    }

    /*
     * We can stop processing if any of the following is true: a) All streams are empty b) Any stream is empty and join
     * type is INNER
     */
    int streamSize = data.metas.length;
    if ( data.optional ) {
      if ( data.queue.isEmpty() ) {
        setOutputDone();
        return false;
      }
      MultiMergeJoinData.QueueEntry minEntry = data.queue.poll();
      int drainSize = 1;
      data.rows[ minEntry.index ] = minEntry.row;
      data.drainIndices[ 0 ] = minEntry.index;
      MultiMergeJoinData.QueueComparator comparator = (MultiMergeJoinData.QueueComparator) data.queue.comparator();
      while ( !data.queue.isEmpty() && comparator.compare( data.queue.peek(), minEntry ) == 0 ) {
        MultiMergeJoinData.QueueEntry entry = data.queue.poll();
        data.rows[ entry.index ] = entry.row;
        data.drainIndices[ drainSize++ ] = entry.index;
      }
      int index;
      Object[] row = null;
      // rows from nonempty input streams match: get all equal rows and create result set
      for ( int i = 0; i < drainSize; i++ ) {
        index = data.drainIndices[ i ];
        data.results.get( index ).add( data.rows[ index ] );
        while ( !isStopped()
          && ( ( row = getRowFrom( data.rowSets[ index ] ) ) != null && data.metas[ index ].compare(
          data.rows[ index ], row, data.keyNrs[ index ] ) == 0 ) ) {
          data.results.get( index ).add( row );
        }
        if ( isStopped() ) {
          return false;
        }
        if ( row != null ) {
          data.queueEntries[ index ].row = row;
          data.queue.add( data.queueEntries[ index ] );
        }
      }
      for ( int i = 0; i < streamSize; i++ ) {
        data.drainIndices[ i ] = 0;
        if ( data.results.get( i ).isEmpty() ) {
          data.results.get( i ).add( data.dummy[ i ] );
        }
      }

      int current = 0;

      while ( true ) {
        for ( int i = 0; i < streamSize; i++ ) {
          data.rows[ i ] = data.results.get( i ).get( data.drainIndices[ i ] );
        }
        row = RowDataUtil.createResizedCopy( data.rows, data.rowLengths );

        putRow( data.outputRowMeta, row );

        while ( ++data.drainIndices[ current ] >= data.results.get( current ).size() ) {
          data.drainIndices[ current ] = 0;
          if ( ++current >= streamSize ) {
            break;
          }
        }
        if ( current >= streamSize ) {
          break;
        }
        current = 0;
      }
      for ( int i = 0; i < streamSize; i++ ) {
        data.results.get( i ).clear();
      }
    } else {
      if ( data.queue.size() < streamSize ) {
        data.queue.clear();
        for ( int i = 0; i < streamSize; i++ ) {
          while ( data.rows[ i ] != null && !isStopped() ) {
            data.rows[ i ] = getRowFrom( data.rowSets[ i ] );
          }
        }
        setOutputDone();
        return false;
      }

      MultiMergeJoinData.QueueEntry minEntry = data.queue.poll();
      int drainSize = 1;
      data.rows[ minEntry.index ] = minEntry.row;
      data.drainIndices[ 0 ] = minEntry.index;
      MultiMergeJoinData.QueueComparator comparator = (MultiMergeJoinData.QueueComparator) data.queue.comparator();
      while ( !data.queue.isEmpty() && comparator.compare( data.queue.peek(), minEntry ) == 0 ) {
        MultiMergeJoinData.QueueEntry entry = data.queue.poll();
        data.rows[ entry.index ] = entry.row;
        data.drainIndices[ drainSize++ ] = entry.index;
      }
      Object[] row = null;
      if ( data.queue.isEmpty() ) {
        // rows from all input streams match: get all equal rows and create result set
        for ( int i = 0; i < streamSize; i++ ) {
          data.results.get( i ).add( data.rows[ i ] );
          while ( !isStopped()
            && ( ( row = getRowFrom( data.rowSets[ i ] ) ) != null && data.metas[ i ].compare(
            data.rows[ i ], row, data.keyNrs[ i ] ) == 0 ) ) {
            data.results.get( i ).add( row );
          }
          if ( isStopped() ) {
            return false;
          }
          if ( row != null ) {
            data.queueEntries[ i ].row = row;
            data.queue.add( data.queueEntries[ i ] );
          }
        }
        for ( int i = 0; i < streamSize; i++ ) {
          data.drainIndices[ i ] = 0;
        }

        int current = 0;
        while ( true ) {
          for ( int i = 0; i < streamSize; i++ ) {
            data.rows[ i ] = data.results.get( i ).get( data.drainIndices[ i ] );
          }
          row = RowDataUtil.createResizedCopy( data.rows, data.rowLengths );

          putRow( data.outputRowMeta, row );
          while ( ++data.drainIndices[ current ] >= data.results.get( current ).size() ) {
            data.drainIndices[ current ] = 0;
            if ( ++current >= streamSize ) {
              break;
            }
          }
          if ( current >= streamSize ) {
            break;
          }
          current = 0;
        }
        for ( int i = 0; i < streamSize; i++ ) {
          data.results.get( i ).clear();
        }
      } else {
        // mismatch found and no results can be generated

        for ( int i = 0; i < drainSize; i++ ) {
          int index = data.drainIndices[ i ];
          while ( ( row = getRowFrom( data.rowSets[ index ] ) ) != null
            && data.metas[ index ].compare( data.rows[ index ], row, data.keyNrs[ index ] ) == 0 ) {
            if ( isStopped() ) {
              break;
            }
          }
          if ( isStopped() || row == null ) {
            break;
          }
          data.queueEntries[ index ].row = row;
          data.queue.add( data.queueEntries[ index ] );
        }
        if ( isStopped() ) {
          return false;
        }
      }
    }
    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "MultiMergeJoin.LineNumber" ) + getLinesRead() );
    }
    return true;
  }


  public boolean init() {

    if ( super.init() ) {
      ITransformIOMeta transformIOMeta = meta.getTransformIOMeta();
      String[] inputTransformNames = meta.getInputTransforms();
      String inputTransformName;
      List<IStream> infoStreams = transformIOMeta.getInfoStreams();
      IStream stream;
      for ( int i = 0; i < infoStreams.size(); i++ ) {
        inputTransformName = inputTransformNames[ i ];
        stream = infoStreams.get( i );
        if ( stream.getTransformMeta() == null ) {
          logError( BaseMessages.getString( PKG, "MultiMergeJoin.Log.UnableToFindReferenceStream", inputTransformName ) );
          return false;
        }
      }
      String joinType = meta.getJoinType();
      for ( int i = 0; i < MultiMergeJoinMeta.joinTypes.length; ++i ) {
        if ( joinType.equalsIgnoreCase( MultiMergeJoinMeta.joinTypes[ i ] ) ) {
          data.optional = MultiMergeJoinMeta.optionals[ i ];
          return true;
        }
      }
      logError( BaseMessages.getString( PKG, "MultiMergeJoin.Log.InvalidJoinType", meta.getJoinType() ) );
      return false;
    }
    return true;
  }

  /**
   * Checks whether incoming rows are join compatible. This essentially means that the keys being compared should be of
   * the same datatype and both rows should have the same number of keys specified
   *
   * @param rows Reference row
   * @return true when templates are compatible.
   */
  protected boolean isInputLayoutValid( IRowMeta[] rows ) {
    if ( rows != null ) {
      // Compare the key types
      String[] keyFields = meta.getKeyFields();
      /*
       * int nrKeyFields = keyFields.length;
       *
       * for (int i=0;i<nrKeyFields;i++) { IValueMeta v1 = rows[0].searchValueMeta(keyFields[i]); if (v1 ==
       * null) { return false; } for (int j = 1; j < rows.length; j++) { IValueMeta v2 =
       * rows[j].searchValueMeta(keyFields[i]); if (v2 == null) { return false; } if ( v1.getType()!=v2.getType() ) {
       * return false; } } }
       */
      // check 1 : keys are configured for each stream
      if ( rows.length != keyFields.length ) {
        logError( "keys are not configured for all the streams " );
        return false;
      }
      // check:2 No of keys are same for each stream
      int prevCount = 0;

      List<String[]> keyList = new ArrayList<>();
      for ( int i = 0; i < keyFields.length; i++ ) {
        String[] keys = keyFields[ i ].split( "," );
        keyList.add( keys );
        int count = keys.length;
        if ( i != 0 && prevCount != count ) {
          logError( "Number of keys do not match " );
          return false;
        } else {
          prevCount = count;
        }
      }

      // check:3 compare the key types
      for ( int i = 0; i < prevCount; i++ ) {
        IValueMeta preValue = null;
        for ( int j = 0; j < rows.length; j++ ) {
          IValueMeta v = rows[ j ].searchValueMeta( keyList.get( j )[ i ] );
          if ( v == null ) {
            return false;
          }
          if ( j != 0 && v.getType() != preValue.getType() ) {
            logError( "key data type do not match " );
            return false;
          } else {
            preValue = v;
          }
        }
      }
    }
    // we got here, all seems to be ok.
    return true;
  }

}
