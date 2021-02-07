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

package org.apache.hop.pipeline.transforms.groupby;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Groups data based on aggregation rules. (sum, count, ...)
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class GroupBy extends BaseTransform<GroupByMeta, GroupByData> implements ITransform<GroupByMeta, GroupByData> {

  private static final Class<?> PKG = GroupByMeta.class; // For Translator

  private boolean allNullsAreZero = false;
  private boolean minNullIsValued = false;

  public GroupBy( TransformMeta transformMeta, GroupByMeta meta, GroupByData data, int copyNr, PipelineMeta pipelineMeta,
                  Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row!

    if ( first ) {
      String val = getVariable( Const.HOP_AGGREGATION_ALL_NULLS_ARE_ZERO, "N" );
      allNullsAreZero = ValueMetaBase.convertStringToBoolean( val );
      val = getVariable( Const.HOP_AGGREGATION_MIN_NULL_IS_VALUED, "N" );
      minNullIsValued = ValueMetaBase.convertStringToBoolean( val );

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

      // Do all the work we can beforehand
      // Calculate indexes, loop up fields, etc.
      //
      data.counts = new long[ meta.getAggregations().size() ];
      data.subjectnrs = new int[ meta.getAggregations().size() ];

      data.cumulativeSumSourceIndexes = new ArrayList<>();
      data.cumulativeSumTargetIndexes = new ArrayList<>();

      data.cumulativeAvgSourceIndexes = new ArrayList<>();
      data.cumulativeAvgTargetIndexes = new ArrayList<>();

      for ( int i = 0; i < meta.getAggregations().size(); i++ ) {
        Aggregation aggregation = meta.getAggregations().get( i );
        if ( aggregation.getType() == GroupByMeta.TYPE_GROUP_COUNT_ANY ) {
          data.subjectnrs[ i ] = 0;
        } else {
          data.subjectnrs[ i ] = data.inputRowMeta.indexOfValue( aggregation.getSubject() );
        }
        if ( ( r != null ) && ( data.subjectnrs[ i ] < 0 ) ) {
          logError( BaseMessages.getString( PKG, "GroupBy.Log.AggregateSubjectFieldCouldNotFound",
            aggregation.getSubject() ) );
          setErrors( 1 );
          stopAll();
          return false;
        }

        if ( aggregation.getType() == GroupByMeta.TYPE_GROUP_CUMULATIVE_SUM ) {
          data.cumulativeSumSourceIndexes.add( data.subjectnrs[ i ] );

          // The position of the target in the output row is the input row size + i
          //
          data.cumulativeSumTargetIndexes.add( data.inputRowMeta.size() + i );
        }
        if ( aggregation.getType() == GroupByMeta.TYPE_GROUP_CUMULATIVE_AVERAGE ) {
          data.cumulativeAvgSourceIndexes.add( data.subjectnrs[ i ] );

          // The position of the target in the output row is the input row size + i
          //
          data.cumulativeAvgTargetIndexes.add( data.inputRowMeta.size() + i );
        }

      }

      data.previousSums = new Object[ data.cumulativeSumTargetIndexes.size() ];

      data.previousAvgSum = new Object[ data.cumulativeAvgTargetIndexes.size() ];
      data.previousAvgCount = new long[ data.cumulativeAvgTargetIndexes.size() ];

      data.groupnrs = new int[ meta.getGroupField().length ];
      for ( int i = 0; i < meta.getGroupField().length; i++ ) {
        data.groupnrs[ i ] = data.inputRowMeta.indexOfValue( meta.getGroupField()[ i ] );
        if ( ( r != null ) && ( data.groupnrs[ i ] < 0 ) ) {
          logError( BaseMessages.getString( PKG, "GroupBy.Log.GroupFieldCouldNotFound", meta.getGroupField()[ i ] ) );
          setErrors( 1 );
          stopAll();
          return false;
        }
      }

      // Create a metadata value for the counter Integers
      //
      data.valueMetaInteger = new ValueMetaInteger( "count" );
      data.valueMetaNumber = new ValueMetaNumber( "sum" );

      // Initialize the group metadata
      //
      initGroupMeta( data.inputRowMeta );
    }

    if ( first || data.newBatch ) {
      // Create a new group aggregate (init)
      //
      newAggregate( r );
    }

    if ( first ) {
      // for speed: groupMeta+aggMeta
      //
      data.groupAggMeta = new RowMeta();
      data.groupAggMeta.addRowMeta( data.groupMeta );
      data.groupAggMeta.addRowMeta( data.aggMeta );
    }

    if ( r == null ) { // no more input to be expected... (or none received in the first place)
      handleLastOfGroup();
      setOutputDone();
      return false;
    }

    if ( first || data.newBatch ) {
      first = false;
      data.newBatch = false;

      data.previous = data.inputRowMeta.cloneRow( r ); // copy the row to previous
    } else {
      calcAggregate( data.previous );

      if ( meta.passAllRows() ) {
        addToBuffer( data.previous );
      }
    }

    if ( !sameGroup( data.previous, r ) ) {
      if ( meta.passAllRows() ) {
        // Not the same group: close output (if any)
        closeOutput();
        // Get all rows from the buffer!
        data.groupResult = getAggregateResult();
        Object[] row = getRowFromBuffer();

        long lineNr = 0;
        while ( row != null ) {
          int size = data.inputRowMeta.size();

          row = RowDataUtil.addRowData( row, size, data.groupResult );
          size += data.groupResult.length;

          lineNr++;

          if ( meta.isAddingLineNrInGroup() && !Utils.isEmpty( meta.getLineNrInGroupField() ) ) {
            Object lineNrValue = new Long( lineNr );
            // IValueMeta lineNrValueMeta = new ValueMeta(meta.getLineNrInGroupField(),
            // IValueMeta.TYPE_INTEGER);
            // lineNrValueMeta.setLength(9);
            row = RowDataUtil.addValueData( row, size, lineNrValue );
            size++;
          }

          addCumulativeSums( row );
          addCumulativeAverages( row );

          putRow( data.outputRowMeta, row );
          row = getRowFromBuffer();
        }
        closeInput();
      } else {
        Object[] result = buildResult( data.previous );
        if ( result != null ) {
          putRow( data.groupAggMeta, result ); // copy row to possible alternate rowset(s).
        }
      }
      newAggregate( r ); // Create a new group aggregate (init)
    }

    data.previous = data.inputRowMeta.cloneRow( r );

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "GroupBy.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  private void handleLastOfGroup() throws HopException {
    if ( meta.passAllRows() ) {
      // ALL ROWS

      if ( data.previous != null ) {
        calcAggregate( data.previous );
        addToBuffer( data.previous );
      }
      data.groupResult = getAggregateResult();

      Object[] row = getRowFromBuffer();

      long lineNr = 0;
      while ( row != null ) {
        int size = data.inputRowMeta.size();
        row = RowDataUtil.addRowData( row, size, data.groupResult );
        size += data.groupResult.length;
        lineNr++;

        if ( meta.isAddingLineNrInGroup() && !Utils.isEmpty( meta.getLineNrInGroupField() ) ) {
          Object lineNrValue = new Long( lineNr );
          // IValueMeta lineNrValueMeta = new ValueMeta(meta.getLineNrInGroupField(),
          // IValueMeta.TYPE_INTEGER);
          // lineNrValueMeta.setLength(9);
          row = RowDataUtil.addValueData( row, size, lineNrValue );
          size++;
        }

        addCumulativeSums( row );
        addCumulativeAverages( row );

        putRow( data.outputRowMeta, row );
        row = getRowFromBuffer();
      }
      closeInput();
    } else {
      // JUST THE GROUP + AGGREGATE

      // Don't forget the last set of rows...
      if ( data.previous != null ) {
        calcAggregate( data.previous );
      }
      Object[] result = buildResult( data.previous );
      if ( result != null ) {
        putRow( data.groupAggMeta, result );
      }
    }
  }

  private void addCumulativeSums( Object[] row ) throws HopValueException {

    // We need to adjust this row with cumulative averages?
    //
    for ( int i = 0; i < data.cumulativeSumSourceIndexes.size(); i++ ) {
      int sourceIndex = data.cumulativeSumSourceIndexes.get( i );
      Object previousTarget = data.previousSums[ i ];
      Object sourceValue = row[ sourceIndex ];

      int targetIndex = data.cumulativeSumTargetIndexes.get( i );

      IValueMeta sourceMeta = data.inputRowMeta.getValueMeta( sourceIndex );
      IValueMeta targetMeta = data.outputRowMeta.getValueMeta( targetIndex );

      // If the first values where null, or this is the first time around, just take the source value...
      //
      if ( targetMeta.isNull( previousTarget ) ) {
        row[ targetIndex ] = sourceMeta.convertToNormalStorageType( sourceValue );
      } else {
        // If the source value is null, just take the previous target value
        //
        if ( sourceMeta.isNull( sourceValue ) ) {
          row[ targetIndex ] = previousTarget;
        } else {
          row[ targetIndex ] = ValueDataUtil.plus( targetMeta, data.previousSums[ i ], sourceMeta, row[ sourceIndex ] );
        }
      }
      data.previousSums[ i ] = row[ targetIndex ];
    }

  }

  private void addCumulativeAverages( Object[] row ) throws HopValueException {

    // We need to adjust this row with cumulative sums
    //
    for ( int i = 0; i < data.cumulativeAvgSourceIndexes.size(); i++ ) {
      int sourceIndex = data.cumulativeAvgSourceIndexes.get( i );
      Object previousTarget = data.previousAvgSum[ i ];
      Object sourceValue = row[ sourceIndex ];

      int targetIndex = data.cumulativeAvgTargetIndexes.get( i );

      IValueMeta sourceMeta = data.inputRowMeta.getValueMeta( sourceIndex );
      IValueMeta targetMeta = data.outputRowMeta.getValueMeta( targetIndex );

      // If the first values where null, or this is the first time around, just take the source value...
      //
      Object sum = null;

      if ( targetMeta.isNull( previousTarget ) ) {
        sum = sourceMeta.convertToNormalStorageType( sourceValue );
      } else {
        // If the source value is null, just take the previous target value
        //
        if ( sourceMeta.isNull( sourceValue ) ) {
          sum = previousTarget;
        } else {
          if ( sourceMeta.isInteger() ) {
            sum = ValueDataUtil.plus( data.valueMetaInteger, data.previousAvgSum[ i ], sourceMeta, row[ sourceIndex ] );
          } else {
            sum = ValueDataUtil.plus( targetMeta, data.previousAvgSum[ i ], sourceMeta, row[ sourceIndex ] );
          }
        }
      }
      data.previousAvgSum[ i ] = sum;

      if ( !sourceMeta.isNull( sourceValue ) ) {
        data.previousAvgCount[ i ]++;
      }

      if ( sourceMeta.isInteger() ) {
        // Change to number as the exception
        //
        if ( sum == null ) {
          row[ targetIndex ] = null;
        } else {
          row[ targetIndex ] = new Double( ( (Long) sum ).doubleValue() / data.previousAvgCount[ i ] );
        }
      } else {
        row[ targetIndex ] = ValueDataUtil.divide( targetMeta, sum, data.valueMetaInteger, data.previousAvgCount[ i ] );
      }
    }

  }

  // Is the row r of the same group as previous?
  boolean sameGroup( Object[] previous, Object[] r ) throws HopValueException {
    return data.inputRowMeta.compare( previous, r, data.groupnrs ) == 0;
  }

  /**
   * used for junits in GroupByAggregationNullsTest
   *
   * @param row
   * @throws HopValueException
   */
  @SuppressWarnings( "unchecked" ) void calcAggregate( Object[] row ) throws HopValueException {
    for ( int i = 0; i < data.subjectnrs.length; i++ ) {
      Aggregation aggregation = meta.getAggregations().get( i );

      Object subj = row[ data.subjectnrs[ i ] ];
      IValueMeta subjMeta = data.inputRowMeta.getValueMeta( data.subjectnrs[ i ] );
      Object value = data.agg[ i ];
      IValueMeta valueMeta = data.aggMeta.getValueMeta( i );

      switch ( aggregation.getType() ) {
        case GroupByMeta.TYPE_GROUP_SUM:
          data.agg[ i ] = ValueDataUtil.sum( valueMeta, value, subjMeta, subj );
          break;
        case GroupByMeta.TYPE_GROUP_AVERAGE:
          if ( !subjMeta.isNull( subj ) ) {
            data.agg[ i ] = ValueDataUtil.sum( valueMeta, value, subjMeta, subj );
            data.counts[ i ]++;
          }
          break;
        case GroupByMeta.TYPE_GROUP_MEDIAN:
        case GroupByMeta.TYPE_GROUP_PERCENTILE:
        case GroupByMeta.TYPE_GROUP_PERCENTILE_NEAREST_RANK:
          if ( !subjMeta.isNull( subj ) ) {
            ( (List<Double>) data.agg[ i ] ).add( subjMeta.getNumber( subj ) );
          }
          break;
        case GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION:
        case GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION_SAMPLE:
          if ( !subjMeta.isNull( subj ) ) {
            data.counts[ i ]++;
            double n = data.counts[ i ];
            double x = subjMeta.getNumber( subj );
            // for standard deviation null is exact 0
            double sum = value == null ? new Double( 0 ) : (Double) value;
            double mean = data.mean[ i ];

            double delta = x - mean;
            mean = mean + ( delta / n );
            sum = sum + delta * ( x - mean );

            data.mean[ i ] = mean;
            data.agg[ i ] = sum;
          }
          break;
        case GroupByMeta.TYPE_GROUP_COUNT_DISTINCT:
          if ( !subjMeta.isNull( subj ) ) {
            if ( data.distinctObjs == null ) {
              data.distinctObjs = new Set[ meta.getAggregations().size() ];
            }
            if ( data.distinctObjs[ i ] == null ) {
              data.distinctObjs[ i ] = new TreeSet<>();
            }
            Object obj = subjMeta.convertToNormalStorageType( subj );
            if ( !data.distinctObjs[ i ].contains( obj ) ) {
              data.distinctObjs[ i ].add( obj );
              // null is exact 0, or we will not be able to ++.
              value = value == null ? new Long( 0 ) : value;
              data.agg[ i ] = (Long) value + 1;
            }
          }
          break;
        case GroupByMeta.TYPE_GROUP_COUNT_ALL:
          if ( !subjMeta.isNull( subj ) ) {
            data.counts[ i ]++;
          }
          break;
        case GroupByMeta.TYPE_GROUP_COUNT_ANY:
          data.counts[ i ]++;
          break;
        case GroupByMeta.TYPE_GROUP_MIN: {
          if ( subj == null && !minNullIsValued ) {
            // PDI-10250 do not compare null
            break;
          }
          // PDI-15648 set the initial value for further comparing
          if ( value == null && subj != null && !minNullIsValued ) {
            data.agg[ i ] = subj;
            break;
          }

          if ( subjMeta.isSortedDescending() ) {
            // Account for negation in ValueMeta.compare() - See PDI-2302
            if ( subjMeta.compare( value, valueMeta, subj ) < 0 ) {
              data.agg[ i ] = subj;
            }
          } else {
            if ( subjMeta.compare( subj, valueMeta, value ) < 0 ) {
              data.agg[ i ] = subj;
            }
          }
          break;
        }
        case GroupByMeta.TYPE_GROUP_MAX:
          if ( subjMeta.isSortedDescending() ) {
            // Account for negation in ValueMeta.compare() - See PDI-2302
            if ( subjMeta.compare( value, valueMeta, subj ) > 0 ) {
              data.agg[ i ] = subj;
            }
          } else {
            if ( subjMeta.compare( subj, valueMeta, value ) > 0 ) {
              data.agg[ i ] = subj;
            }
          }
          break;
        case GroupByMeta.TYPE_GROUP_FIRST:
          if ( !( subj == null ) && value == null ) {
            data.agg[ i ] = subj;
          }
          break;
        case GroupByMeta.TYPE_GROUP_LAST:
          if ( !( subj == null ) ) {
            data.agg[ i ] = subj;
          }
          break;
        case GroupByMeta.TYPE_GROUP_FIRST_INCL_NULL:
          // This is on purpose. The calculation of the
          // first field is done when setting up a new group
          // This is just the field of the first row
          // if (linesWritten==0) value.setValue(subj);
          break;
        case GroupByMeta.TYPE_GROUP_LAST_INCL_NULL:
          data.agg[ i ] = subj;
          break;
        case GroupByMeta.TYPE_GROUP_CONCAT_COMMA:
          if ( !( subj == null ) ) {
            StringBuilder sb = (StringBuilder) value;
            if ( sb.length() > 0 ) {
              sb.append( ", " );
            }
            sb.append( subjMeta.getString( subj ) );
          }
          break;
        case GroupByMeta.TYPE_GROUP_CONCAT_STRING:
          if ( !( subj == null ) ) {
            String separator = "";
            if ( !Utils.isEmpty( aggregation.getValue() ) ) {
              separator = resolve( aggregation.getValue() );
            }

            StringBuilder sb = (StringBuilder) value;
            if ( sb.length() > 0 ) {
              sb.append( separator );
            }
            sb.append( subjMeta.getString( subj ) );
          }

          break;
        default:
          break;
      }
    }
  }

  /**
   * used for junits in GroupByAggregationNullsTest
   *
   * @param r
   */
  void newAggregate( Object[] r ) throws HopException {
    // Put all the counters at 0
    for ( int i = 0; i < data.counts.length; i++ ) {
      data.counts[ i ] = 0;
    }
    data.distinctObjs = null;
    data.agg = new Object[ data.subjectnrs.length ];
    data.mean = new double[ data.subjectnrs.length ]; // sets all doubles to 0.0
    data.aggMeta = new RowMeta();

    for ( int i = 0; i < data.subjectnrs.length; i++ ) {
      Aggregation aggregation = meta.getAggregations().get( i );
      IValueMeta subjMeta = data.inputRowMeta.getValueMeta( data.subjectnrs[ i ] );
      Object v = null;
      IValueMeta vMeta = null;
      int aggType = aggregation.getType();
      String fieldName = aggregation.getField();

      switch ( aggType ) {
        case GroupByMeta.TYPE_GROUP_SUM:
        case GroupByMeta.TYPE_GROUP_AVERAGE:
        case GroupByMeta.TYPE_GROUP_CUMULATIVE_SUM:
        case GroupByMeta.TYPE_GROUP_CUMULATIVE_AVERAGE:
          if ( subjMeta.isNumeric() ) {
            try {
              vMeta = ValueMetaFactory.createValueMeta( fieldName, subjMeta.getType() );
            } catch ( HopPluginException e ) {
              vMeta = new ValueMetaNone( fieldName );
            }
          } else {
            vMeta = new ValueMetaNumber( fieldName );
          }
          break;
        case GroupByMeta.TYPE_GROUP_MEDIAN:
        case GroupByMeta.TYPE_GROUP_PERCENTILE:
        case GroupByMeta.TYPE_GROUP_PERCENTILE_NEAREST_RANK:
          vMeta = new ValueMetaNumber( fieldName );
          v = new ArrayList<Double>();
          break;
        case GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION:
        case GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION_SAMPLE:
          vMeta = new ValueMetaNumber( fieldName );
          break;
        case GroupByMeta.TYPE_GROUP_COUNT_DISTINCT:
        case GroupByMeta.TYPE_GROUP_COUNT_ANY:
        case GroupByMeta.TYPE_GROUP_COUNT_ALL:
          vMeta = new ValueMetaInteger( fieldName );
          break;
        case GroupByMeta.TYPE_GROUP_FIRST:
        case GroupByMeta.TYPE_GROUP_LAST:
        case GroupByMeta.TYPE_GROUP_FIRST_INCL_NULL:
        case GroupByMeta.TYPE_GROUP_LAST_INCL_NULL:
        case GroupByMeta.TYPE_GROUP_MIN:
        case GroupByMeta.TYPE_GROUP_MAX:
          vMeta = subjMeta.clone();
          vMeta.setName( fieldName );
          v = r == null ? null : r[ data.subjectnrs[ i ] ];
          break;
        case GroupByMeta.TYPE_GROUP_CONCAT_COMMA:
          vMeta = new ValueMetaString( fieldName );
          v = new StringBuilder();
          break;
        case GroupByMeta.TYPE_GROUP_CONCAT_STRING:
          vMeta = new ValueMetaString( fieldName );
          v = new StringBuilder();
          break;
        default:
          // TODO raise an error here because we cannot continue successfully maybe the UI should validate this
          throw new HopException("Please specify an aggregation type for field '"+fieldName+"'");
      }

      if ( ( subjMeta != null )
        && ( aggType != GroupByMeta.TYPE_GROUP_COUNT_ALL
        && aggType != GroupByMeta.TYPE_GROUP_COUNT_DISTINCT
        && aggType != GroupByMeta.TYPE_GROUP_COUNT_ANY ) ) {
        vMeta.setLength( subjMeta.getLength(), subjMeta.getPrecision() );
      }
      data.agg[ i ] = v;
      data.aggMeta.addValueMeta( vMeta );
    }

    // Also clear the cumulative data...
    //
    for ( int i = 0; i < data.previousSums.length; i++ ) {
      data.previousSums[ i ] = null;
    }
    for ( int i = 0; i < data.previousAvgCount.length; i++ ) {
      data.previousAvgCount[ i ] = 0L;
      data.previousAvgSum[ i ] = null;
    }
  }

  private Object[] buildResult( Object[] r ) throws HopValueException {
    Object[] result = null;
    if ( r != null || meta.isAlwaysGivingBackOneRow() ) {
      result = RowDataUtil.allocateRowData( data.groupnrs.length );
      if ( r != null ) {
        for ( int i = 0; i < data.groupnrs.length; i++ ) {
          result[ i ] = r[ data.groupnrs[ i ] ];
        }
      }

      result = RowDataUtil.addRowData( result, data.groupnrs.length, getAggregateResult() );
    }

    return result;
  }

  private void initGroupMeta( IRowMeta previousRowMeta ) throws HopValueException {
    data.groupMeta = new RowMeta();
    for ( int i = 0; i < data.groupnrs.length; i++ ) {
      data.groupMeta.addValueMeta( previousRowMeta.getValueMeta( data.groupnrs[ i ] ) );
    }
  }

  /**
   * Used for junits in GroupByAggregationNullsTest
   *
   * @return
   * @throws HopValueException
   */
  Object[] getAggregateResult() throws HopValueException {

    if ( data.subjectnrs == null ) {
      return new Object[ 0 ];
    }

    Object[] result = new Object[ data.subjectnrs.length ];

    for ( int i = 0; i < data.subjectnrs.length; i++ ) {
      Aggregation aggregation = meta.getAggregations().get(i);
      Object ag = data.agg[ i ];
      int aggType = aggregation.getType();
      String fieldName = aggregation.getField();
      switch ( aggType ) {
        case GroupByMeta.TYPE_GROUP_SUM:
          break;
        case GroupByMeta.TYPE_GROUP_AVERAGE:
          ag =
            ValueDataUtil.divide( data.aggMeta.getValueMeta( i ), ag,
              new ValueMetaInteger( "c" ), new Long( data.counts[ i ] ) );
          break;
        case GroupByMeta.TYPE_GROUP_MEDIAN:
        case GroupByMeta.TYPE_GROUP_PERCENTILE:
          double percentile = 50.0;
          if ( aggType == GroupByMeta.TYPE_GROUP_PERCENTILE ) {
            percentile = Double.parseDouble( aggregation.getValue() );
          }
          @SuppressWarnings( "unchecked" )
          List<Double> valuesList = (List<Double>) data.agg[ i ];
          double[] values = new double[ valuesList.size() ];
          for ( int v = 0; v < values.length; v++ ) {
            values[ v ] = valuesList.get( v );
          }
          ag = new Percentile().evaluate( values, percentile );
          break;
        case GroupByMeta.TYPE_GROUP_PERCENTILE_NEAREST_RANK:
          double percentileValue = 50.0;
          if ( aggType == GroupByMeta.TYPE_GROUP_PERCENTILE_NEAREST_RANK ) {
            percentileValue = Double.parseDouble( aggregation.getValue() );
          }
          @SuppressWarnings( "unchecked" )
          List<Double> latenciesList = (List<Double>) data.agg[ i ];
          Collections.sort( latenciesList );
          Double[] latencies = new Double[ latenciesList.size() ];
          latencies = latenciesList.toArray( latencies );
          int index = (int) Math.ceil( ( percentileValue / 100 ) * latencies.length );
          ag = latencies[ index - 1 ];
          break;
        case GroupByMeta.TYPE_GROUP_COUNT_ANY:
        case GroupByMeta.TYPE_GROUP_COUNT_ALL:
          ag = new Long( data.counts[ i ] );
          break;
        case GroupByMeta.TYPE_GROUP_COUNT_DISTINCT:
          break;
        case GroupByMeta.TYPE_GROUP_MIN:
          break;
        case GroupByMeta.TYPE_GROUP_MAX:
          break;
        case GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION: {
          if ( ag == null ) {
            // PMD-1037 - when all input data is null ag is null, npe on access ag
            break;
          }
          double sum = (Double) ag / data.counts[ i ];
          ag = Math.sqrt( sum );
          break;
        }
        case GroupByMeta.TYPE_GROUP_STANDARD_DEVIATION_SAMPLE: {
          if ( ag == null ) {
            break;
          }
          double sum = (Double) ag / ( data.counts[ i ] - 1 );
          ag = Math.sqrt( sum );
          break;
        }
        case GroupByMeta.TYPE_GROUP_CONCAT_COMMA:
        case GroupByMeta.TYPE_GROUP_CONCAT_STRING:
          ag = ( (StringBuilder) ag ).toString();
          break;
        default:
          break;
      }
      if ( ag == null && allNullsAreZero ) {
        // PDI-10250, 6960 seems all rows for min function was nulls...
        // get output subject meta based on original subject meta calculation
        IValueMeta vm = data.aggMeta.getValueMeta( i );
        ag = ValueDataUtil.getZeroForValueMetaType( vm );
      }
      result[ i ] = ag;
    }

    return result;

  }

  // Method is defined as package-protected in order to be accessible by unit tests
  void addToBuffer( Object[] row ) throws HopFileException {
    data.bufferList.add( row );
    if ( data.bufferList.size() > 5000 && data.rowsOnFile == 0 ) {
      String pathToTmp = resolve( getMeta().getDirectory() );
      try {
        File ioFile = new File( pathToTmp );
        if ( !ioFile.exists() ) {
          // try to resolve as Apache VFS file
          pathToTmp = retrieveVfsPath( pathToTmp );
        }
        data.tempFile = File.createTempFile( getMeta().getPrefix(), ".tmp", new File( pathToTmp ) );
        data.fosToTempFile = new FileOutputStream( data.tempFile );
        data.dosToTempFile = new DataOutputStream( data.fosToTempFile );
        data.firstRead = true;
      } catch ( IOException e ) {
        throw new HopFileException( BaseMessages.getString( PKG, "GroupBy.Exception.UnableToCreateTemporaryFile" ),
          e );
      }
      // OK, save the oldest rows to disk!
      Object[] oldest = data.bufferList.get( 0 );
      data.inputRowMeta.writeData( data.dosToTempFile, oldest );
      data.bufferList.remove( 0 );
      data.rowsOnFile++;
    }
  }

  // Method is defined as public in order to be accessible by unit tests
  public String retrieveVfsPath( String pathToTmp ) throws HopFileException {
    FileObject vfsFile = HopVfs.getFileObject( pathToTmp );
    String path = vfsFile.getName().getPath();
    return path;
  }

  private Object[] getRowFromBuffer() throws HopFileException {
    if ( data.rowsOnFile > 0 ) {
      if ( data.firstRead ) {
        // Open the inputstream first...
        try {
          data.fisToTmpFile = new FileInputStream( data.tempFile );
          data.disToTmpFile = new DataInputStream( data.fisToTmpFile );
          data.firstRead = false;
        } catch ( IOException e ) {
          throw new HopFileException( BaseMessages.getString(
            PKG, "GroupBy.Exception.UnableToReadBackRowFromTemporaryFile" ), e );
        }
      }

      // Read one row from the file!
      Object[] row;
      try {
        row = data.inputRowMeta.readData( data.disToTmpFile );
      } catch ( SocketTimeoutException e ) {
        throw new HopFileException( e ); // Shouldn't happen on files
      }
      data.rowsOnFile--;

      return row;
    } else {
      if ( data.bufferList.size() > 0 ) {
        Object[] row = data.bufferList.get( 0 );
        data.bufferList.remove( 0 );
        return row;
      } else {
        return null; // Nothing left!
      }
    }
  }

  private void closeOutput() throws HopFileException {
    try {
      if ( data.dosToTempFile != null ) {
        data.dosToTempFile.close();
        data.dosToTempFile = null;
      }
      if ( data.fosToTempFile != null ) {
        data.fosToTempFile.close();
        data.fosToTempFile = null;
      }
      data.firstRead = true;
    } catch ( IOException e ) {
      throw new HopFileException(
        BaseMessages.getString( PKG, "GroupBy.Exception.UnableToCloseInputStream", data.tempFile.getPath() ), e );
    }
  }

  private void closeInput() throws HopFileException {
    try {
      if ( data.fisToTmpFile != null ) {
        data.fisToTmpFile.close();
        data.fisToTmpFile = null;
      }
      if ( data.disToTmpFile != null ) {
        data.disToTmpFile.close();
        data.disToTmpFile = null;
      }
    } catch ( IOException e ) {
      throw new HopFileException(
        BaseMessages.getString( PKG, "GroupBy.Exception.UnableToCloseInputStream", data.tempFile.getPath() ), e );
    }
  }

  @Override
  public boolean init( ) {

    if ( super.init() ) {
      data.bufferList = new ArrayList<>();

      data.rowsOnFile = 0;

      return true;
    }
    return false;
  }

  @Override
  public void dispose() {
    if ( data.tempFile != null ) {
      try {
        closeInput();
        closeOutput();
      } catch ( HopFileException e ) {
        log.logError( e.getLocalizedMessage() );
      }

      boolean tempFileDeleted = data.tempFile.delete();

      if ( !tempFileDeleted && log.isDetailed() ) {
        log.logDetailed(
          BaseMessages.getString( PKG, "GroupBy.Exception.UnableToDeleteTemporaryFile", data.tempFile.getPath() ) );
      }
    }

    super.dispose();
  }

  @Override
  public void batchComplete() throws HopException {
    handleLastOfGroup();
    data.newBatch = true;
  }

  /**
   * Used for junits in GroupByAggregationNullsTest
   *
   * @param allNullsAreZero the allNullsAreZero to set
   */
  void setAllNullsAreZero( boolean allNullsAreZero ) {
    this.allNullsAreZero = allNullsAreZero;
  }

  /**
   * Used for junits in GroupByAggregationNullsTest
   *
   * @param minNullIsValued the minNullIsValued to set
   */
  void setMinNullIsValued( boolean minNullIsValued ) {
    this.minNullIsValued = minNullIsValued;
  }

  public GroupByMeta getMeta() {
    return meta;
  }
}
