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

package org.apache.hop.pipeline.transforms.denormaliser;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Denormalises data based on key-value pairs
 *
 * @author Matt
 * @since 17-jan-2006
 */
public class Denormaliser extends BaseTransform<DenormaliserMeta, DenormaliserData> implements ITransform<DenormaliserMeta, DenormaliserData> {

  private static final Class<?> PKG = DenormaliserMeta.class; // For Translator

  private boolean allNullsAreZero = false;
  private boolean minNullIsValued = false;

  private Map<String, IValueMeta> conversionMetaCache = new HashMap<>();

  public Denormaliser( TransformMeta transformMeta, DenormaliserMeta meta, DenormaliserData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // get row!

    if ( r == null ) {
      // no more input to be expected...
      handleLastRow();
      setOutputDone();
      return false;
    }

    if ( first ) {
      // perform all allocations
      if ( !processFirstRow() ) {
        // we failed on first row....
        return false;
      }

      newGroup(); // Create a new result row (init)
      deNormalise( data.inputRowMeta, r );
      data.previous = r; // copy the row to previous

      // we don't need feedback here
      first = false;

      // ok, we done with first row
      return true;
    }

    if ( !sameGroup( data.inputRowMeta, data.previous, r ) ) {

      Object[] outputRowData = buildResult( data.inputRowMeta, data.previous );
      putRow( data.outputRowMeta, outputRowData ); // copy row to possible alternate rowset(s).
      newGroup(); // Create a new group aggregate (init)
      deNormalise( data.inputRowMeta, r );
    } else {
      deNormalise( data.inputRowMeta, r );
    }

    data.previous = r;

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "Denormaliser.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  private boolean processFirstRow() throws HopTransformException {
    String val = getVariable( Const.HOP_AGGREGATION_ALL_NULLS_ARE_ZERO, "N" );
    this.allNullsAreZero = ValueMetaBase.convertStringToBoolean( val );
    val = getVariable( Const.HOP_AGGREGATION_MIN_NULL_IS_VALUED, "N" );
    this.minNullIsValued = ValueMetaBase.convertStringToBoolean( val );
    data.inputRowMeta = getInputRowMeta();
    data.outputRowMeta = data.inputRowMeta.clone();
    meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

    data.keyFieldNr = data.inputRowMeta.indexOfValue( meta.getKeyField() );
    if ( data.keyFieldNr < 0 ) {
      logError( BaseMessages.getString( PKG, "Denormaliser.Log.KeyFieldNotFound", meta.getKeyField() ) );
      setErrors( 1 );
      stopAll();
      return false;
    }

    Map<Integer, Integer> subjects = new Hashtable<>();
    data.fieldNameIndex = new int[ meta.getDenormaliserTargetField().length ];
    for ( int i = 0; i < meta.getDenormaliserTargetField().length; i++ ) {
      DenormaliserTargetField field = meta.getDenormaliserTargetField()[ i ];
      int idx = data.inputRowMeta.indexOfValue( field.getFieldName() );
      if ( idx < 0 ) {
        logError( BaseMessages.getString( PKG, "Denormaliser.Log.UnpivotFieldNotFound", field.getFieldName() ) );
        setErrors( 1 );
        stopAll();
        return false;
      }
      data.fieldNameIndex[ i ] = idx;
      subjects.put( Integer.valueOf( idx ), Integer.valueOf( idx ) );

      // See if by accident, the value fieldname isn't the same as the key fieldname.
      // This is not supported of-course and given the complexity of the transform, you can miss:
      if ( data.fieldNameIndex[ i ] == data.keyFieldNr ) {
        logError( BaseMessages.getString( PKG, "Denormaliser.Log.ValueFieldSameAsKeyField", field.getFieldName() ) );
        setErrors( 1 );
        stopAll();
        return false;
      }

      // Fill a hashtable with the key strings and the position(s) of the field(s) in the row to take.
      // Store the indexes in a List so that we can accommodate multiple key/value pairs...
      //
      String keyValue = resolve( field.getKeyValue() );
      List<Integer> indexes = data.keyValue.get( keyValue );
      if ( indexes == null ) {
        indexes = new ArrayList<>( 2 );
      }
      indexes.add( Integer.valueOf( i ) ); // Add the index to the list...
      data.keyValue.put( keyValue, indexes ); // store the list
    }

    Set<Integer> subjectSet = subjects.keySet();
    data.fieldNrs = subjectSet.toArray( new Integer[ subjectSet.size() ] );

    data.groupnrs = new int[ meta.getGroupField().length ];
    for ( int i = 0; i < meta.getGroupField().length; i++ ) {
      data.groupnrs[ i ] = data.inputRowMeta.indexOfValue( meta.getGroupField()[ i ] );
      if ( data.groupnrs[ i ] < 0 ) {
        logError( BaseMessages.getString( PKG, "Denormaliser.Log.GroupingFieldNotFound", meta.getGroupField()[ i ] ) );
        setErrors( 1 );
        stopAll();
        return false;
      }
    }

    List<Integer> removeList = new ArrayList<>();
    removeList.add( Integer.valueOf( data.keyFieldNr ) );
    for ( int i = 0; i < data.fieldNrs.length; i++ ) {
      removeList.add( data.fieldNrs[ i ] );
    }
    Collections.sort( removeList );

    data.removeNrs = new int[ removeList.size() ];
    for ( int i = 0; i < removeList.size(); i++ ) {
      data.removeNrs[ i ] = removeList.get( i );
    }
    return true;
  }

  private void handleLastRow() throws HopException {
    // Don't forget the last set of rows...
    if ( data.previous != null ) {
      // deNormalise(data.previous); --> That would over-do it.
      //
      Object[] outputRowData = buildResult( data.inputRowMeta, data.previous );
      putRow( data.outputRowMeta, outputRowData );
    }
  }

  /**
   * Used for junits in DenormaliserAggregationsTest
   *
   * @param rowMeta
   * @param rowData
   * @return
   * @throws HopValueException
   */
  Object[] buildResult( IRowMeta rowMeta, Object[] rowData ) throws HopValueException {
    // Deleting objects: we need to create a new object array
    // It's useless to call RowDataUtil.resizeArray
    //
    Object[] outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    int outputIndex = 0;

    // Copy the data from the incoming row, but remove the unwanted fields in the same loop...
    //
    int removeIndex = 0;
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( removeIndex < data.removeNrs.length && i == data.removeNrs[ removeIndex ] ) {
        removeIndex++;
      } else {
        outputRowData[ outputIndex++ ] = rowData[ i ];
      }
    }

    // Add the unpivoted fields...
    //
    for ( int i = 0; i < data.targetResult.length; i++ ) {
      Object resultValue = data.targetResult[ i ];
      DenormaliserTargetField field = meta.getDenormaliserTargetField()[ i ];
      switch ( field.getTargetAggregationType() ) {
        case DenormaliserTargetField.TYPE_AGGR_AVERAGE:
          long count = data.counters[ i ];
          Object sum = data.sum[ i ];
          if ( count > 0 ) {
            if ( sum instanceof Long ) {
              resultValue = (Long) sum / count;
            } else if ( sum instanceof Double ) {
              resultValue = (Double) sum / count;
            } else if ( sum instanceof BigDecimal ) {
              resultValue = ( (BigDecimal) sum ).divide( new BigDecimal( count ) );
            } else {
              resultValue = null; // TODO: perhaps throw an exception here?<
            }
          }
          break;
        case DenormaliserTargetField.TYPE_AGGR_COUNT_ALL:
          if ( resultValue == null ) {
            resultValue = Long.valueOf( 0 );
          }
          if ( field.getTargetType() != IValueMeta.TYPE_INTEGER ) {
            resultValue =
              data.outputRowMeta.getValueMeta( outputIndex ).convertData(
                new ValueMetaInteger( "num_values_aggregation" ), resultValue );
          }
          break;
        default:
          break;
      }
      if ( resultValue == null && allNullsAreZero ) {
        // PDI-9662 seems all rows for min function was nulls...
        resultValue = getZero( outputIndex );
      }
      outputRowData[ outputIndex++ ] = resultValue;
    }

    return outputRowData;
  }

  private Object getZero( int field ) throws HopValueException {
    IValueMeta vm = data.outputRowMeta.getValueMeta( field );
    return ValueDataUtil.getZeroForValueMetaType( vm );
  }

  // Is the row r of the same group as previous?
  private boolean sameGroup( IRowMeta rowMeta, Object[] previous, Object[] rowData ) throws HopValueException {
    return rowMeta.compare( previous, rowData, data.groupnrs ) == 0;
  }

  /**
   * Initialize a new group...
   *
   * @throws HopException
   */
  private void newGroup() throws HopException {
    // There is no need anymore to take care of the meta-data.
    // That is done once in DenormaliserMeta.getFields()
    //
    data.targetResult = new Object[ meta.getDenormaliserTargetFields().length ];

    DenormaliserTargetField[] fields = meta.getDenormaliserTargetField();

    for ( int i = 0; i < fields.length; i++ ) {
      data.counters[ i ] = 0L; // set to 0
      data.sum[ i ] = null;
    }
  }

  /**
   * This method de-normalizes a single key-value pair. It looks up the key and determines the value name to store it
   * in. It converts it to the right type and stores it in the result row.
   * <p>
   * Used for junits in DenormaliserAggregationsTest
   *
   * @param rowMeta
   * @param rowData
   * @throws HopValueException
   */
  void deNormalise( IRowMeta rowMeta, Object[] rowData ) throws HopValueException {
    IValueMeta valueMeta = rowMeta.getValueMeta( data.keyFieldNr );
    Object valueData = rowData[ data.keyFieldNr ];

    String key = valueMeta.getCompatibleString( valueData );
    if ( Utils.isEmpty( key ) ) {
      return;
    }
    // Get all the indexes for the given key value...
    //
    List<Integer> indexes = data.keyValue.get( key );
    if ( indexes == null ) { // otherwise we're not interested.
      return;
    }

    for ( Integer keyNr : indexes ) {
      if ( keyNr == null ) {
        continue;
      }
      // keyNr is the field in DenormaliserTargetField[]
      //
      int idx = keyNr.intValue();
      DenormaliserTargetField field = meta.getDenormaliserTargetField()[ idx ];

      // This is the value we need to de-normalise, convert, aggregate.
      //
      IValueMeta sourceMeta = rowMeta.getValueMeta( data.fieldNameIndex[ idx ] );
      Object sourceData = rowData[ data.fieldNameIndex[ idx ] ];
      Object targetData;
      // What is the target value metadata??
      //
      IValueMeta targetMeta =
        data.outputRowMeta.getValueMeta( data.inputRowMeta.size() - data.removeNrs.length + idx );
      // What was the previous target in the result row?
      //
      Object prevTargetData = data.targetResult[ idx ];

      // clone source meta as it can be used by other transforms ans set conversion meta
      // to convert date to target format
      // See PDI-4910 for details
      IValueMeta origSourceMeta = sourceMeta;
      if ( targetMeta.isDate() ) {
        sourceMeta = origSourceMeta.clone();
        sourceMeta.setConversionMetadata( getConversionMeta( field.getTargetFormat() ) );
      }

      switch ( field.getTargetAggregationType() ) {
        case DenormaliserTargetField.TYPE_AGGR_SUM:
          targetData = targetMeta.convertData( sourceMeta, sourceData );
          if ( prevTargetData != null ) {
            prevTargetData = ValueDataUtil.sum( targetMeta, prevTargetData, targetMeta, targetData );
          } else {
            prevTargetData = targetData;
          }
          break;
        case DenormaliserTargetField.TYPE_AGGR_MIN:
          if ( sourceData == null && !minNullIsValued ) {
            // PDI-9662 do not compare null
            break;
          }
          if ( ( prevTargetData == null && !minNullIsValued )
            || sourceMeta.compare( sourceData, targetMeta, prevTargetData ) < 0 ) {
            prevTargetData = targetMeta.convertData( sourceMeta, sourceData );
          }
          break;
        case DenormaliserTargetField.TYPE_AGGR_MAX:
          if ( sourceMeta.compare( sourceData, targetMeta, prevTargetData ) > 0 ) {
            prevTargetData = targetMeta.convertData( sourceMeta, sourceData );
          }
          break;
        case DenormaliserTargetField.TYPE_AGGR_COUNT_ALL:
          prevTargetData = ++data.counters[ idx ];
          break;
        case DenormaliserTargetField.TYPE_AGGR_AVERAGE:
          targetData = targetMeta.convertData( sourceMeta, sourceData );
          if ( !sourceMeta.isNull( sourceData ) ) {
            prevTargetData = data.counters[ idx ]++;
            if ( data.sum[ idx ] == null ) {
              data.sum[ idx ] = targetData;
            } else {
              data.sum[ idx ] = ValueDataUtil.plus( targetMeta, data.sum[ idx ], targetMeta, targetData );
            }
            // data.sum[idx] = (Integer)data.sum[idx] + (Integer)sourceData;
          }
          break;
        case DenormaliserTargetField.TYPE_AGGR_CONCAT_COMMA:
          String separator = ",";

          targetData = targetMeta.convertData( sourceMeta, sourceData );
          if ( prevTargetData != null ) {
            prevTargetData = prevTargetData + separator + targetData;
          } else {
            prevTargetData = targetData;
          }
          break;
        case DenormaliserTargetField.TYPE_AGGR_NONE:
        default:
          prevTargetData = targetMeta.convertData( sourceMeta, sourceData ); // Overwrite the previous
          break;
      }

      // Update the result row too
      //
      data.targetResult[ idx ] = prevTargetData;
    }
  }

  @Override
  public boolean init(){
    if ( super.init() ) {
      data.counters = new long[ meta.getDenormaliserTargetField().length ];
      data.sum = new Object[ meta.getDenormaliserTargetField().length ];

      return true;
    }
    return false;
  }

  @Override
  public void batchComplete() throws HopException {
    handleLastRow();
    data.previous = null;
  }

  /**
   * Get the metadata used for conversion to date format See related PDI-4019
   *
   * @param mask
   * @return
   */
  private IValueMeta getConversionMeta( String mask ) {
    IValueMeta meta = null;
    if ( !Utils.isEmpty( mask ) ) {
      meta = conversionMetaCache.get( mask );
      if ( meta == null ) {
        meta = new ValueMetaDate();
        meta.setConversionMask( mask );
        conversionMetaCache.put( mask, meta );
      }
    }
    return meta;
  }

  /**
   * Used for junits in DenormaliserAggregationsTest
   *
   * @param allNullsAreZero the allNullsAreZero to set
   */
  void setAllNullsAreZero( boolean allNullsAreZero ) {
    this.allNullsAreZero = allNullsAreZero;
  }

  /**
   * Used for junits in DenormaliserAggregationsTest
   *
   * @param minNullIsValued the minNullIsValued to set
   */
  void setMinNullIsValued( boolean minNullIsValued ) {
    this.minNullIsValued = minNullIsValued;
  }

}
