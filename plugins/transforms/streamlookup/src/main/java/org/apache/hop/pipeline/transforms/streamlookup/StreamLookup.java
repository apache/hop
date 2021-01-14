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

package org.apache.hop.pipeline.transforms.streamlookup;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.hash.ByteArrayHashIndex;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.util.Collections;

/**
 * Looks up information by first reading data into a hash table (in memory)
 * <p>
 * TODO: add warning with conflicting types OR modify the lookup values to the input row type. (this is harder to do as
 * currently we don't know the types)
 *
 * @author Matt
 * @since 26-apr-2003
 */
public class StreamLookup extends BaseTransform<StreamLookupMeta, StreamLookupData> implements ITransform<StreamLookupMeta, StreamLookupData> {
  private static final Class<?> PKG = StreamLookupMeta.class; // For Translator

  public StreamLookup(TransformMeta transformMeta, StreamLookupMeta meta, StreamLookupData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private void handleNullIf() {
    data.nullIf = new Object[ meta.getValue().length ];

    for ( int i = 0; i < meta.getValue().length; i++ ) {
      if ( meta.getValueDefaultType()[ i ] < 0 ) {
        //CHECKSTYLE:Indentation:OFF
        meta.getValueDefaultType()[ i ] = IValueMeta.TYPE_STRING;
      }
      data.nullIf[ i ] = null;
      switch ( meta.getValueDefaultType()[ i ] ) {
        case IValueMeta.TYPE_STRING:
          if ( Utils.isEmpty( meta.getValueDefault()[ i ] ) ) {
            data.nullIf[ i ] = null;
          } else {
            data.nullIf[ i ] = meta.getValueDefault()[ i ];
          }
          break;
        case IValueMeta.TYPE_DATE:
          try {
            data.nullIf[ i ] = DateFormat.getInstance().parse( meta.getValueDefault()[ i ] );
          } catch ( Exception e ) {
            // Ignore errors
          }
          break;
        case IValueMeta.TYPE_NUMBER:
          try {
            data.nullIf[ i ] = Double.parseDouble( meta.getValueDefault()[ i ] );
          } catch ( Exception e ) {
            // Ignore errors
          }
          break;
        case IValueMeta.TYPE_INTEGER:
          try {
            data.nullIf[ i ] = Long.parseLong( meta.getValueDefault()[ i ] );
          } catch ( Exception e ) {
            // Ignore errors
          }
          break;
        case IValueMeta.TYPE_BOOLEAN:
          if ( "TRUE".equalsIgnoreCase( meta.getValueDefault()[ i ] )
            || "Y".equalsIgnoreCase( meta.getValueDefault()[ i ] ) ) {
            data.nullIf[ i ] = Boolean.TRUE;
          } else {
            data.nullIf[ i ] = Boolean.FALSE;
          }
          break;
        case IValueMeta.TYPE_BIGNUMBER:
          try {
            data.nullIf[ i ] = new BigDecimal( meta.getValueDefault()[ i ] );
          } catch ( Exception e ) {
            // Ignore errors
          }
          break;
        default:
          // if a default value is given and no conversion is implemented throw an error
          if ( meta.getValueDefault()[ i ] != null && meta.getValueDefault()[ i ].trim().length() > 0 ) {
            throw new RuntimeException( BaseMessages.getString(
              PKG, "StreamLookup.Exception.ConversionNotImplemented" )
              + " " + ValueMetaFactory.getValueMetaName( meta.getValueDefaultType()[ i ] ) );
          } else {
            // no default value given: just set it to null
            data.nullIf[ i ] = null;
            break;
          }
      }
    }
  }

  private boolean readLookupValues() throws HopException {
    data.infoStream = meta.getTransformIOMeta().getInfoStreams().get( 0 );
    if ( data.infoStream.getTransformMeta() == null ) {
      logError( BaseMessages.getString( PKG, "StreamLookup.Log.NoLookupTransformSpecified" ) );
      return false;
    }
    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PKG, "StreamLookup.Log.ReadingFromStream" )
        + data.infoStream.getTransformName() + "]" );
    }

    int[] keyNrs = new int[ meta.getKeylookup().length ];
    int[] valueNrs = new int[ meta.getValue().length ];
    boolean firstRun = true;

    // Which row set do we read from?
    //
    IRowSet rowSet = findInputRowSet( data.infoStream.getTransformName() );
    Object[] rowData = getRowFrom( rowSet ); // rows are originating from "lookup_from"
    while ( rowData != null ) {
      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "StreamLookup.Log.ReadLookupRow" )
          + rowSet.getRowMeta().getString( rowData ) );
      }

      if ( firstRun ) {
        firstRun = false;
        data.hasLookupRows = true;

        data.infoMeta = rowSet.getRowMeta().clone();
        IRowMeta cacheKeyMeta = new RowMeta();
        IRowMeta cacheValueMeta = new RowMeta();

        // Look up the keys in the source rows
        for ( int i = 0; i < meta.getKeylookup().length; i++ ) {
          keyNrs[ i ] = rowSet.getRowMeta().indexOfValue( meta.getKeylookup()[ i ] );
          if ( keyNrs[ i ] < 0 ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "StreamLookup.Exception.UnableToFindField", meta.getKeylookup()[ i ] ) );
          }
          cacheKeyMeta.addValueMeta( rowSet.getRowMeta().getValueMeta( keyNrs[ i ] ) );
        }
        // Save the data types of the keys to optionally convert input rows later on...
        if ( data.keyTypes == null ) {
          data.keyTypes = cacheKeyMeta.clone();
        }

        // ICache keys are stored as normal types, not binary
        for ( int i = 0; i < keyNrs.length; i++ ) {
          cacheKeyMeta.getValueMeta( i ).setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
        }

        for ( int v = 0; v < meta.getValue().length; v++ ) {
          valueNrs[ v ] = rowSet.getRowMeta().indexOfValue( meta.getValue()[ v ] );
          if ( valueNrs[ v ] < 0 ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "StreamLookup.Exception.UnableToFindField", meta.getValue()[ v ] ) );
          }
          cacheValueMeta.addValueMeta( rowSet.getRowMeta().getValueMeta( valueNrs[ v ] ) );
        }

        data.cacheKeyMeta = cacheKeyMeta;
        data.cacheValueMeta = cacheValueMeta;
      }

      Object[] keyData = new Object[ keyNrs.length ];
      for ( int i = 0; i < keyNrs.length; i++ ) {
        IValueMeta keyMeta = data.keyTypes.getValueMeta( i );
        // Convert keys to normal storage type
        keyData[ i ] = keyMeta.convertToNormalStorageType( rowData[ keyNrs[ i ] ] );
      }

      Object[] valueData = new Object[ valueNrs.length ];
      for ( int i = 0; i < valueNrs.length; i++ ) {
        // Store value as is, avoid preliminary binary->normal storage type conversion
        valueData[ i ] = rowData[ valueNrs[ i ] ];
      }

      addToCache( data.cacheKeyMeta, keyData, data.cacheValueMeta, valueData );

      rowData = getRowFrom( rowSet );
    }

    return true;
  }

  private Object[] lookupValues( IRowMeta rowMeta, Object[] row ) throws HopException {
    // See if we need to stop.
    if ( isStopped() ) {
      return null;
    }

    if ( data.lookupColumnIndex == null ) {
      String[] names = data.lookupMeta.getFieldNames();
      data.lookupColumnIndex = new int[ names.length ];

      for ( int i = 0; i < names.length; i++ ) {
        data.lookupColumnIndex[ i ] = rowMeta.indexOfValue( names[ i ] );
        if ( data.lookupColumnIndex[ i ] < 0 ) {
          // we should not get here
          throw new HopTransformException( "The lookup column '" + names[ i ] + "' could not be found" );
        }
      }
    }

    // Copy value references to lookup table.
    //
    Object[] lu = new Object[ data.keynrs.length ];
    for ( int i = 0; i < data.keynrs.length; i++ ) {
      // If the input is binary storage data, we convert it to normal storage.
      //
      if ( data.convertKeysToNative[ i ] ) {
        lu[ i ] = data.lookupMeta.getValueMeta( i ).convertBinaryStringToNativeType( (byte[]) row[ data.keynrs[ i ] ] );
      } else {
        lu[ i ] = row[ data.keynrs[ i ] ];
      }
    }

    // Handle conflicting types (Number-Integer-String conversion to lookup type in hashtable)
    if ( data.keyTypes != null ) {
      for ( int i = 0; i < data.lookupMeta.size(); i++ ) {
        IValueMeta inputValue = data.lookupMeta.getValueMeta( i );
        IValueMeta lookupValue = data.keyTypes.getValueMeta( i );
        if ( inputValue.getType() != lookupValue.getType() ) {
          try {
            // Change the input value to match the lookup value
            //
            lu[ i ] = lookupValue.convertDataCompatible( inputValue, lu[ i ] );
          } catch ( HopValueException e ) {
            throw new HopTransformException( "Error converting data while looking up value", e );
          }
        }
      }
    }

    Object[] add = null;

    if ( data.hasLookupRows ) {
      try {
        if ( meta.getKeystream().length > 0 ) {
          add = getFromCache( data.cacheKeyMeta, lu );
        } else {
          // Just take the first element in the hashtable...
          throw new HopTransformException( BaseMessages.getString( PKG, "StreamLookup.Log.GotRowWithoutKeys" ) );
        }
      } catch ( Exception e ) {
        throw new HopTransformException( e );
      }
    }

    if ( add == null ) { // nothing was found, unknown code: add the specified default value...
      add = data.nullIf;
    }

    return RowDataUtil.addRowData( row, rowMeta.size(), add );
  }

  private void addToCache( IRowMeta keyMeta, Object[] keyData, IRowMeta valueMeta,
                           Object[] valueData ) throws HopValueException {
    if ( meta.isMemoryPreservationActive() ) {
      if ( meta.isUsingSortedList() ) {
        KeyValue keyValue = new KeyValue( keyData, valueData );
        int idx = Collections.binarySearch( data.list, keyValue, data.comparator );
        if ( idx < 0 ) {
          int index = -idx - 1; // this is the insertion point
          data.list.add( index, keyValue ); // insert to keep sorted.
        } else {
          data.list.set( idx, keyValue ); // Overwrite to simulate Hashtable behaviour
        }
      } else {
        if ( meta.isUsingIntegerPair() ) {
          if ( !data.metadataVerifiedIntegerPair ) {
            data.metadataVerifiedIntegerPair = true;
            if ( keyMeta.size() != 1
              || valueMeta.size() != 1 || !keyMeta.getValueMeta( 0 ).isInteger()
              || !valueMeta.getValueMeta( 0 ).isInteger() ) {

              throw new HopValueException( BaseMessages.getString(
                PKG, "StreamLookup.Exception.CanNotUseIntegerPairAlgorithm" ) );
            }
          }

          Long key = keyMeta.getInteger( keyData, 0 );
          Long value = valueMeta.getInteger( valueData, 0 );
          data.longIndex.put( key, value );
        } else {
          if ( data.hashIndex == null ) {
            data.hashIndex = new ByteArrayHashIndex( keyMeta );
          }
          data.hashIndex
            .put( RowMeta.extractData( keyMeta, keyData ), RowMeta.extractData( valueMeta, valueData ) );
        }
      }
    } else {
      // We can't just put Object[] in the map The compare function is not in it.
      // We need to wrap in and use that. Let's use RowMetaAndData for this one.
      data.look.put( new RowMetaAndData( keyMeta, keyData ), valueData );
    }
  }

  private Object[] getFromCache( IRowMeta keyMeta, Object[] keyData ) throws HopValueException {
    if ( meta.isMemoryPreservationActive() ) {
      if ( meta.isUsingSortedList() ) {
        KeyValue keyValue = new KeyValue( keyData, null );
        int idx = Collections.binarySearch( data.list, keyValue, data.comparator );
        if ( idx < 0 ) {
          return null; // nothing found
        }

        keyValue = data.list.get( idx );
        return keyValue.getValue();
      } else {
        if ( meta.isUsingIntegerPair() ) {
          Long value = data.longIndex.get( keyMeta.getInteger( keyData, 0 ) );
          if ( value == null ) {
            return null;
          }
          return new Object[] { value, };
        } else {
          try {
            byte[] value = data.hashIndex.get( RowMeta.extractData( keyMeta, keyData ) );
            if ( value == null ) {
              return null;
            }
            return RowMeta.getRow( data.cacheValueMeta, value );
          } catch ( Exception e ) {
            logError( "Oops", e );
            throw new RuntimeException( e );
          }
        }
      }
    } else {
      return data.look.get( new RowMetaAndData( keyMeta, keyData ) );
    }
  }

  @Override
  public boolean processRow() throws HopException {

    if ( data.readLookupValues ) {
      data.readLookupValues = false;

      if ( !readLookupValues() ) {
        // Read values in lookup table (look)
        logError( BaseMessages.getString( PKG, "StreamLookup.Log.UnableToReadDataFromLookupStream" ) );
        setErrors( 1 );
        stopAll();
        return false;
      }

      return true;
    }

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) {
      // no more input to be expected...

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "StreamLookup.Log.StoppedProcessingWithEmpty", getLinesRead()
          + "" ) );
      }
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // read the lookup values!
      data.keynrs = new int[ meta.getKeystream().length ];
      data.lookupMeta = new RowMeta();
      data.convertKeysToNative = new boolean[ meta.getKeystream().length ];

      for ( int i = 0; i < meta.getKeystream().length; i++ ) {
        // Find the keynr in the row (only once)
        data.keynrs[ i ] = getInputRowMeta().indexOfValue( meta.getKeystream()[ i ] );
        if ( data.keynrs[ i ] < 0 ) {
          throw new HopTransformException(
            BaseMessages
              .getString(
                PKG,
                "StreamLookup.Log.FieldNotFound", meta.getKeystream()[ i ], "" + getInputRowMeta().getString( r ) ) );
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "StreamLookup.Log.FieldInfo", meta.getKeystream()[ i ], "" + data.keynrs[ i ] ) );
          }
        }

        data.lookupMeta.addValueMeta( getInputRowMeta().getValueMeta( data.keynrs[ i ] ).clone() );

        // If we have binary storage data coming in, we convert it to normal data storage.
        // The storage in the lookup data store is also normal data storage. TODO: enforce normal data storage??
        //
        data.convertKeysToNative[ i ] = getInputRowMeta().getValueMeta( data.keynrs[ i ] ).isStorageBinaryString();
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), new IRowMeta[] { data.infoMeta }, null, this, metadataProvider );

      // Handle the NULL values (not found...)
      handleNullIf();
    }

    Object[] outputRow = lookupValues( getInputRowMeta(), r ); // Do the actual lookup in the hastable.
    if ( outputRow == null ) {
      setOutputDone(); // signal end to receiver(s)

      return false;
    }

    putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "StreamLookup.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  @Override
  public boolean init() {

    if ( super.init() ) {
      data.readLookupValues = true;

      return true;
    }

    return false;
  }

  @Override
  public void dispose() {
    // Recover memory immediately, allow in-memory data to be garbage collected
    //
    data.look = null;
    data.list = null;
    data.hashIndex = null;
    data.longIndex = null;

    super.dispose();
  }
}
