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

package org.apache.hop.core.row;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RowMeta implements IRowMeta {
  public static final String XML_META_TAG = "row-meta";
  public static final String XML_DATA_TAG = "row-data";

  private final ReentrantReadWriteLock lock;
  private final RowMetaCache cache;
  List<IValueMeta> valueMetaList;
  List<Integer> needRealClone;

  public RowMeta() {
    this( new ArrayList<>(), new RowMetaCache() );
  }

  /**
   * Copy constructor for clone
   *
   * @param rowMeta
   * @throws HopPluginException
   */
  private RowMeta( RowMeta rowMeta, Integer targetType ) throws HopPluginException {
    this( new ArrayList<>( rowMeta.valueMetaList.size() ), new RowMetaCache( rowMeta.cache ) );
    for ( IValueMeta iValueMeta : rowMeta.valueMetaList ) {
      valueMetaList.add( ValueMetaFactory
        .cloneValueMeta( iValueMeta, targetType == null ? iValueMeta.getType() : targetType ) );
    }
    this.needRealClone = rowMeta.needRealClone;
  }

  private RowMeta( List<IValueMeta> valueMetaList, RowMetaCache rowMetaCache ) {
    lock = new ReentrantReadWriteLock();
    this.cache = rowMetaCache;
    this.valueMetaList = valueMetaList;
    this.needRealClone = new ArrayList<>();
  }

  @Override
  public RowMeta clone() {
    lock.readLock().lock();
    try {
      return new RowMeta( this, null );
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * This method copies the row metadata and sets all values to the specified type (usually String)
   *
   * @param targetType The target type
   * @return The cloned metadata
   * @throws if the target type could not be loaded from the plugin registry
   */
  @Override
  public IRowMeta cloneToType( int targetType ) throws HopValueException {
    lock.readLock().lock();
    try {
      return new RowMeta( this, targetType );
    } catch ( HopPluginException e ) {
      throw new HopValueException( e );
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    lock.readLock().lock();
    try {
      boolean notFirst = false;
      for ( IValueMeta valueMeta : valueMetaList ) {
        if ( notFirst ) {
          buffer.append( ", " );
        } else {
          notFirst = true;
        }
        buffer.append( "[" ).append( valueMeta.toString() ).append( "]" );
      }
      return buffer.toString();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return the list of value metadata
   */
  @Override
  public List<IValueMeta> getValueMetaList() {
    List<IValueMeta> copy;

    lock.readLock().lock();
    try {
      copy = new ArrayList<>( valueMetaList );
    } finally {
      lock.readLock().unlock();
    }
    // kept for backward compatibility
    return Collections.unmodifiableList( copy );
  }

  /**
   * @param valueMetaList the list of valueMeta to set
   */
  @Override
  public void setValueMetaList( List<IValueMeta> valueMetaList ) {
    lock.writeLock().lock();
    try {
      this.valueMetaList = valueMetaList;
      this.cache.invalidate();
      for ( int i = 0, len = valueMetaList.size(); i < len; i++ ) {
        IValueMeta valueMeta = valueMetaList.get( i );
        cache.storeMapping( valueMeta.getName(), i );
      }
      this.needRealClone = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @return the number of values in the row
   */
  @Override
  public int size() {
    lock.readLock().lock();
    try {
      return valueMetaList.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return true if there are no elements in the row metadata
   */
  @Override
  public boolean isEmpty() {
    lock.readLock().lock();
    try {
      return valueMetaList.isEmpty();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean exists( IValueMeta meta ) {
    return ( meta != null ) && searchValueMeta( meta.getName() ) != null;
  }

  /**
   * Add a metadata value. If a value with the same name already exists, it gets renamed.
   *
   * @param meta The metadata value to add
   */
  @Override
  public void addValueMeta( IValueMeta meta ) {
    if ( meta != null ) {
      lock.writeLock().lock();
      try {
        IValueMeta newMeta;
        Integer existsIdx = cache.findAndCompare( meta.getName(), valueMetaList );
        if ( existsIdx == null ) {
          newMeta = meta;
        } else {
          newMeta = renameValueMetaIfInRow( meta, null );
        }
        int sz = valueMetaList.size();
        valueMetaList.add( newMeta );
        cache.storeMapping( newMeta.getName(), sz );
        needRealClone = null;
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  /**
   * Add a metadata value on a certain location in the row. If a value with the same name already exists, it gets
   * renamed. Remember to change the data row according to this.
   *
   * @param index The index where the metadata value needs to be put in the row
   * @param meta  The metadata value to add to the row
   */
  @Override
  public void addValueMeta( int index, IValueMeta meta ) {
    if ( meta != null ) {
      lock.writeLock().lock();
      try {
        IValueMeta newMeta;
        Integer existsIdx = cache.findAndCompare( meta.getName(), valueMetaList );
        if ( existsIdx == null ) {
          newMeta = meta;
        } else {
          newMeta = renameValueMetaIfInRow( meta, null );
        }
        valueMetaList.add( index, newMeta );
        cache.invalidate();
        needRealClone = null;
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  /**
   * Get the value metadata on the specified index.
   *
   * @param index The index to get the value metadata from
   * @return The value metadata specified by the index.
   */
  @Override
  public IValueMeta getValueMeta( int index ) {
    lock.readLock().lock();
    try {
      if ( ( index >= 0 ) && ( index < valueMetaList.size() ) ) {
        return valueMetaList.get( index );
      } else {
        return null;
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Replaces a value meta entry in the row metadata with another one
   *
   * @param index     The index in the row to replace at
   * @param valueMeta the metadata to replace with
   */
  @Override
  public void setValueMeta( int index, IValueMeta valueMeta ) {
    if ( valueMeta != null ) {
      lock.writeLock().lock();
      try {
        IValueMeta old = valueMetaList.get( index );
        IValueMeta newMeta = valueMeta;

        // try to check if a ValueMeta with the same name already exists
        int existsIndex = indexOfValue( valueMeta.getName() );
        // if it exists and it's not in the requested position
        // we need to take care of renaming
        if ( existsIndex >= 0 && existsIndex != index ) {
          newMeta = renameValueMetaIfInRow( valueMeta, null );
        }
        valueMetaList.set( index, newMeta );
        cache.replaceMapping( old.getName(), newMeta.getName(), index );
        needRealClone = null;
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  /**
   * Get a String value from a row of data. Convert data if this needed.
   *
   * @param dataRow the row of data
   * @param index   the index
   * @return The string found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  @Override
  public String getString( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      return null;
    }
    IValueMeta meta = getValueMeta( index );
    return meta.getString( dataRow[ index ] );
  }

  /**
   * Get an Integer value from a row of data. Convert data if this needed.
   *
   * @param dataRow the row of data
   * @param index   the index
   * @return The integer found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  @Override
  public Long getInteger( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      return null;
    }
    IValueMeta meta = getValueMeta( index );
    return meta.getInteger( dataRow[ index ] );
  }

  /**
   * Get a Number value from a row of data. Convert data if this needed.
   *
   * @param dataRow the row of data
   * @param index   the index
   * @return The number found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  @Override
  public Double getNumber( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      return null;
    }
    IValueMeta meta = getValueMeta( index );
    return meta.getNumber( dataRow[ index ] );
  }

  /**
   * Get a Date value from a row of data. Convert data if this needed.
   *
   * @param dataRow the row of data
   * @param index   the index
   * @return The date found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  @Override
  public Date getDate( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      return null;
    }
    IValueMeta meta = getValueMeta( index );
    return meta.getDate( dataRow[ index ] );
  }

  /**
   * Get a BigNumber value from a row of data. Convert data if this needed.
   *
   * @param dataRow the row of data
   * @param index   the index
   * @return The bignumber found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  @Override
  public BigDecimal getBigNumber( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      return null;
    }
    IValueMeta meta = getValueMeta( index );
    return meta.getBigNumber( dataRow[ index ] );
  }

  /**
   * Get a Boolean value from a row of data. Convert data if this needed.
   *
   * @param dataRow the row of data
   * @param index   the index
   * @return The boolean found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  @Override
  public Boolean getBoolean( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      return null;
    }
    IValueMeta meta = getValueMeta( index );
    return meta.getBoolean( dataRow[ index ] );
  }

  /**
   * Get a Binary value from a row of data. Convert data if this needed.
   *
   * @param dataRow the row of data
   * @param index   the index
   * @return The binary found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  @Override
  public byte[] getBinary( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      return null;
    }
    IValueMeta meta = getValueMeta( index );
    return meta.getBinary( dataRow[ index ] );
  }

  /**
   * Determines whether a value in a row is null. A value is null when the object is null or when it's an empty String
   *
   * @param dataRow The row of data
   * @param index   the index to reference
   * @return true if the value on the index is null.
   * @throws HopValueException in case there is a conversion error (only thrown in case of lazy conversion)
   */
  @Override
  public boolean isNull( Object[] dataRow, int index ) throws HopValueException {
    if ( dataRow == null ) {
      // I guess so...
      return true;
    }
    return getValueMeta( index ).isNull( dataRow[ index ] );
  }

  /**
   * @return a cloned Object[] object.
   * @throws HopValueException in case something is not quite right with the expected data
   */
  @Override
  public Object[] cloneRow( Object[] objects ) throws HopValueException {
    return cloneRow( objects, objects.clone() );
  }

  /**
   * @return a cloned Object[] object.
   * @throws HopValueException in case something is not quite right with the expected data
   */
  @Override
  public Object[] cloneRow( Object[] objects, Object[] newObjects ) throws HopValueException {
    List<Integer> list = getOrCreateValuesThatNeedRealClone( valueMetaList );
    lock.readLock().lock();
    try {
      for ( Integer i : list ) {
        IValueMeta valueMeta = valueMetaList.get( i );
        newObjects[ i ] = valueMeta.cloneValueData( objects[ i ] );
      }
      return newObjects;
    } finally {
      lock.readLock().unlock();
    }
  }

  @VisibleForTesting
  List<Integer> getOrCreateValuesThatNeedRealClone( List<IValueMeta> values ) {
    lock.writeLock().lock();
    try {
      if ( needRealClone == null ) {
        int len = values.size();
        needRealClone = new ArrayList<>( len );
        for ( int i = 0; i < len; i++ ) {
          IValueMeta valueMeta = values.get( i );
          if ( valueMeta.requiresRealClone() ) {
            needRealClone.add( i );
          }
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
    return needRealClone;
  }

  @Override
  public String getString( Object[] dataRow, String valueName, String defaultValue ) throws HopValueException {
    int index = indexOfValue( valueName );
    if ( index < 0 ) {
      return defaultValue;
    }
    return getString( dataRow, index );
  }

  @Override
  public Long getInteger( Object[] dataRow, String valueName, Long defaultValue ) throws HopValueException {
    int index = indexOfValue( valueName );
    if ( index < 0 ) {
      return defaultValue;
    }
    return getInteger( dataRow, index );
  }

  @Override
  public Date getDate( Object[] dataRow, String valueName, Date defaultValue ) throws HopValueException {
    int index = indexOfValue( valueName );
    if ( index < 0 ) {
      return defaultValue;
    }
    return getDate( dataRow, index );
  }

  /**
   * Searches the index of a value meta with a given name
   *
   * @param valueName the name of the value metadata to look for
   * @return the index or -1 in case we didn't find the value
   */
  @Override
  public int indexOfValue( String valueName ) {
    if ( valueName == null ) {
      return -1;
    }

    lock.readLock().lock();
    try {
      Integer index = cache.findAndCompare( valueName, valueMetaList );
      for ( int i = 0; ( index == null ) && ( i < valueMetaList.size() ); i++ ) {
        if ( valueName.equalsIgnoreCase( valueMetaList.get( i ).getName() ) ) {
          index = i;
          // it is possible, that several threads can call storing simultaneously
          // but it makes no harm as they will put the same value,
          // because valueMetaList is defended from modifications by read lock
          cache.storeMapping( valueName, index );
          needRealClone = null;
        }
      }
      if ( index == null ) {
        return -1;
      }
      return index;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Searches for a value with a certain name in the value meta list
   *
   * @param valueName The value name to search for
   * @return The value metadata or null if nothing was found
   */
  @Override
  public IValueMeta searchValueMeta( String valueName ) {
    lock.readLock().lock();
    try {
      Integer index = indexOfValue( valueName );
      if ( index < 0 ) {
        return null;
      }
      return valueMetaList.get( index );
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void addRowMeta( IRowMeta rowMeta ) {
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      addValueMeta( rowMeta.getValueMeta( i ) );
    }
  }

  /**
   * Merge the values of row r to this Row. The values that are not yet in the row are added unchanged. The values that
   * are in the row are renamed to name_2, name_3, etc.
   *
   * @param r The row to be merged with this row
   */
  @Override
  public void mergeRowMeta( IRowMeta r ) {
    mergeRowMeta( r, null );
  }

  /**
   * Merge the values of row r to this Row. The fields that are not yet in the row are added unchanged. The fields that
   * are in the row are renamed to name_2, name_3, etc. If the fields are renamed, the provided originTransformName will be
   * assigned as the origin transform for those fields.
   *
   * @param r              The row to be merged with this row
   * @param originTransformName The name to use as the origin transform
   */
  @Override
  public void mergeRowMeta( IRowMeta r, String originTransformName ) {
    lock.writeLock().lock();
    try {
      for ( int x = 0; x < r.size(); x++ ) {
        IValueMeta field = r.getValueMeta( x );
        if ( searchValueMeta( field.getName() ) == null ) {
          addValueMeta( field ); // Not in list yet: add
        } else {
          // We want to rename the field to Name[2], Name[3], ...
          //
          addValueMeta( renameValueMetaIfInRow( field, originTransformName ) );
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private IValueMeta renameValueMetaIfInRow( IValueMeta valueMeta, String originTransform ) {
    // We want to rename the field to Name[2], Name[3], ...
    //
    int index = 1;
    String name = valueMeta.getName() + "_" + index;
    while ( searchValueMeta( name ) != null ) {
      index++;
      name = valueMeta.getName() + "_" + index;
    }

    // Create a copy of the valueMeta object to make sure we don't rename any other value meta objects.
    // It's only being renamed because of the addition to THIS row metadata object, not another.
    //
    IValueMeta copy = valueMeta.clone();

    // OK, this is the new name and origin to pick
    //
    copy.setName( name );
    if ( originTransform != null ) {
      copy.setOrigin( originTransform );
    }
    return copy;
  }

  /**
   * Get an array of the names of all the Values in the Row.
   *
   * @return an array of Strings: the names of all the Values in the Row.
   */
  @Override
  public String[] getFieldNames() {
    lock.readLock().lock();
    try {
      String[] retval = new String[ size() ];

      for ( int i = 0; i < size(); i++ ) {
        String valueName = getValueMeta( i ).getName();
        retval[ i ] = valueName == null ? "" : valueName;
      }

      return retval;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Write ONLY the specified data to the outputStream
   *
   * @throws HopFileException in case things go awry
   */
  @Override
  public void writeData( DataOutputStream outputStream, Object[] data ) throws HopFileException {
    lock.readLock().lock();
    try {
      // Write all values in the row
      for ( int i = 0; i < size(); i++ ) {
        getValueMeta( i ).writeData( outputStream, data[ i ] );
      }

      // If there are 0 values in the row, we write a marker flag to be able to detect an EOF on the other end (sockets
      // etc)
      //
      if ( size() == 0 ) {
        try {
          outputStream.writeBoolean( true );
        } catch ( IOException e ) {
          throw new HopFileException( "Error writing marker flag", e );
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Write ONLY the specified metadata to the outputStream
   *
   * @throws HopFileException in case things go awry
   */
  @Override
  public void writeMeta( DataOutputStream outputStream ) throws HopFileException {
    lock.readLock().lock();
    try {
      // First handle the number of fields in a row
      try {
        outputStream.writeInt( size() );
      } catch ( IOException e ) {
        throw new HopFileException( "Unable to write nr of metadata values", e );
      }

      // Write all values in the row
      for ( int i = 0; i < size(); i++ ) {
        getValueMeta( i ).writeMeta( outputStream );
      }
    } finally {
      lock.readLock().unlock();
    }

  }

  public RowMeta( DataInputStream inputStream ) throws HopFileException, SocketTimeoutException {
    this();

    int nr;
    try {
      nr = inputStream.readInt();
    } catch ( SocketTimeoutException e ) {
      throw e;
    } catch ( EOFException e ) {
      throw new HopEofException(
        "End of file while reading the number of metadata values in the row metadata", e );
    } catch ( IOException e ) {
      throw new HopFileException( "Unable to read nr of metadata values: " + e.toString(), e );
    }

    for ( int i = 0; i < nr; i++ ) {
      try {
        int type = inputStream.readInt();
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta( type );
        valueMeta.readMetaData( inputStream );
        addValueMeta( valueMeta );
      } catch ( EOFException e ) {
        throw new HopEofException( e );
      } catch ( Exception e ) {
        throw new HopFileException( toString() + " : Unable to read row metadata from input stream", e );
      }

    }
  }

  @Override
  public Object[] readData( DataInputStream inputStream ) throws HopFileException, SocketTimeoutException {
    lock.readLock().lock();
    try {
      Object[] data = new Object[ size() ];
      for ( int i = 0; i < size(); i++ ) {
        data[ i ] = getValueMeta( i ).readData( inputStream );
      }
      if ( size() == 0 ) {
        try {
          inputStream.readBoolean();
        } catch ( EOFException e ) {
          throw new HopEofException( e );
        } catch ( SocketTimeoutException e ) {
          throw e;
        } catch ( IOException e ) {
          throw new HopFileException( toString() + " : Unable to read the marker flag data from input stream", e );
        }

      }
      return data;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      valueMetaList.clear();
      cache.invalidate();
      needRealClone = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeValueMeta( String valueName ) throws HopValueException {
    lock.writeLock().lock();
    try {
      int index = indexOfValue( valueName );
      if ( index < 0 ) {
        throw new HopValueException( "Unable to find value metadata with name '"
          + valueName + "', so I can't delete it." );
      }
      removeValueMeta( index );
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeValueMeta( int index ) {
    lock.writeLock().lock();
    try {
      valueMetaList.remove( index );
      cache.invalidate();
      needRealClone = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @return a string with a description of all the metadata values of the complete row of metadata
   */
  @Override
  public String toStringMeta() {
    StringBuilder buffer = new StringBuilder();
    lock.readLock().lock();
    try {
      boolean notFirst = false;
      for ( IValueMeta valueMeta : valueMetaList ) {
        if ( notFirst ) {
          buffer.append( ", " );
        } else {
          notFirst = true;
        }
        buffer.append( "[" ).append( valueMeta.toStringMeta() ).append( "]" );
      }
      return buffer.toString();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the string representation of the data in a row of data
   *
   * @param row the row of data to convert to string
   * @return the row of data in string form
   * @throws HopValueException in case of a conversion error
   */
  @Override
  public String getString( Object[] row ) throws HopValueException {
    lock.readLock().lock();
    try {
      StringBuilder buffer = new StringBuilder();
      for ( int i = 0; i < size(); i++ ) {
        if ( i > 0 ) {
          buffer.append( ", " );
        }
        buffer.append( "[" );
        buffer.append( getString( row, i ) );
        buffer.append( "]" );
      }
      return buffer.toString();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get an array of strings showing the name of the values in the row padded to a maximum length, followed by the types
   * of the values.
   *
   * @param maxlen The length to which the name will be padded.
   * @return an array of strings: the names and the types of the fieldnames in the row.
   */
  @Override
  public String[] getFieldNamesAndTypes( int maxlen ) {
    lock.readLock().lock();
    try {
      final int size = size();
      String[] retval = new String[ size ];

      for ( int i = 0; i < size; i++ ) {
        IValueMeta v = getValueMeta( i );
        retval[ i ] = Const.rightPad( v.getName(), maxlen ) + "   (" + v.getTypeDesc() + ")";
      }

      return retval;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Compare 2 rows with each other using certain values in the rows and also considering the specified ascending
   * clauses of the value metadata.
   *
   * @param rowData1 The first row of data
   * @param rowData2 The second row of data
   * @param fieldnrs the fields to compare on (in that order)
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException
   */
  @Override
  public int compare( Object[] rowData1, Object[] rowData2, int[] fieldnrs ) throws HopValueException {
    lock.readLock().lock();
    try {
      for ( int fieldnr : fieldnrs ) {
        IValueMeta valueMeta = getValueMeta( fieldnr );

        int cmp = valueMeta.compare( rowData1[ fieldnr ], rowData2[ fieldnr ] );
        if ( cmp != 0 ) {
          return cmp;
        }
      }

      return 0;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Compare 2 rows with each other for equality using certain values in the rows and also considering the case
   * sensitivity flag.
   *
   * @param rowData1 The first row of data
   * @param rowData2 The second row of data
   * @param fieldnrs the fields to compare on (in that order)
   * @return true if the rows are considered equal, false if they are not.
   * @throws HopValueException
   */
  @Override
  public boolean equals( Object[] rowData1, Object[] rowData2, int[] fieldnrs ) throws HopValueException {
    lock.readLock().lock();
    try {
      for ( int fieldnr : fieldnrs ) {
        IValueMeta valueMeta = getValueMeta( fieldnr );

        int cmp = valueMeta.compare( rowData1[ fieldnr ], rowData2[ fieldnr ] );
        if ( cmp != 0 ) {
          return false;
        }
      }

      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Compare 2 rows with each other using certain values in the rows and also considering the specified ascending
   * clauses of the value metadata.
   *
   * @param rowData1  The first row of data
   * @param rowData2  The second row of data
   * @param fieldnrs1 The indexes of the values to compare in the first row
   * @param fieldnrs2 The indexes of the values to compare with in the second row
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException
   */
  @Override
  public int compare( Object[] rowData1, Object[] rowData2, int[] fieldnrs1, int[] fieldnrs2 )
    throws HopValueException {
    int len = ( fieldnrs1.length < fieldnrs2.length ) ? fieldnrs1.length : fieldnrs2.length;
    lock.readLock().lock();
    try {
      for ( int i = 0; i < len; i++ ) {
        IValueMeta valueMeta = getValueMeta( fieldnrs1[ i ] );

        int cmp = valueMeta.compare( rowData1[ fieldnrs1[ i ] ], rowData2[ fieldnrs2[ i ] ] );
        if ( cmp != 0 ) {
          return cmp;
        }
      }

      return 0;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Compare 2 rows with each other using certain values in the rows and also considering the specified ascending
   * clauses of the value metadata.
   *
   * @param rowData1  The first row of data
   * @param rowMeta2  the metadata of the second row of data
   * @param rowData2  The second row of data
   * @param fieldnrs1 The indexes of the values to compare in the first row
   * @param fieldnrs2 The indexes of the values to compare with in the second row
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException
   */
  @Override
  public int compare( Object[] rowData1, IRowMeta rowMeta2, Object[] rowData2, int[] fieldnrs1,
                      int[] fieldnrs2 ) throws HopValueException {
    int len = ( fieldnrs1.length < fieldnrs2.length ) ? fieldnrs1.length : fieldnrs2.length;
    lock.readLock().lock();
    try {
      for ( int i = 0; i < len; i++ ) {
        IValueMeta valueMeta1 = getValueMeta( fieldnrs1[ i ] );
        IValueMeta valueMeta2 = rowMeta2.getValueMeta( fieldnrs2[ i ] );

        int cmp = valueMeta1.compare( rowData1[ fieldnrs1[ i ] ], valueMeta2, rowData2[ fieldnrs2[ i ] ] );
        if ( cmp != 0 ) {
          return cmp;
        }
      }

      return 0;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Compare 2 rows with each other using all values in the rows and also considering the specified ascending clauses of
   * the value metadata.
   *
   * @param rowData1 The first row of data
   * @param rowData2 The second row of data
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException
   */
  @Override
  public int compare( Object[] rowData1, Object[] rowData2 ) throws HopValueException {
    lock.readLock().lock();
    try {
      for ( int i = 0; i < size(); i++ ) {
        IValueMeta valueMeta = getValueMeta( i );

        int cmp = valueMeta.compare( rowData1[ i ], rowData2[ i ] );
        if ( cmp != 0 ) {
          return cmp;
        }
      }

      return 0;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Calculate a hashCode of the content (not the index) of the data specified NOTE: This method uses a simple XOR of
   * the individual hashCodes which can result in a lot of collisions for similar types of data (e.g. [A,B] == [B,A] and
   * is not suitable for normal use. It is kept to provide backward compatibility with CombinationLookup.lookupValues()
   *
   * @param rowData The data to calculate a hashCode with
   * @return the calculated hashCode
   * @throws HopValueException in case there is a data conversion error
   * @deprecated
   */
  @Override
  @Deprecated
  public int oldXORHashCode( Object[] rowData ) throws HopValueException {
    int hash = 0;
    lock.readLock().lock();
    try {
      for ( int i = 0; i < size(); i++ ) {
        IValueMeta valueMeta = getValueMeta( i );
        hash ^= valueMeta.hashCode( rowData[ i ] );
      }

      return hash;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Calculates a simple hashCode of all the native data objects in the supplied row. This method will return a better
   * distribution of values for rows of numbers or rows with the same values in different positions. NOTE: This method
   * performs against the native values, not the values returned by ValueMeta. This means that if you have two rows with
   * different primitive values ['2008-01-01:12:30'] and ['2008-01-01:00:00'] that use a format object to change the
   * value (as Date yyyy-MM-dd), the hashCodes will be different resulting in the two rows not being considered equal
   * via the hashCode even though compare() or equals() might consider them to be.
   *
   * @param rowData The data to calculate a hashCode with
   * @return the calculated hashCode
   * @throws HopValueException in case there is a data conversion error
   */
  @Override
  public int hashCode( Object[] rowData ) throws HopValueException {
    return Arrays.deepHashCode( rowData );
  }

  /**
   * Calculates a hashcode of the converted value of all objects in the supplied row. This method returns distinct
   * values for nulls of different data types and will return the same hashCode for different native values that have a
   * ValueMeta converting them into the same value (e.g. ['2008-01-01:12:30'] and ['2008-01-01:00:00'] as Date
   * yyyy-MM-dd)
   *
   * @param rowData The data to calculate a hashCode with
   * @return the calculated hashCode
   * @throws HopValueException in case there is a data conversion error
   */
  @Override
  public int convertedValuesHashCode( Object[] rowData ) throws HopValueException {
    if ( rowData == null ) {
      return 0;
    }

    int result = 1;
    lock.readLock().lock();
    try {
      for ( int i = 0; i < rowData.length; i++ ) {
        result = 31 * result + getValueMeta( i ).hashCode();
      }
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Serialize a row of data to byte[]
   *
   * @param metadata the metadata to use
   * @param row      the row of data
   * @return a serialized form of the data as a byte array
   */
  public static byte[] extractData( IRowMeta metadata, Object[] row ) {
    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream( byteArrayOutputStream );
      metadata.writeData( dataOutputStream, row );
      dataOutputStream.close();
      byteArrayOutputStream.close();
      return byteArrayOutputStream.toByteArray();
    } catch ( Exception e ) {
      throw new RuntimeException( "Error serializing row to byte array", e );
    }
  }

  /**
   * Create a row of data bases on a serialized format (byte[])
   *
   * @param data     the serialized data
   * @param metadata the metadata to use
   * @return a new row of data
   */
  public static Object[] getRow( IRowMeta metadata, byte[] data ) {
    try {
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( data );
      DataInputStream dataInputStream = new DataInputStream( byteArrayInputStream );
      return metadata.readData( dataInputStream );
    } catch ( Exception e ) {
      throw new RuntimeException( "Error de-serializing row of data from byte array", e );
    }
  }

  /**
   * @return an XML representation of the row metadata
   * @throws IOException Thrown in case there is an (Base64/GZip) encoding problem
   */
  @Override
  public String getMetaXml() throws IOException {
    StringBuilder xml = new StringBuilder();

    xml.append( "<" ).append( XML_META_TAG ).append( ">" );

    lock.readLock().lock();
    try {
      for ( int i = 0; i < size(); i++ ) {
        xml.append( getValueMeta( i ).getMetaXml() );
      }
    } finally {
      lock.readLock().unlock();
    }

    xml.append( "</" ).append( XML_META_TAG ).append( ">" );

    return xml.toString();
  }

  /**
   * Create a new row metadata object from XML
   *
   * @param node the XML node to deserialize from
   * @throws IOException Thrown in case there is an (Base64/GZip) decoding problem
   */
  public RowMeta( Node node ) throws HopException {
    this();

    int nrValues = XmlHandler.countNodes( node, ValueMetaBase.XML_META_TAG );
    for ( int i = 0; i < nrValues; i++ ) {
      IValueMeta valueMetaSource = new ValueMetaBase( XmlHandler.getSubNodeByNr( node, ValueMetaBase.XML_META_TAG, i ) );
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta( valueMetaSource.getName(), valueMetaSource.getType(),
        valueMetaSource.getLength(), valueMetaSource.getPrecision() );
      ValueMetaFactory.cloneInfo( valueMetaSource, valueMeta );
      addValueMeta( valueMeta );
    }
  }

  /**
   * @param rowData the row of data to serialize as XML
   * @return an XML representation of the row data
   * @throws IOException Thrown in case there is an (Base64/GZip) encoding problem
   */
  @Override
  public String getDataXml(Object[] rowData ) throws IOException {
    StringBuilder xml = new StringBuilder();

    xml.append( "<" ).append( XML_DATA_TAG ).append( ">" );

    lock.readLock().lock();
    try {
      for ( int i = 0; i < size(); i++ ) {
        xml.append( getValueMeta( i ).getDataXml( rowData[ i ] ) );
      }
    } finally {
      lock.readLock().unlock();
    }

    xml.append( "</" ).append( XML_DATA_TAG ).append( ">" );

    return xml.toString();
  }

  /**
   * Convert an XML node into binary data using the row metadata supplied.
   *
   * @param node The data row node
   * @return a row of data, converted from XML
   * @throws IOException Thrown in case there is an (Base64/GZip) decoding problem
   */
  @Override
  public Object[] getRow( Node node ) throws HopException {
    lock.readLock().lock();
    try {
      Object[] rowData = RowDataUtil.allocateRowData( size() );

      for ( int i = 0; i < size(); i++ ) {
        Node valueDataNode = XmlHandler.getSubNodeByNr( node, ValueMetaBase.XML_DATA_TAG, i );
        rowData[ i ] = getValueMeta( i ).getValue( valueDataNode );
      }
      return rowData;
    } finally {
      lock.readLock().unlock();
    }
  }

  @VisibleForTesting
  static class RowMetaCache {
    @VisibleForTesting final Map<String, Integer> mapping;

    RowMetaCache() {
      this( new ConcurrentHashMap<>() );
    }

    /**
     * Copy constructor for clone
     *
     * @param rowMetaCache
     */
    RowMetaCache( RowMetaCache rowMetaCache ) {
      this( new ConcurrentHashMap<>( rowMetaCache.mapping ) );
    }

    RowMetaCache( Map<String, Integer> mapping ) {
      this.mapping = mapping;
    }

    void invalidate() {
      mapping.clear();
    }

    void storeMapping( String name, int index ) {
      if ( Utils.isEmpty( name ) ) {
        return;
      }
      mapping.put( name.toLowerCase(), index );
    }

    void replaceMapping( String old, String current, int index ) {
      if ( !Utils.isEmpty( old ) ) {
        mapping.remove( old.toLowerCase() );
      }
      storeMapping( current, index );
    }

    Integer findAndCompare( String name, List<? extends IValueMeta> metas ) {
      if ( Utils.isEmpty( name ) ) {
        return null;
      }

      name = name.toLowerCase();
      Integer index = mapping.get( name );
      if ( index != null ) {
        IValueMeta value = metas.get( index );
        if ( !name.equalsIgnoreCase( value.getName() ) ) {
          mapping.remove( name );
          index = null;
        }
      }
      return index;
    }

  }
}
