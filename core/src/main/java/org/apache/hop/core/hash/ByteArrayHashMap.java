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

package org.apache.hop.core.hash;

import org.apache.commons.collections4.map.AbstractHashedMap;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ByteArrayHashMap extends AbstractHashedMap {
  private IRowMeta keyMeta;

  /**
   * Constructs an empty <tt>ByteArrayHashMap</tt> with the specified initial capacity and load factor.
   *
   * @param initialCapacity the initial capacity
   * @param loadFactor      the load factor
   * @throws IllegalArgumentException if the initial capacity is negative or the load factor is nonpositive
   */
  public ByteArrayHashMap( int initialCapacity, float loadFactor, IRowMeta keyMeta ) {
    super( initialCapacity, loadFactor );
    this.keyMeta = keyMeta;
  }

  @Override
  protected boolean isEqualKey( Object key1, Object key2 ) {
    return equalsByteArray( (byte[]) key1, (byte[]) key2 );
  }

  @Override
  protected boolean isEqualValue( Object value1, Object value2 ) {
    return equalsByteArray( (byte[]) value1, (byte[]) value2 );
  }

  public final boolean equalsByteArray( byte[] value, byte[] cmpValue ) {
    if ( value.length == cmpValue.length ) {
      for ( int i = 0; i < value.length; i++ ) {
        if ( value[ i ] != cmpValue[ i ] ) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Constructs an empty <tt>ByteArrayHashMap</tt> with the specified initial capacity and the default load factor
   * (0.75).
   *
   * @param initialCapacity the initial capacity.
   * @throws IllegalArgumentException if the initial capacity is negative.
   */
  public ByteArrayHashMap( int initialCapacity, IRowMeta keyMeta ) {
    this( initialCapacity, DEFAULT_LOAD_FACTOR, keyMeta );
  }

  /**
   * Constructs an empty <tt>HashMap</tt> with the default initial capacity (16) and the default load factor (0.75).
   */
  public ByteArrayHashMap( IRowMeta keyMeta ) {
    this( DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, keyMeta );
  }

  /**
   * Returns the entry to which the specified key is &quot;mapped&quot;, or, in other words, if this index contains an
   * entry that is equal to the given key, or {@code null} if this is not the case.
   *
   * <p>
   * More formally, if this index contains an entry {@code e} such that {@code key.equals(e))}, then this method returns
   * {@code e}; otherwise it returns {@code null}. (There can be at most one such entry.)
   *
   * @param key The key to look up.
   * @throws HopValueException in case of a value conversion error
   * @see #put(Object)
   * @see #insert(Object)
   */
  public byte[] get( byte[] key ) {
    return (byte[]) super.get( key );
  }

  public void put( byte[] key, byte[] value ) {
    super.put( key, value );
  }

  @Override
  protected int hash( Object key ) {
    byte[] rowKey = (byte[]) key;
    try {
      return keyMeta.hashCode( RowMeta.getRow( keyMeta, rowKey ) );
    } catch ( HopValueException ex ) {
      throw new IllegalArgumentException( ex );
    }
  }

  @SuppressWarnings( "unchecked" )
  public List<byte[]> getKeys() {
    List<byte[]> rtn = new ArrayList<>( this.size() );
    Set<byte[]> kSet = this.keySet();
    for ( Iterator<byte[]> it = kSet.iterator(); it.hasNext(); ) {
      rtn.add( it.next() );
    }
    return rtn;
  }
}
