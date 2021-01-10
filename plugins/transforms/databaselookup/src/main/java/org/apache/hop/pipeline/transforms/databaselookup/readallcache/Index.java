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

package org.apache.hop.pipeline.transforms.databaselookup.readallcache;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Andrey Khayrutdinov
 */
abstract class Index {

  final int column;
  final IValueMeta valueMeta;
  final IndexedValue[] values;

  Index( int column, IValueMeta valueMeta, int rowsAmount ) {
    this.column = column;
    this.valueMeta = valueMeta;
    this.values = new IndexedValue[ rowsAmount ];
  }

  Comparator<IndexedValue> createComparator() {
    return new IndexValueComparator( valueMeta );
  }

  void performIndexingOf( Object[][] rows ) {
    for ( int i = 0, len = rows.length; i < len; i++ ) {
      values[ i ] = new IndexedValue( rows[ i ][ column ], i );
    }
    // sort values using meta to compare and row number as seconds dimension
    Arrays.sort( values, createComparator() );
  }

  /**
   * Performs binary search algorithm looking for {@code value} in the sorted array and returns decoded index of
   * insertion (decoded index returned by Arrays.binarySearch())
   *
   * @param value value to look for
   * @return decoded index
   */
  int findInsertionPointOf( IndexedValue value ) {
    int index = Arrays.binarySearch( values, value, createComparator() );
    // index == ( -insertion_point - 1)
    return -( index + 1 );
  }


  public int getColumn() {
    return column;
  }

  public void applyRestrictionsTo( SearchingContext context, IValueMeta lookupMeta, Object lookupValue ) {
    try {
      doApply( context, lookupMeta, lookupValue );
    } catch ( HopException e ) {
      throw new RuntimeException( e );
    }
  }

  abstract void doApply( SearchingContext context, IValueMeta lookupMeta, Object lookupValue )
    throws HopException;

  /**
   * Return the "anti-strength" of the restriction of the index. It is a heuristic weight of the restriction, needed
   * to push more "powerful" filters before less "powerful" to cut as much values as possible at the beginning.
   *
   * @return integer number, the less it is, the more "powerful" restriction is presumed to be
   */
  abstract int getRestrictionPower();

  static Comparator<Index> restrictionComparator() {
    return ( o1, o2 ) -> Integer.compare( o1.getRestrictionPower(), o2.getRestrictionPower() );
  }


  static class IndexedValue {
    final Object key;
    final int row;

    public IndexedValue( Object key, int row ) {
      this.key = key;
      this.row = row;
    }
  }

  static class IndexValueComparator implements Comparator<IndexedValue> {

    private final IValueMeta meta;

    public IndexValueComparator( IValueMeta meta ) {
      this.meta = meta;
    }

    @Override
    public int compare( IndexedValue o1, IndexedValue o2 ) {
      // does not expect nulls here!
      int c;
      try {
        c = meta.compare( o1.key, o2.key );
      } catch ( HopValueException e ) {
        throw new RuntimeException( e );
      }

      return ( c == 0 ) ? Integer.compare( o1.row, o2.row ) : c;
    }
  }
}
