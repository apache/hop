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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transforms.databaselookup.DatabaseLookupData;
import org.apache.hop.pipeline.transforms.databaselookup.DatabaseLookupMeta;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This is a read-only array-based cache to be used in
 * {@linkplain org.apache.hop.pipeline.transforms.databaselookup.DatabaseLookup DatabaseLookup} when "Load All Data In ICache"
 * checkbox is selected. Internally, it uses some optimizations to reduce memory consumption.
 *
 * @author Andrey Khayrutdinov
 */
public class ReadAllCache implements DatabaseLookupData.ICache {

  private final DatabaseLookupData transformData;

  private final Object[][] keys;
  private final IRowMeta keysMeta;

  private final Object[][] data;

  private final Index[] indexes;

  // this is an array of pairs (column; condition),
  // such structure was chosen not to introduce separate data-container class
  private final int[][] otherConditions;

  ReadAllCache( DatabaseLookupData transformData, Object[][] keys, IRowMeta keysMeta, Object[][] data ) {
    this.transformData = transformData;
    this.keys = keys;
    this.keysMeta = keysMeta;
    this.data = data;

    Object[] pair = createIndexes( transformData, keysMeta, keys );
    indexes = (Index[]) pair[ 0 ];
    otherConditions = (int[][]) pair[ 1 ];
  }


  private static Object[] createIndexes( DatabaseLookupData transformData, IRowMeta keysMeta, Object[][] keys ) {
    final int rowsAmount = keys.length;
    final int[] conditions = transformData.conditions;

    // it makes sense to apply restrictions in the specific order, namely, to use those, that can filter more elements
    // Index.restrictionComparator() uses heuristic "restriction power" of each index
    PriorityQueue<Index> indexes = new PriorityQueue<>( conditions.length, Index.restrictionComparator() );
    List<int[]> otherConditions = new ArrayList<>();
    for ( int i = 0, len = conditions.length; i < len; i++ ) {
      int condition = conditions[ i ];
      Index index = null;
      switch ( condition ) {
        case DatabaseLookupMeta.CONDITION_EQ:
          index = new EqIndex( i, keysMeta.getValueMeta( i ), rowsAmount );
          break;
        case DatabaseLookupMeta.CONDITION_NE:
          index = EqIndex.nonEqualityIndex( i, keysMeta.getValueMeta( i ), rowsAmount );
          break;
        case DatabaseLookupMeta.CONDITION_LT:
          index = new LtIndex( i, keysMeta.getValueMeta( i ), rowsAmount );
          break;
        case DatabaseLookupMeta.CONDITION_LE:
          index = GtIndex.lessOrEqualCache( i, keysMeta.getValueMeta( i ), rowsAmount );
          break;
        case DatabaseLookupMeta.CONDITION_GT:
          index = new GtIndex( i, keysMeta.getValueMeta( i ), rowsAmount );
          break;
        case DatabaseLookupMeta.CONDITION_GE:
          index = LtIndex.greaterOrEqualCache( i, keysMeta.getValueMeta( i ), rowsAmount );
          break;
        case DatabaseLookupMeta.CONDITION_IS_NULL:
          index = new IsNullIndex( i, keysMeta.getValueMeta( i ), rowsAmount, true );
          break;
        case DatabaseLookupMeta.CONDITION_IS_NOT_NULL:
          index = new IsNullIndex( i, keysMeta.getValueMeta( i ), rowsAmount, false );
          break;
      }
      if ( index == null ) {
        otherConditions.add( new int[] { i, condition } );
      } else {
        index.performIndexingOf( keys );
        indexes.add( index );
      }
    }

    return new Object[] {
      indexes.toArray( new Index[ indexes.size() ] ),
      otherConditions.toArray( new int[ otherConditions.size() ][] )
    };
  }


  @Override
  public Object[] getRowFromCache( IRowMeta lookupMeta, Object[] lookupRow ) throws HopException {
    if ( transformData.hasDBCondition ) {
      // actually, there was no sense in executing SELECT from db in this case,
      // should be reported as improvement
      return null;
    }

    SearchingContext context = new SearchingContext();
    context.init( keys.length );

    for ( Index index : indexes ) {
      int column = index.getColumn();
      // IS (NOT) NULL operation does not require second argument
      // hence, lookupValue can be absent
      // basically, the index ignores both meta and value, so we can pass everything there
      Object lookupValue = ( column < lookupRow.length ) ? lookupRow[ column ] : null;
      index.applyRestrictionsTo( context, lookupMeta.getValueMeta( column ), lookupValue );
      if ( context.isEmpty() ) {
        // if nothing matches, break the search
        return null;
      }
    }

    // iterate through all elements survived after filtering stage
    // and find the first matching
    BitSet candidates = context.getCandidates();
    int candidate = candidates.nextSetBit( 0 );
    while ( candidate != -1 ) {
      Object[] dataKeys = keys[ candidate ];

      boolean matches = true;
      int lookupShift = 0;
      for ( int i = 0, len = otherConditions.length; i < len && matches; i++ ) {
        int[] columnConditionPair = otherConditions[ i ];

        final int column = columnConditionPair[ 0 ];
        Object keyData = dataKeys[ column ];
        IValueMeta keyMeta = keysMeta.getValueMeta( column );

        int lookupIndex = column + lookupShift;
        Object cmpData = lookupRow[ lookupIndex ];
        IValueMeta cmpMeta = lookupMeta.getValueMeta( lookupIndex );

        int condition = columnConditionPair[ 1 ];
        if ( condition == DatabaseLookupMeta.CONDITION_BETWEEN ) {
          // BETWEEN is a special condition demanding two arguments
          // technically there are no obstacles to implement it,
          // as it is just a short form of: (a <= b) && (b <= c)
          // however, let it be so for now
          matches = ( keyMeta.compare( keyData, cmpMeta, cmpData ) >= 0 );
          if ( matches ) {
            lookupShift++;
            lookupIndex++;
            IValueMeta cmpMeta2 = lookupMeta.getValueMeta( lookupIndex );
            Object cmpData2 = lookupRow[ lookupIndex ];
            matches = ( keyMeta.compare( keyData, cmpMeta2, cmpData2 ) <= 0 );
          }
        } else {
          // if not BETWEEN, than it is LIKE (or some new operator)
          // for now, LIKE is not supported here
          matches = false;
          transformData.hasDBCondition = true;
        }
      }
      if ( matches ) {
        return data[ candidate ];
      } else {
        candidate = candidates.nextSetBit( candidate + 1 );
      }
    }
    return null;
  }

  @Override
  public void storeRowInCache( DatabaseLookupMeta meta, IRowMeta lookupMeta, Object[] lookupRow,
                               Object[] add ) {
    throw new UnsupportedOperationException( "This cache is read-only" );
  }


  /**
   * Builder class for {@linkplain ReadAllCache}. Note, it does no checks or verifications!
   */
  public static class Builder {
    private final DatabaseLookupData transformData;
    private final Object[][] keys;
    private final Object[][] data;

    private IRowMeta keysMeta;

    private int current;

    public Builder( DatabaseLookupData transformData, int amount ) {
      this.transformData = transformData;
      keys = new Object[ amount ][];
      data = new Object[ amount ][];
    }

    public void setKeysMeta( IRowMeta keysMeta ) {
      this.keysMeta = keysMeta;
    }

    public void add( Object[] keys, Object[] data ) {
      this.keys[ current ] = keys;
      this.data[ current ] = data;
      current++;
    }

    public ReadAllCache build() {
      return new ReadAllCache( transformData, keys, keysMeta, data );
    }
  }
}
