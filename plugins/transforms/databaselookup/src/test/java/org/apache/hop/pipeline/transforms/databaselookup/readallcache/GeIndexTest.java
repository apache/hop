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

import org.apache.hop.core.row.IValueMeta;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * @author Andrey Khayrutdinov
 */
@RunWith( Parameterized.class )
public class GeIndexTest extends IndexTestBase<LtIndex> {

  @Parameterized.Parameters
  public static List<Object[]> createSampleData() {
    return IndexTestBase.createSampleData();
  }

  public GeIndexTest( Long[][] rows ) {
    super( LtIndex.class, rows );
  }

  @Override
  void doAssertMatches( BitSet candidates, long lookupValue, long actualValue ) {
    if ( !( actualValue >= lookupValue ) ) {
      fail( String.format( "All found values are expected to be greater than [%d] or equal to it, but got [%d] among %s",
        lookupValue, actualValue, candidates ) );
    }
  }

  @Override
  LtIndex createIndexInstance( int column, IValueMeta meta, int rowsAmount ) throws Exception {
    return (LtIndex) LtIndex.greaterOrEqualCache( column, meta, rowsAmount );
  }


  @Override
  public void lookupFor_MinusOne() {
    testFindsCorrectly( -1, 5 );
  }

  @Override
  public void lookupFor_Zero() {
    testFindsCorrectly( 0, 5 );
  }

  @Override
  public void lookupFor_One() {
    testFindsCorrectly( 1, 4 );
  }

  @Override
  public void lookupFor_Two() {
    testFindsCorrectly( 2, 3 );
  }

  @Override
  public void lookupFor_Three() {
    testFindsCorrectly( 3, 1 );
  }

  @Override
  public void lookupFor_Hundred() {
    testFindsNothing( 100 );
  }
}
