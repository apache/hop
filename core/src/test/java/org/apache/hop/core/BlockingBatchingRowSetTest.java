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

package org.apache.hop.core;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for the basic functionality of the blocking & batching row set.
 *
 * @author Matt Casters
 */
public class BlockingBatchingRowSetTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  public IRowMeta createRowMetaInterface() {
    IRowMeta rm = new RowMeta();

    IValueMeta[] valuesMeta = { new ValueMetaInteger( "ROWNR" ), };

    for ( int i = 0; i < valuesMeta.length; i++ ) {
      rm.addValueMeta( valuesMeta[ i ] );
    }

    return rm;
  }

  /**
   * The basic stuff.
   */
  @Test
  public void testBasicCreation() {
    IRowSet set = new BlockingBatchingRowSet( 10 );

    assertTrue( !set.isDone() );
    assertEquals( 0, set.size() );
  }

  /**
   * Functionality test.
   */
  @Test
  public void testFuntionality1() {
    BlockingBatchingRowSet set = new BlockingBatchingRowSet( 10 );

    IRowMeta rm = createRowMetaInterface();

    List<Object[]> rows = new ArrayList<>();
    for ( int i = 0; i < 5; i++ ) {
      rows.add( new Object[] { new Long( i ), } );
    }

    assertEquals( 0, set.size() );

    // Pop off row. This should return null (no row available: has a timeout)
    //
    Object[] r = set.getRow();
    assertNull( r );

    // Add rows. set doesn't report rows, batches them
    // this batching row set has 2 buffers with 2 rows, the 5th row will cause the rows to be exposed.
    //
    int index = 0;
    while ( index < 4 ) {
      set.putRow( rm, rows.get( index++ ) );
      assertEquals( 0, set.size() );
    }
    set.putRow( rm, rows.get( index++ ) );
    assertEquals( 5, set.size() );

    // Signal done...
    //
    set.setDone();
    assertTrue( set.isDone() );

    // Get a row back...
    //
    r = set.getRow();
    assertNotNull( r );
    assertArrayEquals( rows.get( 0 ), r );

    // Get a row back...
    //
    r = set.getRow();
    assertNotNull( r );
    assertArrayEquals( rows.get( 1 ), r );

    // Get a row back...
    //
    r = set.getRow();
    assertNotNull( r );
    assertArrayEquals( rows.get( 2 ), r );
  }
}
