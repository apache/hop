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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for the basic functionality of IRowSet.
 *
 * @author Sven Boden
 */
public class RowSetTest {
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
    IRowSet set = new BlockingRowSet( 10 );

    assertTrue( !set.isDone() );
    // TODO assertTrue(set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 0, set.size() );
  }

  /**
   * Functionality test.
   */
  @Test
  public void testFuntionality1() {
    IRowSet set = new BlockingRowSet( 3 );

    IRowMeta rm = createRowMetaInterface();

    Object[] r1 = new Object[] { new Long( 1L ) };
    Object[] r2 = new Object[] { new Long( 2L ) };
    Object[] r3 = new Object[] { new Long( 3L ) };
    Object[] r4 = new Object[] { new Long( 4L ) };
    Object[] r5 = new Object[] { new Long( 5L ) };

    // TODO assertTrue(set.isEmpty());
    assertEquals( 0, set.size() );

    // Add first row. State 1
    set.putRow( rm, r1 );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 1, set.size() );

    // Add another row. State: 1 2
    set.putRow( rm, r2 );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 2, set.size() );

    // Pop off row. State: 2
    Object[] r = set.getRow();
    int i = rm.indexOfValue( "ROWNR" );
    assertEquals( 1L, ( (Long) r[ i ] ).longValue() );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 1, set.size() );

    // Add another row. State: 2 3
    set.putRow( rm, r3 );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 2, set.size() );

    // Add another row. State: 2 3 4
    set.putRow( rm, r4 );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(set.isFull());
    assertEquals( 3, set.size() );

    /*********************************************************************
     * This was made in more restrict in v2.5.0 with a new IRowSet implementation. After v2.5.0 you may not try to put
     * more rows in a rowset then it can hold (this functionality was also never used in PDI anyway).
     *
     * // Add another row. State: 2 3 4 5 // Note that we can still add rows after the set is full. set.putRow(r5);
     * assertTrue(!set.isEmpty()); assertTrue(set.isFull()); assertEquals(4, set.size());
     *********************************************************************/

    // Pop off row. State: 3 4
    r = set.getRow();
    i = rm.indexOfValue( "ROWNR" );
    assertEquals( 2L, ( (Long) r[ i ] ).longValue() );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 2, set.size() );

    // Add another row. State: 3 4 5
    set.putRow( rm, r5 );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(set.isFull());
    assertEquals( 3, set.size() );

    // Pop off row. State: 4 5
    r = set.getRow();
    i = rm.indexOfValue( "ROWNR" );
    assertEquals( 3L, ( (Long) r[ i ] ).longValue() );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 2, set.size() );

    // Pop off row. State: 5
    r = set.getRow();
    i = rm.indexOfValue( "ROWNR" );
    assertEquals( 4L, ( (Long) r[ i ] ).longValue() );
    // TODO assertTrue(!set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 1, set.size() );

    // Pop off row. State:
    r = set.getRow();
    i = rm.indexOfValue( "ROWNR" );
    assertEquals( 5L, ( (Long) r[ i ] ).longValue() );
    // TODO assertTrue(set.isEmpty());
    // TODO assertTrue(!set.isFull());
    assertEquals( 0, set.size() );

    /*********************************************************************
     * This was changed v2.5.0 with a new IRowSet // Pop off row. State: try { r = set.getRow();
     * fail("expected NoSuchElementException"); } catch ( IndexOutOfBoundsException ex ) { } assertTrue(set.isEmpty());
     * assertTrue(!set.isFull()); assertEquals(0, set.size());
     **********************************************************************/
  }

  /**
   * Names test. Just for completeness.
   */
  @Test
  public void testNames() {
    IRowSet set = new BlockingRowSet( 3 );

    set.setThreadNameFromToCopy( "from", 2, "to", 3 );

    assertEquals( "from", set.getOriginTransformName() );
    assertEquals( 2, set.getOriginTransformCopy() );
    assertEquals( "to", set.getDestinationTransformName() );
    assertEquals( 3, set.getDestinationTransformCopy() );
    assertEquals( set.toString(), set.getName() );
    assertEquals( "from.2 - to.3", set.getName() );
  }
}
