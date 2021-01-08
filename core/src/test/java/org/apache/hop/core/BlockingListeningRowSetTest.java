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

import org.apache.hop.core.row.RowMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class BlockingListeningRowSetTest {
  @Test
  public void testClass() {
    BlockingListeningRowSet rowSet = new BlockingListeningRowSet( 1 );
    assertEquals( 0, rowSet.size() );
    final Object[] row = new Object[] {};
    final RowMeta meta = new RowMeta();
    rowSet.putRow( meta, row );
    assertSame( meta, rowSet.getRowMeta() );
    assertEquals( 1, rowSet.size() );
    assertFalse( rowSet.isBlocking() );
    assertSame( row, rowSet.getRow() );
    assertEquals( 0, rowSet.size() );
    rowSet.putRow( meta, row );
    assertSame( row, rowSet.getRowImmediate() );
    rowSet.putRow( meta, row );
    assertEquals( 1, rowSet.size() );
    rowSet.clear();
    assertEquals( 0, rowSet.size() );
  }
}
