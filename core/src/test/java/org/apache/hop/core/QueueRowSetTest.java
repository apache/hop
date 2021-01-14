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
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class QueueRowSetTest {
  Object[] row;
  QueueRowSet rowSet;

  @Before
  public void setup() {
    rowSet = new QueueRowSet();
    row = new Object[] {};
  }

  @Test
  public void testPutRow() throws Exception {
    rowSet.putRow( new RowMeta(), row );
    assertSame( row, rowSet.getRow() );
  }

  @Test
  public void testPutRowWait() throws Exception {
    rowSet.putRowWait( new RowMeta(), row, 1, TimeUnit.SECONDS );
    assertSame( row, rowSet.getRowWait( 1, TimeUnit.SECONDS ) );
  }

  @Test
  public void testGetRowImmediate() throws Exception {
    rowSet.putRow( new RowMeta(), row );
    assertSame( row, rowSet.getRowImmediate() );
  }

  @Test
  public void testSize() throws Exception {
    assertEquals( 0, rowSet.size() );
    rowSet.putRow( new RowMeta(), row );
    assertEquals( 1, rowSet.size() );
    rowSet.putRow( new RowMeta(), row );
    assertEquals( 2, rowSet.size() );
    rowSet.clear();
    assertEquals( 0, rowSet.size() );
  }
}
