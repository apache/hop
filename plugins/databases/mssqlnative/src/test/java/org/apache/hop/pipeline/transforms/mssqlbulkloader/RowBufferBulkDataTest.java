/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;

class RowBufferBulkDataTest {

  private static final RowBufferBulkData.Column[] COLUMNS = {
    new RowBufferBulkData.Column("id", Types.INTEGER, 10, 0),
    new RowBufferBulkData.Column("amount", Types.DECIMAL, 18, 4)
  };

  @Test
  void ordinalsAreOneBasedAndContiguous() {
    // The driver iterates this set and indexes getRowData() positionally, so 1..n has to hold.
    RowBufferBulkData data = new RowBufferBulkData(List.<Object[]>of(), COLUMNS);
    assertIterableEquals(List.of(1, 2), data.getColumnOrdinals());
  }

  @Test
  void describesEachColumnTheWayTheDriverAsks() {
    RowBufferBulkData data = new RowBufferBulkData(List.<Object[]>of(), COLUMNS);

    assertEquals("id", data.getColumnName(1));
    assertEquals(Types.INTEGER, data.getColumnType(1));
    assertEquals(10, data.getPrecision(1));
    assertEquals(0, data.getScale(1));

    assertEquals("amount", data.getColumnName(2));
    assertEquals(Types.DECIMAL, data.getColumnType(2));
    assertEquals(18, data.getPrecision(2));
    assertEquals(4, data.getScale(2));
  }

  @Test
  void walksTheBufferOnceAndThenStops() {
    Object[] firstRow = {1L, "10.00"};
    Object[] secondRow = {2L, "20.00"};
    RowBufferBulkData data = new RowBufferBulkData(List.of(firstRow, secondRow), COLUMNS);

    assertTrue(data.next());
    assertArrayEquals(firstRow, data.getRowData());
    assertTrue(data.next());
    assertArrayEquals(secondRow, data.getRowData());
    assertFalse(data.next());
  }

  @Test
  void anEmptyBufferYieldsNoRows() {
    assertFalse(new RowBufferBulkData(List.<Object[]>of(), COLUMNS).next());
  }

  @Test
  void nullsReachTheDriverAsNulls() {
    // The step this transform replaces rendered rows to CSV, which turned every null into an empty
    // string. Nothing between the pipeline row and the driver may do that again.
    Object[] row = {null, null};
    RowBufferBulkData data = new RowBufferBulkData(List.<Object[]>of(row), COLUMNS);

    assertTrue(data.next());
    assertNull(data.getRowData()[0]);
    assertNull(data.getRowData()[1]);
  }
}
