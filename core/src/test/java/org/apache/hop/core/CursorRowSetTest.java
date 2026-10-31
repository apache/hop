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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class CursorRowSetTest {

  private static IRowMeta rowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("n"));
    return rowMeta;
  }

  private static void put(CursorRowSet rowSet, long value) {
    assertTrue(rowSet.putRow(rowMeta(), new Object[] {value}));
  }

  private static long value(CursorRowSet rowSet) {
    return ((Number) rowSet.getRowImmediate()[0]).longValue();
  }

  @Test
  void cursorLeavesRowsInPlace() {
    CursorRowSet rowSet = new CursorRowSet(10);
    try {
      put(rowSet, 1L);
      put(rowSet, 2L);
      put(rowSet, 3L);

      assertEquals(3, rowSet.size());
      assertEquals(3L, rowSet.storedCount());
      assertEquals(1L, value(rowSet));
      assertEquals(2, rowSet.size());
      assertEquals(3L, rowSet.storedCount());

      rowSet.setCursor(0);
      assertEquals(3, rowSet.size());
      assertEquals(1L, value(rowSet));
      assertEquals(2L, value(rowSet));
      assertEquals(3L, value(rowSet));
      assertNull(rowSet.getRowImmediate());
    } finally {
      rowSet.clear();
    }
  }

  @Test
  void markAndResetRewindsTheCursor() {
    CursorRowSet rowSet = new CursorRowSet(10);
    try {
      put(rowSet, 4L);
      put(rowSet, 5L);
      rowSet.mark();
      assertEquals(4L, value(rowSet));
      rowSet.resetToMark();
      assertEquals(0, rowSet.getCursor());
      assertEquals(4L, value(rowSet));
    } finally {
      rowSet.clear();
    }
  }

  @Test
  void trialPutsStayOutOfTheCommittedBuffer() {
    CursorRowSet rowSet = new CursorRowSet(10);
    try {
      put(rowSet, 7L);
      rowSet.setTrialMode(true);
      put(rowSet, 8L);
      rowSet.setTrialMode(false);

      assertEquals(1, rowSet.size());
      assertEquals(1L, rowSet.storedCount());
      assertEquals(1, rowSet.getTrialCount());
      assertEquals(8L, ((Number) rowSet.getTrialRows().get(0)[0]).longValue());
      assertEquals(7L, value(rowSet));
    } finally {
      rowSet.clear();
    }
  }

  @Test
  void rowsPastTheThresholdSpillAndStayReadable() {
    CursorRowSet rowSet = new CursorRowSet(2);
    try {
      put(rowSet, 1L);
      put(rowSet, 2L);
      put(rowSet, 3L);
      put(rowSet, 4L);

      assertTrue(rowSet.hasSpilled());
      assertEquals(2, rowSet.getInMemoryCount());
      assertEquals(4L, rowSet.storedCount());
      assertEquals(1L, value(rowSet));
      assertEquals(2L, value(rowSet));
      assertEquals(3L, value(rowSet));
      assertEquals(4L, value(rowSet));

      rowSet.setCursor(2);
      assertEquals(3L, value(rowSet));
    } finally {
      rowSet.clear();
    }
  }

  @Test
  void copiesKeepSeparateCursors() {
    CursorRowSet first = new CursorRowSet(10);
    CursorRowSet second = new CursorRowSet(10);
    try {
      put(first, 1L);
      put(second, 9L);
      assertEquals(1L, value(first));
      assertEquals(1, second.size());
      assertEquals(0, second.getCursor());
    } finally {
      first.clear();
      second.clear();
    }
  }

  @Test
  void setCursorRejectsAnIndexPastTheBuffer() {
    CursorRowSet rowSet = new CursorRowSet(10);
    try {
      put(rowSet, 1L);
      assertThrows(IllegalArgumentException.class, () -> rowSet.setCursor(2));
      assertEquals(0, rowSet.getCursor());
    } finally {
      rowSet.clear();
    }
  }
}
