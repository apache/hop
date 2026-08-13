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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class SpillingRowSetTest {

  private static IRowMeta rowMeta() {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaInteger("ROWNR"));
    return rm;
  }

  @Test
  void memoryOnlyPathBehavesLikeBoundedQueue() {
    SpillingRowSet set = new SpillingRowSet(3);
    IRowMeta rm = rowMeta();

    assertTrue(set.putRow(rm, new Object[] {1L}));
    assertTrue(set.putRow(rm, new Object[] {2L}));
    assertTrue(set.size() > 0);
    assertFalse(set.hasSpilled());

    assertEquals(1L, set.getRowImmediate()[0]);
    assertEquals(2L, set.getRowImmediate()[0]);
    assertNull(set.getRowImmediate());
    assertEquals(0, set.size());
  }

  @Test
  void spillsWhenFullAndPreservesFifoOrder() {
    SpillingRowSet set = new SpillingRowSet(2);
    IRowMeta rm = rowMeta();

    for (long i = 1; i <= 5; i++) {
      assertTrue(set.putRow(rm, new Object[] {i}), "put " + i);
    }
    assertTrue(set.hasSpilled());
    // size() is a flow-control signal (mid when work pending), not pure memory count
    assertTrue(set.size() > 0);
    assertTrue(set.size() < 2, "must not report full or BaseTransform will sleep per row");
    assertEquals(3, set.getUnreadSpilled());

    set.setDone();

    for (long expected = 1; expected <= 5; expected++) {
      Object[] row = set.getRow();
      assertNotNull(row, "missing row " + expected);
      assertEquals(expected, row[0]);
    }
    assertNull(set.getRow());
  }

  @Test
  void clearDeletesState() {
    SpillingRowSet set = new SpillingRowSet(1);
    IRowMeta rm = rowMeta();
    assertTrue(set.putRow(rm, new Object[] {1L}));
    assertTrue(set.putRow(rm, new Object[] {2L}));
    assertTrue(set.hasSpilled());

    set.clear();
    assertEquals(0, set.size());
    assertEquals(0, set.getUnreadSpilled());
    assertFalse(set.isDone());
    assertNull(set.getRowImmediate());
  }

  @Test
  void doneWithEmptyReturnsNull() {
    SpillingRowSet set = new SpillingRowSet(2);
    set.setDone();
    assertNull(set.getRow());
  }

  @Test
  void sizeDoesNotAdvertiseFullWhileSpilling() {
    // BaseTransform uses size() >= 0.99 * capacity to Thread.sleep(0,1) before put.
    SpillingRowSet set = new SpillingRowSet(100);
    IRowMeta rm = rowMeta();
    for (long i = 0; i < 150; i++) {
      assertTrue(set.putRow(rm, new Object[] {i}));
    }
    assertTrue(set.hasSpilled());
    int upper = (int) (100 * 0.99);
    assertTrue(
        set.size() < upper,
        "size()=" + set.size() + " must stay below upper boundary " + upper + " while spilling");
    // Still not "empty" so consumer low-water sleep is avoided
    int lower = (int) (100 * 0.01);
    assertTrue(set.size() > lower, "size() must stay above lower boundary while work is pending");
  }
}
