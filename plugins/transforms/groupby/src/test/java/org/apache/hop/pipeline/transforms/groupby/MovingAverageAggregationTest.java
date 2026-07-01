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

package org.apache.hop.pipeline.transforms.groupby;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the Moving Average (Last N Events) aggregation type in the Group By transform.
 *
 * <p>Uses the same package as GroupBy so that package-private methods (addMovingAverages,
 * newAggregate, getAggregateResult, setAllNullsAreZero) are accessible.
 */
class MovingAverageAggregationTest {

  @BeforeAll
  static void setUpClass() throws HopException {
    HopEnvironment.init();
  }

  private GroupByMeta meta;
  private GroupByData data;
  private GroupBy groupBy;

  /** Builds a minimal GroupBy with one MOVING_AVERAGE aggregation on a Number column (N=3). */
  @BeforeEach
  void setUp() throws HopException {
    meta = new GroupByMeta();

    // Minimal TransformMeta stub required by BaseTransform constructor
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("test-groupby");

    // Input row: [amount (Number)]
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaNumber("amount"));

    // One aggregation: MOVING_AVERAGE of "amount" with N=3
    Aggregation agg =
        new Aggregation(
            "moving_avg_amount",
            "amount",
            Aggregation.getTypeDescLongFromCode(Aggregation.TYPE_GROUP_MOVING_AVERAGE),
            "3",
            "");
    meta.getAggregations().add(agg);

    data = new GroupByData();
    data.inputRowMeta = inputRowMeta;
    data.subjectnrs = new int[] {0}; // "amount" is at index 0
    data.counts = new long[1];
    data.mean = new double[1];
    // Cumulative-sum/avg tracking arrays (required by newAggregate's teardown section)
    data.previousSums = new Object[0];
    data.previousAvgSum = new Object[0];
    data.previousAvgCount = new long[0];

    // Initialise moving average metadata lists used by addMovingAverages
    data.movingAvgSourceIndexes = List.of(0);
    data.movingAvgTargetIndexes = List.of(1);
    data.movingAvgWidths = new ArrayList<>(List.of(3));
    data.movingAvgIndexes = List.of(0);

    // Initialise the sliding window array
    @SuppressWarnings("unchecked")
    ArrayDeque<Double>[] windows = new ArrayDeque[1];
    data.movingAvgWindows = windows;
    data.movingAvgWindows[0] = new ArrayDeque<>();

    // Minimal GroupBy instance - we only exercise addMovingAverages/newAggregate/getAggregateResult
    groupBy =
        new GroupBy(transformMeta, meta, data, 0, null, null) {
          // Override resolve so variable expressions are returned as-is
          @Override
          public String resolve(String str) {
            return str;
          }
        };
    groupBy.setAllNullsAreZero(false);

    // Call newAggregate to initialise data.agg[] and data.aggMeta
    groupBy.newAggregate(new Object[] {null});
  }

  // Helper: feed a value through addMovingAverages and return the computed moving_avg value
  private Object feedValue(Double value) throws Exception {
    Object[] row = new Object[] {value, null};
    groupBy.addMovingAverages(row);
    return row[1];
  }

  /**
   * With N=3, the first two rows should produce null (window not full), and the third row should
   * produce the average of the three values.
   */
  @Test
  void testPartialWindowReturnsNull() throws Exception {
    assertNull(feedValue(10.0), "1st row: window not full -> null");
    assertNull(feedValue(20.0), "2nd row: window not full -> null");
    assertEquals(20.0, (Double) feedValue(30.0), 1e-9, "3rd row: full window -> avg(10,20,30)=20");
  }

  /** After the window fills, it slides correctly: removes oldest, adds newest, recomputes. */
  @Test
  void testSlidingWindowMovesCorrectly() throws Exception {
    feedValue(10.0); // null
    feedValue(20.0); // null
    feedValue(30.0); // avg(10,20,30)=20
    // Now add 40: window = [20,30,40], avg = 30
    assertEquals(30.0, (Double) feedValue(40.0), 1e-9, "avg(20,30,40)=30");
    // Add 50: window = [30,40,50], avg = 40
    assertEquals(40.0, (Double) feedValue(50.0), 1e-9, "avg(30,40,50)=40");
  }

  /** A null subject value must be ignored: the window does not grow and result stays null. */
  @Test
  void testNullSubjectIsSkipped() throws Exception {
    feedValue(10.0); // window=[10], size=1 -> null
    feedValue(null); // null value skipped; window=[10], size=1 -> still null
    assertNull(feedValue(20.0), "Window size 2 < N=3 -> null");
    assertEquals(20.0, (Double) feedValue(30.0), 1e-9, "After null skip: avg(10,20,30)=20");
  }

  /**
   * Resetting the window (simulating a new group) via newAggregate must clear the deque and reset
   * to null.
   */
  @Test
  void testWindowResetsOnNewGroup() throws Exception {
    feedValue(10.0);
    feedValue(20.0);
    feedValue(30.0); // full window, avg=20

    // Simulate start of a new group
    groupBy.newAggregate(new Object[] {null});

    // Window should be clear; first value produces null again
    assertNull(feedValue(5.0), "After group reset, 1st value -> null");
    assertNull(feedValue(15.0), "After group reset, 2nd value -> null");
    assertEquals(
        15.0, (Double) feedValue(25.0), 1e-9, "After group reset, 3rd value avg(5,15,25)=15");
  }

  /** With N=1, every non-null row should immediately return that row's own value. */
  @Test
  void testWindowSizeOne() throws Exception {
    // Reconfigure with N=1
    meta.getAggregations().get(0).setValue("1");
    data.movingAvgWidths.set(0, 1);
    data.movingAvgWindows[0].clear();
    groupBy.newAggregate(new Object[] {null});

    assertEquals(42.0, (Double) feedValue(42.0), 1e-9, "N=1: returns current value 42");
    assertEquals(7.0, (Double) feedValue(7.0), 1e-9, "N=1: returns current value 7");
    assertEquals(100.0, (Double) feedValue(100.0), 1e-9, "N=1: returns current value 100");
  }

  /** Verify that getAggregateResult passes through the rolling value stored in agg[0]. */
  @Test
  void testGetAggregateResultPassesThrough() throws Exception {
    data.agg[0] = 20.0;
    Object[] result = groupBy.getAggregateResult();
    assertEquals(20.0, (Double) result[0], 1e-9, "getAggregateResult passes through moving avg");
  }
}
