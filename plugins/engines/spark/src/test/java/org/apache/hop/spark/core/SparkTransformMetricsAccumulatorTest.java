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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SparkTransformMetricsAccumulatorTest {

  @Test
  void addAndMergeKeepsPartitionsSeparateAndTakesMaxCounters() {
    SparkTransformMetricsAccumulator acc = new SparkTransformMetricsAccumulator();
    acc.add(slice("Dummy", 0, 100, 100, true, false));
    acc.add(slice("Dummy", 0, 250, 250, true, false)); // progress on same partition
    acc.add(slice("Dummy", 1, 80, 80, true, false));

    Map<String, SparkTransformMetricSlice> value = acc.value();
    assertEquals(2, value.size());
    assertEquals(250, value.get(key("Dummy", 0)).getLinesRead());
    assertEquals(250, value.get(key("Dummy", 0)).getLinesWritten());
    assertEquals(80, value.get(key("Dummy", 1)).getLinesRead());
    assertTrue(value.get(key("Dummy", 0)).isRunning());
    assertFalse(value.get(key("Dummy", 0)).isFinished());
    assertEquals(0L, value.get(key("Dummy", 0)).getEndTimeMs()); // still running

    SparkTransformMetricsAccumulator other = new SparkTransformMetricsAccumulator();
    other.add(slice("Dummy", 0, 300, 300, false, true));
    other.add(slice("Filter", 0, 10, 9, false, true));
    acc.merge(other);

    value = acc.value();
    assertEquals(3, value.size());
    assertEquals(300, value.get(key("Dummy", 0)).getLinesRead());
    assertTrue(value.get(key("Dummy", 0)).isFinished());
    assertFalse(value.get(key("Dummy", 0)).isRunning());
    assertEquals(10, value.get(key("Filter", 0)).getLinesRead());
    assertEquals(1_000L, value.get(key("Dummy", 0)).getStartTimeMs());
    assertEquals(1_500L, value.get(key("Dummy", 0)).getEndTimeMs());
    assertEquals(500L, value.get(key("Dummy", 0)).durationMs());
  }

  @Test
  void copyAndReset() {
    SparkTransformMetricsAccumulator acc = new SparkTransformMetricsAccumulator();
    acc.add(slice("A", 0, 1, 1, true, false));
    assertFalse(acc.isZero());

    SparkTransformMetricsAccumulator copy = (SparkTransformMetricsAccumulator) acc.copy();
    assertEquals(1, copy.value().size());

    acc.reset();
    assertTrue(acc.isZero());
    assertEquals(1, copy.value().size());
  }

  private static SparkTransformMetricSlice slice(
      String name, int copy, long read, long written, boolean running, boolean finished) {
    long start = 1_000L;
    long end = finished ? 1_500L : 0L;
    return new SparkTransformMetricSlice(
        name, copy, "host1", read, written, 0, 0, 0, running, finished, start, end);
  }

  private static String key(String name, int copy) {
    return name + '\0' + copy;
  }
}
