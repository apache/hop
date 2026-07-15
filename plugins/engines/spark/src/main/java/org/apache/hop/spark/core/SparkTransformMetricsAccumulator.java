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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.util.AccumulatorV2;

/**
 * Merging Spark accumulator of per-partition Hop transform metric slices. Registered on the driver
 * and referenced from {@link HopMapPartitionsFn} closures so executors can report progress.
 */
public class SparkTransformMetricsAccumulator
    extends AccumulatorV2<SparkTransformMetricSlice, Map<String, SparkTransformMetricSlice>> {

  private static final long serialVersionUID = 1L;

  private final Map<String, SparkTransformMetricSlice> map;

  public SparkTransformMetricsAccumulator() {
    this.map = new HashMap<>();
  }

  private SparkTransformMetricsAccumulator(Map<String, SparkTransformMetricSlice> map) {
    this.map = map;
  }

  @Override
  public boolean isZero() {
    synchronized (map) {
      return map.isEmpty();
    }
  }

  @Override
  public AccumulatorV2<SparkTransformMetricSlice, Map<String, SparkTransformMetricSlice>> copy() {
    synchronized (map) {
      return new SparkTransformMetricsAccumulator(new HashMap<>(map));
    }
  }

  @Override
  public void reset() {
    synchronized (map) {
      map.clear();
    }
  }

  @Override
  public void add(SparkTransformMetricSlice v) {
    if (v == null || v.getTransformName() == null) {
      return;
    }
    synchronized (map) {
      String key = v.key();
      SparkTransformMetricSlice existing = map.get(key);
      map.put(key, existing == null ? v : existing.mergeWith(v));
    }
  }

  @Override
  public void merge(
      AccumulatorV2<SparkTransformMetricSlice, Map<String, SparkTransformMetricSlice>> other) {
    if (other == null) {
      return;
    }
    Map<String, SparkTransformMetricSlice> otherMap = other.value();
    if (otherMap == null || otherMap.isEmpty()) {
      return;
    }
    synchronized (map) {
      for (Map.Entry<String, SparkTransformMetricSlice> entry : otherMap.entrySet()) {
        SparkTransformMetricSlice existing = map.get(entry.getKey());
        map.put(
            entry.getKey(),
            existing == null ? entry.getValue() : existing.mergeWith(entry.getValue()));
      }
    }
  }

  @Override
  public Map<String, SparkTransformMetricSlice> value() {
    synchronized (map) {
      if (map.isEmpty()) {
        return Collections.emptyMap();
      }
      return Collections.unmodifiableMap(new HashMap<>(map));
    }
  }
}
