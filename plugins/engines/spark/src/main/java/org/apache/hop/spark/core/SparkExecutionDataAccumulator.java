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
 * Collects JSON-serialized {@code ExecutionData} samples from Spark executors and merges them on
 * the driver. Keyed by owner id (transform copy log channel id) so the latest flush per copy wins.
 *
 * <p>Caching execution info locations keep a single parent {@code CacheEntry} in the driver
 * process; executor-side {@code registerData} cannot reliably attach samples to that entry.
 * Shipping samples through this accumulator lets the driver register them where the parent entry
 * already exists.
 */
public class SparkExecutionDataAccumulator extends AccumulatorV2<String, Map<String, String>> {

  private static final long serialVersionUID = 1L;

  /** Delimiter between ownerId and JSON payload in accumulator elements. */
  private static final char SEP = '\u0001';

  private final Map<String, String> byOwnerId;

  public SparkExecutionDataAccumulator() {
    this.byOwnerId = new HashMap<>();
  }

  private SparkExecutionDataAccumulator(Map<String, String> byOwnerId) {
    this.byOwnerId = byOwnerId;
  }

  /**
   * Pack owner id + JSON for {@link #add(String)}. Prefer {@link #addSample(String, String)} from
   * Hop code.
   */
  public static String pack(String ownerId, String executionDataJson) {
    if (ownerId == null) {
      ownerId = "";
    }
    if (executionDataJson == null) {
      executionDataJson = "";
    }
    return ownerId + SEP + executionDataJson;
  }

  public void addSample(String ownerId, String executionDataJson) {
    add(pack(ownerId, executionDataJson));
  }

  @Override
  public boolean isZero() {
    synchronized (byOwnerId) {
      return byOwnerId.isEmpty();
    }
  }

  @Override
  public AccumulatorV2<String, Map<String, String>> copy() {
    synchronized (byOwnerId) {
      return new SparkExecutionDataAccumulator(new HashMap<>(byOwnerId));
    }
  }

  @Override
  public void reset() {
    synchronized (byOwnerId) {
      byOwnerId.clear();
    }
  }

  @Override
  public void add(String v) {
    if (v == null || v.isEmpty()) {
      return;
    }
    int idx = v.indexOf(SEP);
    if (idx < 0) {
      return;
    }
    String ownerId = v.substring(0, idx);
    String json = v.substring(idx + 1);
    if (ownerId.isEmpty() || json.isEmpty()) {
      return;
    }
    synchronized (byOwnerId) {
      byOwnerId.put(ownerId, json);
    }
  }

  @Override
  public void merge(AccumulatorV2<String, Map<String, String>> other) {
    if (other == null) {
      return;
    }
    Map<String, String> otherMap = other.value();
    if (otherMap == null || otherMap.isEmpty()) {
      return;
    }
    synchronized (byOwnerId) {
      byOwnerId.putAll(otherMap);
    }
  }

  @Override
  public Map<String, String> value() {
    synchronized (byOwnerId) {
      if (byOwnerId.isEmpty()) {
        return Collections.emptyMap();
      }
      return Collections.unmodifiableMap(new HashMap<>(byOwnerId));
    }
  }
}
