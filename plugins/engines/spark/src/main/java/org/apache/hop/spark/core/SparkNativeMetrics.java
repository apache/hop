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

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Instruments native Spark {@link Dataset} stages so row flow is visible in Hop {@code
 * EngineMetrics}.
 *
 * <p>Each tracked stage gets a named {@code CollectMetrics} node ({@link Dataset#observe}) that
 * stays inside the Catalyst plan: no RDD round-trip, so whole-stage codegen, column pruning and
 * filter pushdown are preserved. The counts themselves are read from Spark's own per-task SQL
 * metrics by {@link SparkNativeMetricsListener}, which attributes them to the transform via the
 * anchor and reports partition / host / timing from the task info.
 */
public final class SparkNativeMetrics {

  /** How counters are attributed for a native Dataset stage. */
  public enum Role {
    /** Source / file input: physical input + rows produced. */
    INPUT,
    /** Sink / file output: rows consumed + physical output. */
    OUTPUT,
    /** Intermediate Dataset op: rows in and out of the stage (output cardinality). */
    TRANSFORM
  }

  private SparkNativeMetrics() {}

  /**
   * Anchor {@code dataset} so that its row count is attributed to {@code transformName} by the
   * listener. Returns {@code dataset} unchanged when listener or name is null.
   */
  public static Dataset<Row> track(
      Dataset<Row> dataset, String transformName, SparkNativeMetricsListener listener, Role role) {
    if (dataset == null || listener == null || transformName == null || transformName.isEmpty()) {
      return dataset;
    }
    String token = listener.register(transformName, role != null ? role : Role.TRANSFORM);
    return dataset.observe(token, count(lit(1)).alias("rows"));
  }
}
