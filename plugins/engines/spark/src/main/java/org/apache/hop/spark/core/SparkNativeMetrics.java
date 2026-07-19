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

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Objects;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Instruments native Spark {@link Dataset} stages so row flow is visible in Hop {@code
 * EngineMetrics}. Inserts a pass-through {@code mapPartitions} that reports absolute counters into
 * the same {@link SparkTransformMetricsAccumulator} used by {@link HopMapPartitionsFn}.
 *
 * <p>Counters are only updated when the lineage is materialized (an action). Per-partition {@code
 * copyNr} matches {@link TaskContext#partitionId()}.
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

  private static final int ROW_INTERVAL = 1000;
  private static final long TIME_INTERVAL_MS = 1000L;

  private SparkNativeMetrics() {}

  /**
   * Wrap {@code dataset} so that when it is computed, each partition reports metrics for {@code
   * transformName}. Returns {@code dataset} unchanged when accumulator or name is null.
   */
  public static Dataset<Row> track(
      Dataset<Row> dataset,
      String transformName,
      SparkTransformMetricsAccumulator accumulator,
      Role role) {
    if (dataset == null
        || accumulator == null
        || transformName == null
        || transformName.isEmpty()) {
      return dataset;
    }
    Role effectiveRole = role != null ? role : Role.TRANSFORM;
    StructType schema = dataset.schema();
    JavaRDD<Row> tracked =
        dataset
            .toJavaRDD()
            .mapPartitions(
                iterator ->
                    new CountingIterator(iterator, transformName, accumulator, effectiveRole),
                true);
    return dataset.sparkSession().createDataFrame(tracked, schema);
  }

  static final class CountingIterator implements Iterator<Row>, Serializable {
    private static final long serialVersionUID = 1L;

    private final Iterator<Row> source;
    private final String transformName;
    private final SparkTransformMetricsAccumulator accumulator;
    private final Role role;
    private final int copyNr;
    private final String host;

    private long count;
    private long partitionStartMs;
    private long lastPublishMs;
    private boolean finishedPublished;
    private boolean startedPublished;

    CountingIterator(
        Iterator<Row> source,
        String transformName,
        SparkTransformMetricsAccumulator accumulator,
        Role role) {
      this.source = Objects.requireNonNull(source, "source");
      this.transformName = transformName;
      this.accumulator = accumulator;
      this.role = role;
      this.copyNr = partitionId();
      this.host = localHost();
    }

    @Override
    public boolean hasNext() {
      ensureStarted();
      if (source.hasNext()) {
        return true;
      }
      publishFinished();
      return false;
    }

    @Override
    public Row next() {
      ensureStarted();
      Row row = source.next();
      count++;
      if (shouldPublishProgress()) {
        publish(true, false);
      }
      return row;
    }

    private void ensureStarted() {
      if (!startedPublished) {
        startedPublished = true;
        partitionStartMs = System.currentTimeMillis();
        lastPublishMs = partitionStartMs;
        publish(true, false);
      }
    }

    private boolean shouldPublishProgress() {
      if (count > 0 && count % ROW_INTERVAL == 0) {
        return true;
      }
      long now = System.currentTimeMillis();
      if (now - lastPublishMs >= TIME_INTERVAL_MS) {
        lastPublishMs = now;
        return true;
      }
      return false;
    }

    private void publishFinished() {
      if (!finishedPublished) {
        finishedPublished = true;
        publish(false, true);
      }
    }

    private void publish(boolean running, boolean finished) {
      long read = 0;
      long written = 0;
      long input = 0;
      long output = 0;
      switch (role) {
        case INPUT:
          input = count;
          written = count;
          break;
        case OUTPUT:
          read = count;
          output = count;
          break;
        case TRANSFORM:
        default:
          read = count;
          written = count;
          break;
      }
      long endTimeMs = finished ? System.currentTimeMillis() : 0L;
      accumulator.add(
          new SparkTransformMetricSlice(
              transformName,
              copyNr,
              host,
              read,
              written,
              input,
              output,
              0,
              running,
              finished,
              partitionStartMs,
              endTimeMs));
    }

    private static int partitionId() {
      TaskContext ctx = TaskContext.get();
      return ctx != null ? ctx.partitionId() : 0;
    }

    private static String localHost() {
      try {
        return InetAddress.getLocalHost().getHostName();
      } catch (Exception e) {
        return null;
      }
    }
  }
}
