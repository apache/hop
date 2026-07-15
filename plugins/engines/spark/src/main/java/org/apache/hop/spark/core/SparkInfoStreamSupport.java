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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Collects an info/side Dataset into Hop rows and broadcasts it to executors (Beam {@code
 * View.asList} analogue).
 *
 * <p>Info streams must fit in memory on the driver and on each executor that receives the
 * broadcast.
 */
public final class SparkInfoStreamSupport {

  /** Soft guard: fail fast if an info stream is unexpectedly huge. 0 = unlimited. */
  public static final int DEFAULT_MAX_INFO_ROWS = 5_000_000;

  private SparkInfoStreamSupport() {}

  /**
   * Collect {@code infoDataset} as Hop {@code Object[]} rows and broadcast them.
   *
   * @param spark active session
   * @param infoDataset source Dataset (already computed in the transform graph)
   * @param rowMeta Hop row metadata matching the Dataset columns
   * @param transformName name of the info source (for error messages)
   * @param maxRows maximum rows to collect; use {@link #DEFAULT_MAX_INFO_ROWS} or {@literal <=0}
   *     for unlimited
   * @return broadcast of Hop rows (empty list if Dataset has no rows)
   */
  public static Broadcast<List<Object[]>> broadcastInfoRows(
      SparkSession spark,
      Dataset<Row> infoDataset,
      IRowMeta rowMeta,
      String transformName,
      int maxRows)
      throws HopException {
    if (spark == null || infoDataset == null) {
      throw new HopException(
          "Cannot broadcast info stream for transform '"
              + transformName
              + "': null session/dataset");
    }
    if (rowMeta == null) {
      throw new HopException(
          "Cannot broadcast info stream for transform '" + transformName + "': null row meta");
    }

    List<Row> sparkRows = infoDataset.collectAsList();
    if (maxRows > 0 && sparkRows.size() > maxRows) {
      throw new HopException(
          "Info stream from transform '"
              + transformName
              + "' has "
              + sparkRows.size()
              + " rows which exceeds the maximum allowed ("
              + maxRows
              + "). Reduce the lookup size or raise the limit.");
    }

    List<Object[]> hopRows = new ArrayList<>(sparkRows.size());
    for (Row sparkRow : sparkRows) {
      hopRows.add(HopSparkRowConverter.toHopRow(rowMeta, sparkRow));
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    return jsc.broadcast(hopRows);
  }

  public static Broadcast<List<Object[]>> broadcastInfoRows(
      SparkSession spark, Dataset<Row> infoDataset, IRowMeta rowMeta, String transformName)
      throws HopException {
    return broadcastInfoRows(spark, infoDataset, rowMeta, transformName, DEFAULT_MAX_INFO_ROWS);
  }
}
