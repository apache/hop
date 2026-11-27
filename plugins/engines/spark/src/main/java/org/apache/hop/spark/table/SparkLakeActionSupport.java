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

package org.apache.hop.spark.table;

import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Shared action-sink helpers for lakehouse write/merge/maintenance handlers. Mirrors {@code
 * SparkFileOutputHandler}: run the action during {@code handleTransform}, then register an empty
 * leaf so a later engine {@code count()} does not re-commit.
 */
public final class SparkLakeActionSupport {

  public static final String EMPTY_LEAF_COLUMN = "_spark_hop_written";

  private SparkLakeActionSupport() {}

  /** Empty Dataset registered as the transform output after a successful write action. */
  public static Dataset<Row> emptyLeaf(SparkSession spark) {
    StructType emptySchema = new StructType().add(EMPTY_LEAF_COLUMN, DataTypes.BooleanType, false);
    return spark.createDataFrame(new ArrayList<>(), emptySchema);
  }

  /**
   * Whether a Dataset is the empty leaf of a transform that already wrote: counting it would only
   * start a Spark job that finds no rows.
   */
  public static boolean isEmptyLeaf(Dataset<Row> dataset) {
    String[] columns = dataset.columns();
    return columns.length == 1 && EMPTY_LEAF_COLUMN.equals(columns[0]);
  }

  public static void putEmptyLeaf(
      Map<String, Dataset<Row>> transformDatasetMap, String transformName, SparkSession spark) {
    transformDatasetMap.put(transformName, emptyLeaf(spark));
  }
}
