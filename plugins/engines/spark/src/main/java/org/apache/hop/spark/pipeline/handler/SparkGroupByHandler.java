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

package org.apache.hop.spark.pipeline.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupingField;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pipeline.SparkTransformPlan;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Group By on several partitions. Spark brings the rows of every group together: a shuffle on the
 * grouping fields, then the rows of each partition sorted on those fields and, within a group, in
 * the order they came in. Hop's own Group By then aggregates every partition. A group never spans
 * two partitions, so every aggregation type, the cumulative and moving ones, the line number per
 * group and "include all rows" give the local engine's results.
 *
 * <p>On sorted input that is exactly what the local engine does. On unsorted input the local engine
 * starts a new group every time the key changes; here every key gives one group.
 *
 * <p>Without grouping fields, or when an empty input must still give one row, the rows go to one
 * partition: the one result row is computed once.
 */
public class SparkGroupByHandler extends SparkBaseTransformHandler {

  /** Column that keeps the order the rows came in while they are shuffled. */
  static final String ROW_NR_COLUMN = "__hop_group_by_row_nr";

  private final SparkGenericTransformHandler genericHandler;

  public SparkGroupByHandler(SparkGenericTransformHandler genericHandler) {
    this.genericHandler = genericHandler;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      String metastoreJson,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, Dataset<Row>> transformDatasetMap,
      SparkSession spark,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      Dataset<Row> input)
      throws HopException {
    if (input == null) {
      throw new HopException("Group By '" + transformMeta.getName() + "' has no input");
    }
    GroupByMeta meta = transformMeta(GroupByMeta.class, transformMeta, pipelineMeta);
    List<String> keys = groupingFields(meta);
    for (String key : keys) {
      if (rowMeta.indexOfValue(key) < 0) {
        throw new HopException(
            "Group By '"
                + transformMeta.getName()
                + "': grouping field '"
                + key
                + "' is not in the input");
      }
    }

    List<Column> order = new ArrayList<>();
    for (String key : keys) {
      order.add(functions.col(key));
    }
    order.add(functions.col(ROW_NR_COLUMN));
    Dataset<Row> numbered =
        input.withColumn(ROW_NR_COLUMN, functions.monotonically_increasing_id());
    Dataset<Row> grouped;
    if (onOnePartition(meta)) {
      // Exactly one partition, also for an empty input: a shuffle of nothing can end up with no
      // partitions at all (adaptive execution), and then Group By never runs to give its row
      grouped = SparkGenericTransformHandler.onePartition(spark, numbered);
    } else {
      Column[] keyColumns = order.subList(0, keys.size()).toArray(new Column[0]);
      grouped = numbered.repartitionByRange(keyColumns);
    }
    grouped = grouped.sortWithinPartitions(order.toArray(new Column[0])).drop(ROW_NR_COLUMN);
    log.logBasic(
        "Group By '"
            + transformMeta.getName()
            + "': rows shuffled on "
            + (keys.isEmpty() ? "nothing (one partition)" : keys)
            + ", aggregated by Hop per partition");

    genericHandler.handleTransform(
        log,
        variables,
        runConfigurationName,
        runConfiguration,
        metadataProvider,
        metastoreJson,
        pipelineMeta,
        transformMeta,
        transformDatasetMap,
        spark,
        rowMeta,
        previousTransforms,
        grouped);
  }

  @Override
  public SparkTransformPlan plan(
      PipelineMeta pipelineMeta, TransformMeta transformMeta, IVariables variables)
      throws HopException {
    return onOnePartition(transformMeta(GroupByMeta.class, transformMeta, pipelineMeta))
        ? SparkTransformPlan.of(
            SparkTransformPlan.Kind.ONE_STREAM, "SparkTransformPlan.GroupBy.OnePartition")
        : SparkTransformPlan.of(SparkTransformPlan.Kind.HOP, "SparkTransformPlan.GroupBy.ByGroup");
  }

  /** Without grouping fields, or when an empty input must still give one row: one partition. */
  static boolean onOnePartition(GroupByMeta meta) {
    return groupingFields(meta).isEmpty() || meta.isAlwaysGivingBackOneRow();
  }

  static List<String> groupingFields(GroupByMeta meta) {
    List<String> keys = new ArrayList<>();
    for (GroupingField field : meta.getGroupingFields()) {
      String name = field.getName();
      if (name != null && !name.isEmpty()) {
        keys.add(name);
      }
    }
    return keys;
  }
}
