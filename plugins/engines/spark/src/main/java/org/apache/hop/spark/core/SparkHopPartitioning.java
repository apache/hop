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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.HashPartitioner;
import org.apache.hop.pipeline.IPartitioner;
import org.apache.hop.pipeline.ModPartitioner;
import org.apache.hop.pipeline.RebalancePartitioner;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * A partitioned Hop transform on Spark: its input is shuffled so that rows with the same key are on
 * the same Spark partition (Remainder of division, Hash on fields), or spread evenly (Rebalance).
 * Spark picks the number of partitions ({@code spark.sql.shuffle.partitions}, adaptive execution):
 * the partition schema sizes threads on one machine, not a cluster.
 */
public final class SparkHopPartitioning {

  /** Set on the variables of a partitioned transform's mini-pipeline. */
  public static final String VAR_PARTITIONED = "Internal.Spark.Partitioned";

  private static final String REBALANCE = "rebalance";

  private SparkHopPartitioning() {}

  /**
   * How the transform's rows are placed: {@code hash[fields]} or {@code rebalance}. Two transforms
   * with the same placement hand on their rows partition by partition, without a shuffle.
   */
  public static String placement(TransformMeta transformMeta) throws HopException {
    TransformPartitioningMeta partitioning = transformMeta.getTransformPartitioningMeta();
    IPartitioner partitioner = partitioning.getPartitioner();
    if (partitioning.getMethodType() == TransformPartitioningMeta.PARTITIONING_METHOD_SPECIAL) {
      if (partitioner instanceof RebalancePartitioner) {
        return REBALANCE;
      }
      List<String> fields = keyFields(partitioner);
      if (fields != null) {
        if (fields.isEmpty()) {
          throw new HopException(
              "Transform '" + transformMeta.getName() + "' is partitioned on no field");
        }
        return "hash" + fields;
      }
    }
    throw new HopException(
        "Transform '"
            + transformMeta.getName()
            + "' is partitioned with '"
            + partitioning.getMethodDescription()
            + "', which native Spark does not run: use Remainder of division, Hash on fields or"
            + " Rebalance");
  }

  /** The fields a key partitioner hashes on, or null for another partitioner. */
  static List<String> keyFields(IPartitioner partitioner) {
    if (partitioner instanceof ModPartitioner mod) {
      return StringUtils.isEmpty(mod.getFieldName()) ? List.of() : List.of(mod.getFieldName());
    }
    if (partitioner instanceof HashPartitioner hash) {
      return hash.getFieldNames();
    }
    return null;
  }

  /** The input, shuffled the way the transform is partitioned. */
  public static Dataset<Row> repartition(Dataset<Row> input, TransformMeta transformMeta)
      throws HopException {
    String placement = placement(transformMeta);
    if (REBALANCE.equals(placement)) {
      // With adaptive execution Spark also splits skewed partitions and merges small ones; without
      // it the hint is ignored and a round-robin shuffle spreads the rows
      boolean adaptive =
          Boolean.parseBoolean(
              input.sparkSession().conf().get("spark.sql.adaptive.enabled", "true"));
      return adaptive ? input.hint(REBALANCE) : input.repartition(new Column[0]);
    }
    List<String> fields = keyFields(transformMeta.getTransformPartitioningMeta().getPartitioner());
    List<String> columns = Arrays.asList(input.columns());
    List<Column> keys = new ArrayList<>();
    for (String field : fields) {
      if (!columns.contains(field)) {
        throw new HopException(
            "Transform '"
                + transformMeta.getName()
                + "' is partitioned on field '"
                + field
                + "', which is not in its input "
                + columns);
      }
      keys.add(functions.col("`" + field.replace("`", "``") + "`"));
    }
    return input.repartition(keys.toArray(new Column[0]));
  }

  /**
   * {@code Internal.Transform.Partition.NR} ({@code 000}, as the local engine writes it) and {@code
   * Internal.Transform.Partition.ID} of a partitioned transform's mini-pipeline: the Spark
   * partition. Variables of a transform that is not partitioned are left alone.
   */
  public static void applyPartitionVariables(IVariables variables, int partitionNr) {
    if (!"Y".equals(variables.getVariable(VAR_PARTITIONED))) {
      return;
    }
    variables.setVariable(
        Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_NR,
        new DecimalFormat("000").format(partitionNr));
    variables.setVariable(
        Const.INTERNAL_VARIABLE_TRANSFORM_PARTITION_ID, Integer.toString(partitionNr));
  }
}
