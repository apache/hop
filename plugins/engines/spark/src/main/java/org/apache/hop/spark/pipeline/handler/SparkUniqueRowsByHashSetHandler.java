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
import org.apache.hop.pipeline.transforms.uniquerowsbyhashset.UniqueRowsByHashSetMeta;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Unique Rows By Hashset on several partitions. Spark brings the rows with the same values in the
 * compare fields (all fields when none are set) to one partition, in the order they came in. Hop's
 * own transform then keeps its hash set per partition: a set of duplicates never spans two
 * partitions, and the first of them in the stream is the one kept, as on the local engine. Rejected
 * duplicates go to the error hop as they do there.
 */
public class SparkUniqueRowsByHashSetHandler extends SparkBaseTransformHandler {

  /** Column that keeps the order the rows came in while they are shuffled. */
  static final String ROW_NR_COLUMN = "__hop_unique_rows_row_nr";

  private final SparkGenericTransformHandler genericHandler;

  public SparkUniqueRowsByHashSetHandler(SparkGenericTransformHandler genericHandler) {
    this.genericHandler = genericHandler;
  }

  /** The duplicates are rows of Hop's own transform: they can go to an error hop. */
  @Override
  public boolean supportsErrorHops() {
    return true;
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
      throw new HopException(
          "Unique Rows By Hashset '" + transformMeta.getName() + "' has no input");
    }
    List<String> keys =
        compareFields(transformMeta(UniqueRowsByHashSetMeta.class, transformMeta, pipelineMeta));
    if (keys.isEmpty()) {
      keys = List.of(rowMeta.getFieldNames());
    }
    List<Column> keyColumns = new ArrayList<>();
    for (String key : keys) {
      if (rowMeta.indexOfValue(key) < 0) {
        throw new HopException(
            "Unique Rows By Hashset '"
                + transformMeta.getName()
                + "': compare field '"
                + key
                + "' is not in the input");
      }
      keyColumns.add(column(key));
    }

    Dataset<Row> placed =
        input
            .withColumn(ROW_NR_COLUMN, functions.monotonically_increasing_id())
            .repartition(keyColumns.toArray(new Column[0]))
            .sortWithinPartitions(functions.col(ROW_NR_COLUMN))
            .drop(ROW_NR_COLUMN);
    log.logBasic(
        "Unique Rows By Hashset '"
            + transformMeta.getName()
            + "': rows shuffled on "
            + keys
            + ", a hash set per partition");

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
        placed);
  }

  /** The compare fields of the transform: none means all fields. */
  static List<String> compareFields(UniqueRowsByHashSetMeta meta) {
    List<String> keys = new ArrayList<>();
    for (UniqueRowsByHashSetMeta.CompareField field : meta.getCompareFields()) {
      String name = field.getName();
      if (name != null && !name.isEmpty()) {
        keys.add(name);
      }
    }
    return keys;
  }

  private static Column column(String name) {
    return functions.col("`" + name.replace("`", "``") + "`");
  }
}
