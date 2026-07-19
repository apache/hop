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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lower;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.uniquerows.UniqueField;
import org.apache.hop.pipeline.transforms.uniquerows.UniqueRowsMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Native Spark distinct / dropDuplicates for Unique Rows. Count of duplicates is supported via
 * groupBy; error-handling reject mode is not supported.
 */
public class SparkUniqueRowsHandler extends SparkBaseTransformHandler {

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
          "Unique Rows transform '" + transformMeta.getName() + "' has no input Dataset");
    }

    UniqueRowsMeta meta = new UniqueRowsMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    if (meta.isRejectDuplicateRow()) {
      throw new HopException(
          "Unique Rows '"
              + transformMeta.getName()
              + "': reject duplicate rows (error handling) is not supported on the native Spark engine");
    }

    List<UniqueField> compareFields = meta.getCompareFields();
    List<String> keyNames = new ArrayList<>();
    if (compareFields != null) {
      for (UniqueField field : compareFields) {
        if (StringUtils.isNotEmpty(field.getName())) {
          keyNames.add(field.getName());
        }
      }
    }

    Dataset<Row> output;
    if (meta.isCountRows()) {
      output = distinctWithCount(input, meta, keyNames, rowMeta);
    } else if (keyNames.isEmpty()) {
      output = input.dropDuplicates();
    } else if (hasCaseInsensitive(compareFields)) {
      output = dropDuplicatesCaseInsensitive(input, compareFields, keyNames);
    } else {
      output = input.dropDuplicates(keyNames.toArray(new String[0]));
    }

    output = trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM);
    transformDatasetMap.put(transformMeta.getName(), output);
    log.logBasic(
        "Handled Unique Rows (native Spark) : "
            + transformMeta.getName()
            + " keys="
            + (keyNames.isEmpty() ? "(all columns)" : keyNames)
            + " countRows="
            + meta.isCountRows());
  }

  private static boolean hasCaseInsensitive(List<UniqueField> fields) {
    if (fields == null) {
      return false;
    }
    for (UniqueField f : fields) {
      if (f.isCaseInsensitive()) {
        return true;
      }
    }
    return false;
  }

  private static Dataset<Row> dropDuplicatesCaseInsensitive(
      Dataset<Row> input, List<UniqueField> compareFields, List<String> keyNames) {
    // Normalize case-insensitive keys into temp columns, dropDuplicates, drop temps
    Dataset<Row> work = input;
    List<String> dedupeKeys = new ArrayList<>();
    List<String> tempCols = new ArrayList<>();
    for (UniqueField field : compareFields) {
      if (StringUtils.isEmpty(field.getName())) {
        continue;
      }
      if (field.isCaseInsensitive()) {
        String temp = "__ci__" + field.getName();
        work = work.withColumn(temp, lower(col(field.getName())));
        dedupeKeys.add(temp);
        tempCols.add(temp);
      } else {
        dedupeKeys.add(field.getName());
      }
    }
    work = work.dropDuplicates(dedupeKeys.toArray(new String[0]));
    for (String temp : tempCols) {
      work = work.drop(temp);
    }
    return work;
  }

  private static Dataset<Row> distinctWithCount(
      Dataset<Row> input, UniqueRowsMeta meta, List<String> keyNames, IRowMeta rowMeta)
      throws HopException {
    String countField =
        StringUtils.isNotEmpty(meta.getCountField()) ? meta.getCountField() : "count";

    if (keyNames.isEmpty()) {
      // Count duplicates across entire row
      if (rowMeta == null || rowMeta.size() == 0) {
        throw new HopException("Unique Rows with count requires known input fields");
      }
      for (int i = 0; i < rowMeta.size(); i++) {
        keyNames.add(rowMeta.getValueMeta(i).getName());
      }
    }

    Column[] groupCols = keyNames.stream().map(f -> col(f)).toArray(Column[]::new);
    // Hop unique with count keeps one full row + the number of occurrences.
    Dataset<Row> counts =
        input
            .groupBy(groupCols)
            .agg(count(org.apache.spark.sql.functions.lit(1)).alias(countField));
    Dataset<Row> uniqueRows = input.dropDuplicates(keyNames.toArray(new String[0]));
    return uniqueRows.join(counts, keyNames.toArray(new String[0]), "inner");
  }
}
