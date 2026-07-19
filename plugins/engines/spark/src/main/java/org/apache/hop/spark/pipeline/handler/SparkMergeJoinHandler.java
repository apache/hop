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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mergejoin.MergeJoinMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Native Spark Dataset join for the Merge Join transform. */
public class SparkMergeJoinHandler extends SparkBaseTransformHandler {

  private static final String RIGHT_PREFIX = "__spark_r__";

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

    MergeJoinMeta meta = new MergeJoinMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String leftName = resolveLeftTransformName(meta, previousTransforms);
    String rightName = resolveRightTransformName(meta, previousTransforms);

    Dataset<Row> left = transformDatasetMap.get(leftName);
    Dataset<Row> right = transformDatasetMap.get(rightName);
    if (left == null) {
      throw new HopException(
          "Merge Join '"
              + transformMeta.getName()
              + "': left Dataset for transform '"
              + leftName
              + "' not found");
    }
    if (right == null) {
      throw new HopException(
          "Merge Join '"
              + transformMeta.getName()
              + "': right Dataset for transform '"
              + rightName
              + "' not found");
    }

    List<String> leftKeys = meta.getKeyFields1();
    List<String> rightKeys = meta.getKeyFields2();
    if (leftKeys == null || rightKeys == null || leftKeys.isEmpty() || rightKeys.isEmpty()) {
      throw new HopException(
          "Merge Join '" + transformMeta.getName() + "' requires key fields on both sides");
    }
    if (leftKeys.size() != rightKeys.size()) {
      throw new HopException(
          "Merge Join '"
              + transformMeta.getName()
              + "' key field counts differ: left="
              + leftKeys.size()
              + " right="
              + rightKeys.size());
    }

    // Prefix right columns to avoid name collisions after join
    Dataset<Row> rightPrefixed = right;
    for (String column : right.columns()) {
      rightPrefixed = rightPrefixed.withColumnRenamed(column, RIGHT_PREFIX + column);
    }

    Column joinCond = null;
    for (int i = 0; i < leftKeys.size(); i++) {
      Column part =
          left.col(leftKeys.get(i)).equalTo(rightPrefixed.col(RIGHT_PREFIX + rightKeys.get(i)));
      joinCond = joinCond == null ? part : joinCond.and(part);
    }

    String sparkJoinType = toSparkJoinType(meta.getJoinType());
    Dataset<Row> joined = left.join(rightPrefixed, joinCond, sparkJoinType);

    // Build output columns: all left columns + right columns (stripped prefix).
    // For duplicate names, keep left and rename right with _1 style only if needed.
    Set<String> usedNames = new HashSet<>(Arrays.asList(left.columns()));
    List<Column> selectCols = new ArrayList<>();
    for (String c : left.columns()) {
      selectCols.add(col(c));
    }
    for (String c : right.columns()) {
      String prefixed = RIGHT_PREFIX + c;
      String outName = c;
      if (usedNames.contains(outName)) {
        outName = c + "_1";
        int n = 1;
        while (usedNames.contains(outName)) {
          n++;
          outName = c + "_" + n;
        }
      }
      usedNames.add(outName);
      selectCols.add(col(prefixed).alias(outName));
    }

    Dataset<Row> output = joined.select(selectCols.toArray(new Column[0]));
    output = trackMetrics(output, transformMeta, SparkNativeMetrics.Role.TRANSFORM);
    transformDatasetMap.put(transformMeta.getName(), output);
    log.logBasic(
        "Handled Merge Join (native Spark) : "
            + transformMeta.getName()
            + " type="
            + meta.getJoinType()
            + " left="
            + leftName
            + " right="
            + rightName);
  }

  private static String toSparkJoinType(String hopJoinType) throws HopException {
    if (StringUtils.isEmpty(hopJoinType)) {
      throw new HopException("Merge Join type is not set");
    }
    return switch (hopJoinType.trim().toUpperCase()) {
      case "INNER" -> "inner";
      case "LEFT OUTER" -> "left_outer";
      case "RIGHT OUTER" -> "right_outer";
      case "FULL OUTER" -> "full_outer";
      default ->
          throw new HopException("Join type '" + hopJoinType + "' is not recognized or supported");
    };
  }

  private static String resolveLeftTransformName(
      MergeJoinMeta meta, List<TransformMeta> previousTransforms) throws HopException {
    if (StringUtils.isNotEmpty(meta.getLeftTransformName())) {
      return meta.getLeftTransformName();
    }
    if (meta.getTransformIOMeta().getInfoStreams().size() >= 1
        && meta.getTransformIOMeta().getInfoStreams().get(0).getTransformMeta() != null) {
      return meta.getTransformIOMeta().getInfoStreams().get(0).getTransformMeta().getName();
    }
    if (previousTransforms != null && !previousTransforms.isEmpty()) {
      return previousTransforms.get(0).getName();
    }
    throw new HopException("Unable to resolve left transform for Merge Join");
  }

  private static String resolveRightTransformName(
      MergeJoinMeta meta, List<TransformMeta> previousTransforms) throws HopException {
    if (StringUtils.isNotEmpty(meta.getRightTransformName())) {
      return meta.getRightTransformName();
    }
    if (meta.getTransformIOMeta().getInfoStreams().size() >= 2
        && meta.getTransformIOMeta().getInfoStreams().get(1).getTransformMeta() != null) {
      return meta.getTransformIOMeta().getInfoStreams().get(1).getTransformMeta().getName();
    }
    if (previousTransforms != null && previousTransforms.size() >= 2) {
      return previousTransforms.get(1).getName();
    }
    throw new HopException("Unable to resolve right transform for Merge Join");
  }
}
