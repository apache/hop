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
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.table.SparkLakeActionSupport;
import org.apache.hop.spark.table.SparkLakeTableSupport;
import org.apache.hop.spark.table.SparkMergeSqlBuilder;
import org.apache.hop.spark.transforms.table.SparkLakeTableMergeMeta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Native Spark {@code MERGE INTO} against a Delta or Iceberg table. Single upstream Dataset is
 * registered as a temp view; metrics are log-only (no reliable Dataset row counts for merge).
 */
public class SparkLakeTableMergeHandler extends SparkBaseTransformHandler {

  @Override
  public boolean isOutput() {
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
          "Spark Lake Table Merge '"
              + transformMeta.getName()
              + "' requires exactly one upstream Dataset (source rows for USING).");
    }

    SparkLakeTableMergeMeta meta = new SparkLakeTableMergeMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String targetSqlId =
        SparkLakeTableSupport.resolveMergeTargetSqlId(
            spark, variables, meta, transformMeta.getName());
    String sourceView = SparkMergeSqlBuilder.sourceViewName(transformMeta.getName());

    String sql;
    String raw = meta.getRawMergeSql();
    if (StringUtils.isNotEmpty(raw) && StringUtils.isNotBlank(variables.resolve(raw))) {
      sql = variables.resolve(raw).trim();
      // Best-effort: if raw SQL references the default source view name, still register the view
    } else {
      String condition =
          StringUtils.isNotEmpty(meta.getMergeCondition())
              ? variables.resolve(meta.getMergeCondition())
              : null;
      sql =
          SparkMergeSqlBuilder.build(
              targetSqlId,
              sourceView,
              condition,
              meta.getMatchedAction(),
              meta.getNotMatchedAction(),
              meta.getNotMatchedBySourceAction());
    }

    try {
      input.createOrReplaceTempView(sourceView);
      if (log != null) {
        log.logBasic(
            "Spark Lake Table Merge '"
                + transformMeta.getName()
                + "' source view="
                + sourceView
                + " target="
                + targetSqlId);
        log.logBasic("MERGE SQL:\n" + sql);
      }
      spark.sql(sql);
      if (log != null) {
        log.logBasic(
            "Spark Lake Table Merge '" + transformMeta.getName() + "' completed successfully");
      }
    } catch (Exception e) {
      throw new HopException(
          "MERGE failed in transform '"
              + transformMeta.getName()
              + "' against target "
              + targetSqlId
              + ". Ensure connectors and session extensions/catalogs are configured. SQL:\n"
              + sql,
          e);
    } finally {
      try {
        spark.catalog().dropTempView(sourceView);
      } catch (Exception ignored) {
        // best effort cleanup
      }
    }

    SparkLakeActionSupport.putEmptyLeaf(transformDatasetMap, transformMeta.getName(), spark);
  }
}
