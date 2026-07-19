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
import org.apache.hop.spark.table.SparkLakeTableSupport.MaintenanceTarget;
import org.apache.hop.spark.table.SparkMaintenanceSqlBuilder;
import org.apache.hop.spark.transforms.table.SparkLakeTableMaintenanceMeta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Zero-input lakehouse maintenance action (KD-20). Upstream Dataset is ignored if present. Metrics
 * are log-only.
 */
public class SparkLakeTableMaintenanceHandler extends SparkBaseTransformHandler {

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

    // Zero-input: ignore upstream if hop-connected (KD-20)
    SparkLakeTableMaintenanceMeta meta = new SparkLakeTableMaintenanceMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String operation =
        StringUtils.isNotEmpty(meta.getOperation())
            ? variables.resolve(meta.getOperation())
            : meta.getOperation();

    if (SparkMaintenanceSqlBuilder.requiresDestructiveAck(operation)
        && !meta.isAcknowledgeDestructive()) {
      throw new HopException(
          "Spark Lake Table Maintenance '"
              + transformMeta.getName()
              + "' operation "
              + operation
              + " is destructive. Check 'I understand this may delete data files / snapshots'"
              + " (acknowledgeDestructive) before running.");
    }

    String pathSchemeMap = runConfiguration != null ? runConfiguration.getPathSchemeMap() : null;
    MaintenanceTarget target =
        SparkLakeTableSupport.resolveMaintenanceTarget(
            spark, variables, meta, transformMeta.getName(), pathSchemeMap);

    String retention =
        StringUtils.isNotEmpty(meta.getRetentionHours())
            ? variables.resolve(meta.getRetentionHours())
            : null;
    String retainLast =
        StringUtils.isNotEmpty(meta.getRetainLast())
            ? variables.resolve(meta.getRetainLast())
            : null;
    String where =
        StringUtils.isNotEmpty(meta.getWhereClause())
            ? variables.resolve(meta.getWhereClause())
            : null;
    String zorder =
        StringUtils.isNotEmpty(meta.getZOrderColumns())
            ? variables.resolve(meta.getZOrderColumns())
            : null;

    String sql =
        SparkMaintenanceSqlBuilder.build(
            target.format(),
            target.targetSqlId(),
            target.procedureCatalog(),
            target.tableRefForCall(),
            operation,
            retention,
            where,
            zorder,
            retainLast);

    try {
      if (log != null) {
        log.logBasic(
            "Spark Lake Table Maintenance '"
                + transformMeta.getName()
                + "' op="
                + operation
                + " target="
                + target.targetSqlId());
        log.logBasic("Maintenance SQL:\n" + sql);
      }
      spark.sql(sql);
      if (log != null) {
        log.logBasic(
            "Spark Lake Table Maintenance '"
                + transformMeta.getName()
                + "' completed successfully");
      }
    } catch (Exception e) {
      throw new HopException(
          "Maintenance failed in transform '"
              + transformMeta.getName()
              + "' ("
              + operation
              + "). SQL:\n"
              + sql,
          e);
    }

    SparkLakeActionSupport.putEmptyLeaf(transformDatasetMap, transformMeta.getName(), spark);
  }
}
