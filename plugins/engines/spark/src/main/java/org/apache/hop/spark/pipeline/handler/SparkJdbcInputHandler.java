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
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.apache.hop.pipeline.transforms.tableinput.TableInputPartitioning;
import org.apache.hop.pipeline.transforms.tableinput.TableInputSql;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.core.SparkNativeMetricsListener;
import org.apache.hop.spark.core.SparkTransformMetricsAccumulator;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Native Spark handler for the classic Table Input transform.
 *
 * <p>When the transform is configured with the four parallel-read properties ({@code
 * jdbc_partition_column}, {@code jdbc_lower_bound}, {@code jdbc_upper_bound}, {@code
 * jdbc_num_partitions}), the JDBC connection is read through Spark's partitioned {@code
 * spark.read.jdbc} data source: one disjoint range per Spark task, exactly like the classic engine
 * reads one range per transform copy.
 *
 * <p>Any configuration the native reader cannot reproduce (execute-per-row, effective SQL
 * parameters, row limit, incoming parameter rows) falls back to the generic single-task handler so
 * behavior is unchanged for existing pipelines.
 */
public class SparkJdbcInputHandler extends SparkBaseTransformHandler {

  private final SparkGenericTransformHandler fallback = new SparkGenericTransformHandler();

  @Override
  public void setMetricsAccumulator(SparkTransformMetricsAccumulator metricsAccumulator) {
    super.setMetricsAccumulator(metricsAccumulator);
    // The generic handler runs the fallback path; the converter only propagates metrics to the
    // registered handlers, so this delegate has to be wired here explicitly.
    // Execution-data sampling is only wired to the converter's own generic handler, so sampling
    // coverage of a fallback Table Input is best-effort (the native Spark path has no sampling).
    fallback.setMetricsAccumulator(metricsAccumulator);
  }

  @Override
  public void setMetricsListener(SparkNativeMetricsListener metricsListener) {
    super.setMetricsListener(metricsListener);
  }

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  /**
   * Whether this handler can natively (parallel) read the given Table Input configuration:
   * partitioned read requested, no per-row execution, no JDBC parameters in the effective SQL, no
   * effective row limit, and no incoming parameter rows. Variables must already be the resolved
   * ones.
   *
   * <p>The SQL is inspected directly (named {@code {param}} and positional {@code ?} placeholders
   * both disqualify) rather than relying on the {@code useNamedParameters} flag: the flag only
   * changes how the SQL is prepared, and legacy metadata keeps it unset while the SQL still
   * contains placeholders. A parse failure is treated as a fallback so the classic engine keeps its
   * established behavior.
   *
   * <p>The effective row limit is compared after the same resolution used by the classic engine
   * ({@link org.apache.hop.core.Const#toIntExpanded(String, int)} with default 0 = unlimited), so
   * {@code rowLimit="0"} or a variable resolving to zero still reads the whole table natively.
   */
  public static boolean isNativePartitionedRead(
      TableInputMeta meta, IVariables variables, boolean incomingInput) {
    if (incomingInput
        || meta.isExecuteEachInputRow()
        || !Utils.isEmpty(meta.getLookup())
        || !TableInputPartitioning.isPartitioningConfigured(meta, variables)) {
      return false;
    }
    if (!hasUnlimitedEffectiveRowLimit(meta, variables)) {
      return false;
    }
    // Decide by the actual SQL: named or positional placeholders disqualify.
    try {
      String sql = meta.getEffectiveSql(variables);
      if (meta.isVariableReplacementActive()) {
        sql = variables.resolve(sql);
      }
      // Empty SQL has no bindable parameters but would produce a broken native read (the classic
      // engine reports "no SQL" with the pipeline context); keep the conservative fallback.
      if (Utils.isEmpty(sql)) {
        return false;
      }
      TableInputSql.Parsed parsed = TableInputSql.parse(sql);
      return !parsed.hasNamedParameters() && parsed.getPositionalParameterCount() == 0;
    } catch (HopException e) {
      return false;
    }
  }

  /** The effective row limit is unlimited when empty or resolving to a non-positive number. */
  private static boolean hasUnlimitedEffectiveRowLimit(TableInputMeta meta, IVariables variables) {
    String rowLimit = meta.getRowLimit();
    if (Utils.isEmpty(rowLimit)) {
      return true;
    }
    return Const.toIntExpanded(variables.resolve(rowLimit), 0) <= 0;
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

    TableInputMeta meta = new TableInputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    if (TableInputPartitioning.isPartitioningRequested(meta)
        && !TableInputPartitioning.isPartitioningConfigured(meta, variables)) {
      throw new HopException(
          "Incomplete or invalid JDBC partition configuration: all four options are required.");
    }

    // The four properties must be configured against the ACTUAL variables.
    boolean partitioned = isNativePartitionedRead(meta, variables, input != null);

    if (!partitioned) {
      fallback.handleTransform(
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
          input);
      return;
    }

    Dataset<Row> dataset =
        buildPartitionedRead(meta, variables, metadataProvider, spark, log, transformMeta);
    dataset = trackMetrics(dataset, transformMeta, SparkNativeMetrics.Role.INPUT);
    transformDatasetMap.put(transformMeta.getName(), dataset);

    log.logBasic(
        "Handled Table Input (native partitioned JDBC read) : "
            + transformMeta.getName()
            + " partitions="
            + TableInputPartitioning.resolveNumPartitions(meta, variables));
  }

  private Dataset<Row> buildPartitionedRead(
      TableInputMeta meta,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      SparkSession spark,
      ILogChannel log,
      TransformMeta transformMeta)
      throws HopException {
    org.apache.hop.core.database.DatabaseMeta databaseMeta =
        resolveDatabaseMeta(meta, variables, metadataProvider);

    String sql;
    try {
      sql = meta.getEffectiveSql(variables);
      if (meta.isVariableReplacementActive()) {
        sql = variables.resolve(sql);
      }
    } catch (HopException e) {
      throw new HopException(
          "Could not get SQL for Table Input '" + transformMeta.getName() + "': " + e.getMessage(),
          e);
    }
    sql = TableInputPartitioning.trimSql(sql);

    TableInputPartitioning.TableInputPartitionSlice slice =
        TableInputPartitioning.resolveSlice(meta, variables, databaseMeta, 0);
    if (slice == null) {
      // not configured after resolution with actual variables
      throw new HopException(
          "Table Input '"
              + transformMeta.getName()
              + "': JDBC partitioning is incomplete. Set all four properties "
              + "(jdbc_partition_column, jdbc_lower_bound, jdbc_upper_bound, jdbc_num_partitions).");
    }

    int numPartitions = slice.getNumPartitions();
    long lower = slice.getLower();
    long upper = slice.getUpper();
    long range = upper - lower; // may overflow (MIN..MAX)
    int numPartitionsEffective =
        (numPartitions <= 1 || range == 0)
            ? 1
            : ((range >= numPartitions || range < 0) ? numPartitions : (int) range);

    org.apache.spark.sql.DataFrameReader reader =
        spark
            .read()
            .format("jdbc")
            .option("url", databaseMeta.getURL(variables))
            .option("driver", databaseMeta.getDriverClass(variables))
            .option("dbtable", slice.buildSparkDbtable(sql))
            .option("partitionColumn", slice.getPartitionColumn())
            .option("lowerBound", Long.toString(lower))
            .option("upperBound", Long.toString(upper))
            .option("numPartitions", Integer.toString(numPartitionsEffective));

    String username = databaseMeta.getUsername();
    if (!Utils.isEmpty(username)) {
      reader = reader.option("user", variables.resolve(username));
    }
    // The metadata password can be stored encrypted (Encr); the classic Database decrypts it the
    // same way before connecting, so Spark must receive the plain value.
    String password = databaseMeta.getPassword();
    if (!Utils.isEmpty(password)) {
      reader =
          reader.option(
              "password", Encr.decryptPasswordOptionallyEncrypted(variables.resolve(password)));
    }
    Dataset<Row> dataset = reader.load();

    log.logBasic(
        "Table Input '"
            + transformMeta.getName()
            + "': native JDBC partitioned read with "
            + numPartitionsEffective
            + " partitions, partition column '"
            + slice.getPartitionColumn()
            + "'");
    return dataset;
  }

  private org.apache.hop.core.database.DatabaseMeta resolveDatabaseMeta(
      TableInputMeta meta, IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    String connectionName = variables.resolve(meta.getConnection());
    if (Utils.isEmpty(connectionName)) {
      throw new HopException("Table Input has no database connection configured.");
    }
    org.apache.hop.core.database.DatabaseMeta databaseMeta;
    try {
      databaseMeta =
          metadataProvider
              .getSerializer(org.apache.hop.core.database.DatabaseMeta.class)
              .load(connectionName);
    } catch (Exception e) {
      throw new HopException(
          "Unable to load JDBC connection '" + connectionName + "': " + e.getMessage(), e);
    }
    if (databaseMeta == null) {
      throw new HopException(
          "JDBC connection '" + connectionName + "' not found in the metadata provider.");
    }
    return databaseMeta;
  }
}
