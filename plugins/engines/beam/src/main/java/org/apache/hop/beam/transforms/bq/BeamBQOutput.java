/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.transforms.bq;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Local-engine implementation of the BigQuery Output transform. Uses the standard
 * google-cloud-bigquery Java client (already on the Beam plugin classpath) to stream rows to a
 * BigQuery table via {@code insertAll}. Configuration mirrors the Beam runtime side: optional
 * project id, dataset + table, plus the three boolean flags create-if-needed, truncate-table, and
 * fail-if-not-empty.
 *
 * <p>Rows are buffered into batches (default 500) and flushed when the batch is full or on {@link
 * #dispose}. Schema is derived from the input row meta on the first row (HOP type → BQ
 * StandardSQLTypeName via {@link #mapType}); the table is created, truncated, or rejected as the
 * meta flags dictate at that point.
 *
 * <p>This is a leaf transform on the Hop engine — input rows are not forwarded downstream
 * (mirroring the BigQuery Output role) but {@code BeamBQOutputMeta.getFields} leaves the row meta
 * untouched so chaining additional transforms after it works exactly like {@code TableOutput}.
 */
public class BeamBQOutput extends BaseTransform<BeamBQOutputMeta, BeamBQOutputData> {

  public BeamBQOutput(
      TransformMeta transformMeta,
      BeamBQOutputMeta meta,
      BeamBQOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    String project = resolve(meta.getProjectId());
    data.bigquery =
        StringUtils.isNotEmpty(project)
            ? BigQueryOptions.newBuilder().setProjectId(project).build().getService()
            : BigQueryOptions.getDefaultInstance().getService();
    data.batch = new ArrayList<>();
    data.batchSize = 500;
    data.tableReady = false;
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      flushBatch();
      setOutputDone();
      return false;
    }

    if (!data.tableReady) {
      data.inputRowMeta = getInputRowMeta().clone();
      prepareTable();
      data.tableReady = true;
    }

    data.batch.add(toInsertRow(data.inputRowMeta, row));

    if (data.batch.size() >= data.batchSize) {
      flushBatch();
    }

    incrementLinesOutput();
    putRow(getInputRowMeta(), row);

    if (isStopped()) {
      setOutputDone();
      return false;
    }
    return true;
  }

  @Override
  public void dispose() {
    try {
      if (data.batch != null && !data.batch.isEmpty()) {
        flushBatch();
      }
    } catch (Exception e) {
      logError("Error flushing final BigQuery batch", e);
    }
    super.dispose();
  }

  private void prepareTable() throws HopException {
    String project = resolve(meta.getProjectId());
    String dataset = resolve(meta.getDatasetId());
    String tableName = resolve(meta.getTableId());

    if (StringUtils.isEmpty(dataset) || StringUtils.isEmpty(tableName)) {
      throw new HopException("Dataset and table id are required");
    }

    data.tableId =
        StringUtils.isNotEmpty(project)
            ? TableId.of(project, dataset, tableName)
            : TableId.of(dataset, tableName);

    Table existing = data.bigquery.getTable(data.tableId);

    if (existing == null) {
      if (!meta.isCreatingIfNeeded()) {
        throw new HopException(
            "Table '"
                + dataset
                + "."
                + tableName
                + "' does not exist and 'create if needed' is disabled");
      }
      Schema schema = buildSchema(data.inputRowMeta);
      // schema is built from the input row meta via the same package-private helper unit-tested
      // in BeamBQOutputTest — the instance and the test agree on the type mapping.
      data.bigquery.create(TableInfo.of(data.tableId, StandardTableDefinition.of(schema)));
      logBasic("Created BigQuery table " + dataset + "." + tableName);
      return;
    }

    if (meta.isFailingIfNotEmpty()) {
      BigInteger numRows = existing.getNumRows();
      if (numRows != null && numRows.signum() > 0) {
        throw new HopException(
            "Table '"
                + dataset
                + "."
                + tableName
                + "' has "
                + numRows
                + " rows and 'fail if not empty' is enabled");
      }
    }

    if (meta.isTruncatingTable()) {
      truncateTable(project, dataset, tableName);
    }
  }

  private void truncateTable(String project, String dataset, String tableName) throws HopException {
    String fqn =
        StringUtils.isNotEmpty(project)
            ? String.format("`%s.%s.%s`", project, dataset, tableName)
            : String.format("`%s.%s`", dataset, tableName);
    // TRUNCATE TABLE is DDL — free (no bytes-processed bill), no DML quota, instant, and
    // preserves schema/partitioning/clustering. DELETE FROM ... WHERE TRUE would also work but
    // is DML: slower, billed, and limited to ~1000 DML statements per table per day.
    String sql = "TRUNCATE TABLE " + fqn;
    QueryJobConfiguration cfg =
        QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build();
    try {
      Job job = data.bigquery.create(JobInfo.newBuilder(cfg).build());
      job = job.waitFor();
      if (job == null) {
        throw new HopException("Truncate of " + fqn + " failed: job disappeared");
      }
      if (job.getStatus() != null && job.getStatus().getError() != null) {
        throw new HopException("Truncate of " + fqn + " failed: " + job.getStatus().getError());
      }
      logBasic("Truncated " + fqn);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Truncate of " + fqn + " interrupted", e);
    }
  }

  @VisibleForTesting
  static Schema buildSchema(IRowMeta rowMeta) {
    java.util.List<Field> fields = new ArrayList<>();
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta vm = rowMeta.getValueMeta(i);
      fields.add(Field.newBuilder(vm.getName(), mapType(vm)).setMode(Field.Mode.NULLABLE).build());
    }
    return Schema.of(fields);
  }

  /** Hop value-meta type → BigQuery StandardSQLTypeName. Unknown types fall back to STRING. */
  @VisibleForTesting
  static StandardSQLTypeName mapType(IValueMeta vm) {
    return switch (vm.getType()) {
      case IValueMeta.TYPE_STRING -> StandardSQLTypeName.STRING;
      case IValueMeta.TYPE_INTEGER -> StandardSQLTypeName.INT64;
      case IValueMeta.TYPE_NUMBER -> StandardSQLTypeName.FLOAT64;
      case IValueMeta.TYPE_BOOLEAN -> StandardSQLTypeName.BOOL;
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> StandardSQLTypeName.TIMESTAMP;
      case IValueMeta.TYPE_BIGNUMBER -> StandardSQLTypeName.NUMERIC;
      case IValueMeta.TYPE_BINARY -> StandardSQLTypeName.BYTES;
      default -> StandardSQLTypeName.STRING;
    };
  }

  @VisibleForTesting
  static InsertAllRequest.RowToInsert toInsertRow(IRowMeta rowMeta, Object[] row)
      throws HopException {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta vm = rowMeta.getValueMeta(i);
      Object value = row[i];
      if (value == null) {
        continue;
      }
      try {
        Object bq =
            switch (vm.getType()) {
              case IValueMeta.TYPE_STRING -> vm.getString(value);
              case IValueMeta.TYPE_INTEGER -> vm.getInteger(value);
              case IValueMeta.TYPE_NUMBER -> vm.getNumber(value);
              case IValueMeta.TYPE_BOOLEAN -> vm.getBoolean(value);
              case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> {
                Date d = vm.getDate(value);
                yield d == null ? null : Instant.ofEpochMilli(d.getTime()).toString();
              }
              case IValueMeta.TYPE_BIGNUMBER -> {
                BigDecimal bd = vm.getBigNumber(value);
                yield bd == null ? null : bd.toPlainString();
              }
              case IValueMeta.TYPE_BINARY ->
                  Base64.getEncoder().encodeToString(vm.getBinary(value));
              default -> vm.getString(value);
            };
        if (bq != null) {
          map.put(vm.getName(), bq);
        }
      } catch (HopException e) {
        throw new HopException("Failed converting field '" + vm.getName() + "'", e);
      }
    }
    return InsertAllRequest.RowToInsert.of(map);
  }

  private void flushBatch() throws HopException {
    if (data.batch == null || data.batch.isEmpty() || data.tableId == null) {
      return;
    }
    InsertAllRequest req = InsertAllRequest.newBuilder(data.tableId).setRows(data.batch).build();
    InsertAllResponse resp = data.bigquery.insertAll(req);
    int sent = data.batch.size();
    data.batch.clear();
    if (resp.hasErrors()) {
      StringBuilder sb = new StringBuilder("BigQuery insertAll reported errors:\n");
      resp.getInsertErrors()
          .forEach(
              (idx, errs) ->
                  sb.append("  row ").append(idx).append(": ").append(errs).append("\n"));
      throw new HopException(sb.toString());
    }
    if (isDetailed()) {
      logDetailed("Streamed " + sent + " row(s) to BigQuery");
    }
  }
}
