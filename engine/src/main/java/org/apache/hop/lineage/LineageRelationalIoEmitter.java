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

package org.apache.hop.lineage;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalLifecycle;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RelationalWriteColumn;
import org.apache.hop.pipeline.transform.ITransform;

/**
 * Emits {@link LineageEventKind#RELATIONAL_IO} observations to {@link LineageHub} so sinks can
 * record which relational tables a transform read from or wrote to. The physical dataset identity
 * (the OpenLineage {@code namespace}) is resolved here — engine-side — from the {@link
 * DatabaseMeta} because the plugin never sees a connection; see {@link LineageRelationalIdentity}
 * and the "OpenLineage dataset identity" page of the Hop user manual (relational datasets).
 *
 * <p>Four shapes are supported:
 *
 * <ul>
 *   <li><b>write</b> — a transform whose target table is known up front (e.g. Table Output,
 *       Insert/Update): {@link #emitTransformRelationalWrite}.
 *   <li><b>read</b> — a transform that reads via a {@code SELECT} whose source tables are not known
 *       to the engine (e.g. Table Input): {@link #emitTransformRelationalRead}. The raw SQL is
 *       carried on the payload for a sink to parse into input tables; callers that already know the
 *       tables may pass them directly.
 *   <li><b>delete</b> — a transform that removes rows from a known table (e.g. Delete): {@link
 *       #emitTransformRelationalDelete}. The table is affected but no columns are produced, so no
 *       column schema and no column provenance are carried.
 *   <li><b>exec</b> — arbitrary SQL that may read and write (e.g. Execute SQL): {@link
 *       #emitTransformRelationalExec}.
 * </ul>
 *
 * <p><b>Best-effort by construction.</b> Every method is a no-op when its required arguments are
 * null and <b>never throws into transform execution</b>: the whole emission path is guarded here,
 * so call sites invoke these methods directly without a defensive {@code try/catch} of their own. A
 * failure is logged on the transform's own log channel at debug level and otherwise ignored.
 */
public final class LineageRelationalIoEmitter {

  /** Format hint stored on the row schema of a relational dataset. */
  private static final String JDBC_FORMAT_HINT = "jdbc";

  private LineageRelationalIoEmitter() {}

  /**
   * Records a write to a known relational table (Table Output and similar). {@code database} and
   * {@code schema} may be null/blank for databases without those levels; {@code table} is required.
   *
   * @param transform the transform performing the write (must not be null)
   * @param databaseMeta the target connection (must not be null; supplies the dataset namespace)
   * @param database catalog/database segment, or null/blank when not applicable
   * @param schema schema segment, or null/blank when not applicable
   * @param table target table name (required)
   * @param writtenRowMeta row shape written to the table, for a column schema; may be null
   * @param success whether the write completed without error
   * @param message optional short detail (failure reason, etc.)
   */
  public static void emitTransformRelationalWrite(
      ITransform transform,
      DatabaseMeta databaseMeta,
      String database,
      String schema,
      String table,
      IRowMeta writtenRowMeta,
      boolean success,
      String message) {
    emitTransformRelationalWrite(
        transform, databaseMeta, database, schema, table, writtenRowMeta, null, success, message);
  }

  /**
   * Same as {@link #emitTransformRelationalWrite(ITransform, DatabaseMeta, String, String, String,
   * IRowMeta, boolean, String)} but with per-column provenance ({@code writeColumns}) so a sink can
   * build column-level lineage for the pipeline stream path.
   */
  public static void emitTransformRelationalWrite(
      ITransform transform,
      DatabaseMeta databaseMeta,
      String database,
      String schema,
      String table,
      IRowMeta writtenRowMeta,
      List<RelationalWriteColumn> writeColumns,
      boolean success,
      String message) {
    emitTransformRelationalWrite(
        transform,
        databaseMeta,
        database,
        schema,
        table,
        writtenRowMeta,
        writeColumns,
        null,
        success,
        message);
  }

  /**
   * Same as the {@code writeColumns} overload but also records the lifecycle state the write
   * applied to its target (e.g. {@link RelationalLifecycle#OVERWRITE} when the target was truncated
   * first).
   */
  public static void emitTransformRelationalWrite(
      ITransform transform,
      DatabaseMeta databaseMeta,
      String database,
      String schema,
      String table,
      IRowMeta writtenRowMeta,
      List<RelationalWriteColumn> writeColumns,
      RelationalLifecycle lifecycle,
      boolean success,
      String message) {
    if (transform == null || databaseMeta == null || Utils.isEmpty(table)) {
      return;
    }
    emit(
        transform,
        databaseMeta,
        RelationalIoOperation.WRITE,
        null,
        List.of(),
        List.of(new RelationalTable(database, schema, table)),
        toContentSchema(writtenRowMeta),
        writeColumns,
        lifecycle,
        success,
        message);
  }

  /**
   * Records a read performed by a {@code SELECT} (Table Input and similar). When the engine cannot
   * resolve the source tables it passes {@code inputs = null} and relies on a sink to parse {@code
   * sql}; callers that already know the tables may pass them in {@code inputs}.
   *
   * @param transform the transform performing the read (must not be null)
   * @param databaseMeta the source connection (must not be null; supplies the dataset namespace)
   * @param sql the SQL that produced the rows, retained for downstream table/column parsing
   * @param inputs source tables when already known, or null/empty to defer to SQL parsing
   * @param outputRowMeta row shape produced by the read, for a column schema; may be null
   * @param success whether the read completed without error
   * @param message optional short detail (failure reason, etc.)
   */
  public static void emitTransformRelationalRead(
      ITransform transform,
      DatabaseMeta databaseMeta,
      String sql,
      List<RelationalTable> inputs,
      IRowMeta outputRowMeta,
      boolean success,
      String message) {
    if (transform == null || databaseMeta == null) {
      return;
    }
    emit(
        transform,
        databaseMeta,
        RelationalIoOperation.READ,
        sql,
        inputs,
        List.of(),
        toContentSchema(outputRowMeta),
        success,
        message);
  }

  /**
   * Records a row deletion from a known relational table (Delete and similar). The table is
   * reported as an affected output dataset, but — unlike a write — no column schema and no column
   * provenance are carried: a delete produces no columns, so publishing the transform's input row
   * shape as the target table's schema would be wrong.
   *
   * @param transform the transform performing the delete (must not be null)
   * @param databaseMeta the target connection (must not be null; supplies the dataset namespace)
   * @param database catalog/database segment, or null/blank when not applicable
   * @param schema schema segment, or null/blank when not applicable
   * @param table affected table name (required)
   * @param success whether the delete completed without error
   * @param message optional short detail (failure reason, etc.)
   */
  public static void emitTransformRelationalDelete(
      ITransform transform,
      DatabaseMeta databaseMeta,
      String database,
      String schema,
      String table,
      boolean success,
      String message) {
    if (transform == null || databaseMeta == null || Utils.isEmpty(table)) {
      return;
    }
    emit(
        transform,
        databaseMeta,
        RelationalIoOperation.DELETE,
        null,
        List.of(),
        List.of(new RelationalTable(database, schema, table)),
        null,
        success,
        message);
  }

  /**
   * Records arbitrary SQL that may read and/or write tables (Execute SQL and similar). Both table
   * lists may be null/empty and left for a sink to resolve from {@code sql}.
   *
   * @param transform the transform running the SQL (must not be null)
   * @param databaseMeta the connection the SQL runs on (must not be null)
   * @param sql the SQL statement(s), retained for downstream table/column parsing
   * @param inputs tables read when already known, or null/empty to defer to SQL parsing
   * @param outputs tables written when already known, or null/empty to defer to SQL parsing
   * @param success whether execution completed without error
   * @param message optional short detail (failure reason, etc.)
   */
  public static void emitTransformRelationalExec(
      ITransform transform,
      DatabaseMeta databaseMeta,
      String sql,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      boolean success,
      String message) {
    if (transform == null || databaseMeta == null) {
      return;
    }
    emit(
        transform,
        databaseMeta,
        RelationalIoOperation.EXEC,
        sql,
        inputs,
        outputs,
        null,
        success,
        message);
  }

  private static void emit(
      ITransform transform,
      DatabaseMeta databaseMeta,
      RelationalIoOperation operation,
      String sql,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      FileIoContentSchema outputSchema,
      boolean success,
      String message) {
    emit(
        transform,
        databaseMeta,
        operation,
        sql,
        inputs,
        outputs,
        outputSchema,
        null,
        null,
        success,
        message);
  }

  private static void emit(
      ITransform transform,
      DatabaseMeta databaseMeta,
      RelationalIoOperation operation,
      String sql,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      FileIoContentSchema outputSchema,
      List<RelationalWriteColumn> writeColumns,
      RelationalLifecycle lifecycle,
      boolean success,
      String message) {
    // The single guard for the whole feature: lineage is an observation of the pipeline, never a
    // participant in it, so nothing raised while building or queueing an event may surface to the
    // transform. Keeping it here (rather than at every call site) is what lets the ~15 emit points
    // across the transforms call these methods bare.
    try {
      LineageContext.Builder ctx = LineageRunLifecycleEmitter.transformContextBuilder(transform);
      if (ctx == null) {
        return;
      }
      if (!Utils.isEmpty(transform.getTransformPluginId())) {
        ctx.putAttribute("transformPluginId", transform.getTransformPluginId());
      }
      // The transform is an IVariables; the identity helper resolves host/port against it.
      String namespace = LineageRelationalIdentity.namespace(databaseMeta, transform);
      // Connection catalog + preferred schema, used by a sink to qualify tables recovered from SQL
      // (which may name neither) so reads and writes of the same table share one identity.
      String defaultCatalog = transform.resolve(databaseMeta.getDatabaseName());
      String defaultSchema = transform.resolve(databaseMeta.getPreferredSchemaName());
      RelationalIoLineagePayload payload =
          new RelationalIoLineagePayload(
              operation,
              namespace,
              inputs,
              outputs,
              sql,
              outputSchema,
              defaultCatalog,
              defaultSchema,
              writeColumns,
              lifecycle,
              success,
              message);
      LineageHub.getInstance()
          .emit(LineageEvent.of(LineageEventKind.RELATIONAL_IO, ctx.build(), payload));
    } catch (Exception e) {
      logLineageFailure(transform, e);
    }
  }

  /**
   * Resolves a named connection for lineage without letting a lookup failure reach the transform.
   * Transforms that do not otherwise hold a {@link DatabaseMeta} at emit time (the bulk loaders,
   * which open a native client rather than a JDBC connection) use this instead of calling {@code
   * findDatabase} themselves inside a defensive block.
   *
   * @return the connection, or null when it cannot be resolved (lineage is then skipped)
   */
  public static DatabaseMeta lineageConnection(
      ITransform transform, AbstractMeta meta, String connectionName) {
    if (transform == null || meta == null || Utils.isEmpty(connectionName)) {
      return null;
    }
    try {
      return meta.findDatabase(connectionName, transform);
    } catch (Exception e) {
      logLineageFailure(transform, e);
      return null;
    }
  }

  /** Reports a swallowed lineage failure on the transform's own channel, at debug level only. */
  private static void logLineageFailure(ITransform transform, Exception e) {
    try {
      ILogChannel log = transform.getLogChannel();
      if (log != null && log.isDebug()) {
        log.logDebug("Relational lineage observation skipped: " + e.getMessage());
      }
    } catch (Exception ignored) {
      // The logger itself failed; there is nowhere left to report to.
    }
  }

  /**
   * Converts a Hop {@link IRowMeta} to the neutral {@link FileIoContentSchema} carried on
   * relational events, so a sink can populate OpenLineage dataset {@code schema} fields (column
   * name + Hop type). Returns null for a null/empty row shape. Relational columns have no path
   * locator, so the structure-roots tree is empty.
   */
  static FileIoContentSchema toContentSchema(IRowMeta rowMeta) {
    if (rowMeta == null || rowMeta.isEmpty()) {
      return null;
    }
    List<FileIoTabularColumn> columns = new ArrayList<>(rowMeta.size());
    for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
      columns.add(
          new FileIoTabularColumn(
              valueMeta.getName(),
              valueMeta.getTypeDesc(),
              valueMeta.getLength(),
              valueMeta.getPrecision(),
              null,
              null,
              false));
    }
    return new FileIoContentSchema(JDBC_FORMAT_HINT, columns, List.of());
  }

  /**
   * Builds one column-provenance entry for {@link #emitTransformRelationalWrite}: maps {@code
   * streamField} to {@code targetColumn} (falling back to the stream field name when the target is
   * blank) and resolves the transform that produced the field via {@link IValueMeta#getOrigin()} so
   * a sink can trace the column back to its source. Returns {@code null} — for the caller to skip —
   * when there is no row shape or no stream field (e.g. a generated column with no input).
   */
  public static RelationalWriteColumn writeColumn(
      IRowMeta inputRowMeta, String targetColumn, String streamField) {
    if (inputRowMeta == null || Utils.isEmpty(streamField)) {
      return null;
    }
    IValueMeta valueMeta = inputRowMeta.searchValueMeta(streamField);
    return new RelationalWriteColumn(
        Utils.isEmpty(targetColumn) ? streamField : targetColumn,
        streamField,
        valueMeta != null ? valueMeta.getOrigin() : null);
  }

  /**
   * Convenience over {@link #writeColumn}: appends the resolved column provenance to {@code
   * columns}, skipping it when the stream field is blank or the row shape is unavailable. Lets a
   * transform map its field list to write columns without null-check boilerplate.
   */
  public static void addWriteColumn(
      List<RelationalWriteColumn> columns,
      IRowMeta inputRowMeta,
      String targetColumn,
      String streamField) {
    RelationalWriteColumn column = writeColumn(inputRowMeta, targetColumn, streamField);
    if (column != null) {
      columns.add(column);
    }
  }

  /**
   * Column provenance for a transform that writes its whole input row 1:1 (target column name
   * equals the stream field name) — e.g. a bulk loader with no explicit field mapping. Returns an
   * empty list for a null/empty row shape.
   */
  public static List<RelationalWriteColumn> writeColumnsFromRow(IRowMeta inputRowMeta) {
    List<RelationalWriteColumn> columns = new ArrayList<>();
    if (inputRowMeta != null) {
      for (IValueMeta valueMeta : inputRowMeta.getValueMetaList()) {
        columns.add(
            new RelationalWriteColumn(
                valueMeta.getName(), valueMeta.getName(), valueMeta.getOrigin()));
      }
    }
    return columns;
  }
}
