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

package org.apache.hop.lineage.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Getter;

/**
 * Relational/JDBC read or write observed on a transform, carrying the input and output tables that
 * a sink maps to OpenLineage datasets.
 *
 * <p>The {@code datasetNamespace} is resolved engine-side from the {@code DatabaseMeta} (via {@code
 * LineageRelationalIdentity}) because the plugin never sees a connection; combined with each {@link
 * RelationalTable}'s qualified name it forms the locked {@code (namespace, name)} identity of the
 * "OpenLineage dataset identity" page of the user manual. {@code sqlText} is retained for a SQL job
 * facet and, in a later phase, for column-level lineage parsing. {@code outputSchema} carries
 * column names/Hop types when the transform knows its row shape (e.g. Table Input's {@code
 * outputRowMeta}).
 */
@Getter
public final class RelationalIoLineagePayload implements LineagePayload {

  private final RelationalIoOperation operation;

  /** OpenLineage dataset namespace of the database, e.g. {@code postgres://db:5432}. */
  private final String datasetNamespace;

  private final List<RelationalTable> inputs;
  private final List<RelationalTable> outputs;

  /** Raw SQL that produced this event, when available (Table Input, Execute SQL); may be null. */
  private final String sqlText;

  /** Column schema of the produced/consumed rows when known; may be null. */
  private final FileIoContentSchema outputSchema;

  /**
   * Catalog/database of the connection ({@code DatabaseMeta.getDatabaseName()}), used by a sink to
   * qualify tables recovered by parsing SQL: a {@code SELECT ... FROM schema.table} yields no
   * catalog segment (SQL cannot name it), so the sink prepends this so a read and a write of the
   * same table share one {@code database.schema.table} identity. Null/blank when the database has
   * no catalog level.
   */
  private final String defaultCatalog;

  /**
   * Default schema of the connection ({@code DatabaseMeta.getPreferredSchemaName()}), used by a
   * sink to fill a table's blank schema so a bare {@code table} in a {@code SELECT} resolves to the
   * same {@code schema.table} identity as an explicitly schema-qualified write. Null/blank when the
   * connection has no preferred schema.
   */
  private final String defaultSchema;

  /**
   * Per-column provenance of a write (target column, stream field, origin transform), used by a
   * sink to build column-level lineage for the pipeline stream path. Empty for reads and for writes
   * whose columns could not be traced.
   */
  private final List<RelationalWriteColumn> writeColumns;

  /**
   * Lifecycle state the write applied to its target (e.g. OVERWRITE on truncate); null when none.
   */
  private final RelationalLifecycle lifecycle;

  private final boolean success;
  private final String message;

  public RelationalIoLineagePayload(
      RelationalIoOperation operation,
      String datasetNamespace,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      String sqlText,
      FileIoContentSchema outputSchema,
      boolean success,
      String message) {
    this(
        operation,
        datasetNamespace,
        inputs,
        outputs,
        sqlText,
        outputSchema,
        null,
        success,
        message);
  }

  public RelationalIoLineagePayload(
      RelationalIoOperation operation,
      String datasetNamespace,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      String sqlText,
      FileIoContentSchema outputSchema,
      String defaultCatalog,
      boolean success,
      String message) {
    this(
        operation,
        datasetNamespace,
        inputs,
        outputs,
        sqlText,
        outputSchema,
        defaultCatalog,
        null,
        success,
        message);
  }

  public RelationalIoLineagePayload(
      RelationalIoOperation operation,
      String datasetNamespace,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      String sqlText,
      FileIoContentSchema outputSchema,
      String defaultCatalog,
      String defaultSchema,
      boolean success,
      String message) {
    this(
        operation,
        datasetNamespace,
        inputs,
        outputs,
        sqlText,
        outputSchema,
        defaultCatalog,
        defaultSchema,
        null,
        success,
        message);
  }

  public RelationalIoLineagePayload(
      RelationalIoOperation operation,
      String datasetNamespace,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      String sqlText,
      FileIoContentSchema outputSchema,
      String defaultCatalog,
      String defaultSchema,
      List<RelationalWriteColumn> writeColumns,
      boolean success,
      String message) {
    this(
        operation,
        datasetNamespace,
        inputs,
        outputs,
        sqlText,
        outputSchema,
        defaultCatalog,
        defaultSchema,
        writeColumns,
        null,
        success,
        message);
  }

  public RelationalIoLineagePayload(
      RelationalIoOperation operation,
      String datasetNamespace,
      List<RelationalTable> inputs,
      List<RelationalTable> outputs,
      String sqlText,
      FileIoContentSchema outputSchema,
      String defaultCatalog,
      String defaultSchema,
      List<RelationalWriteColumn> writeColumns,
      RelationalLifecycle lifecycle,
      boolean success,
      String message) {
    this.operation = Objects.requireNonNull(operation, "operation");
    this.datasetNamespace = datasetNamespace;
    this.inputs =
        inputs == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(inputs));
    this.outputs =
        outputs == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(outputs));
    this.sqlText = sqlText;
    this.outputSchema = outputSchema;
    this.defaultCatalog = defaultCatalog;
    this.defaultSchema = defaultSchema;
    this.writeColumns =
        writeColumns == null
            ? List.of()
            : Collections.unmodifiableList(new ArrayList<>(writeColumns));
    this.lifecycle = lifecycle;
    this.success = success;
    this.message = message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationalIoLineagePayload that = (RelationalIoLineagePayload) o;
    return success == that.success
        && operation == that.operation
        && Objects.equals(datasetNamespace, that.datasetNamespace)
        && inputs.equals(that.inputs)
        && outputs.equals(that.outputs)
        && Objects.equals(sqlText, that.sqlText)
        && Objects.equals(outputSchema, that.outputSchema)
        && Objects.equals(defaultCatalog, that.defaultCatalog)
        && Objects.equals(defaultSchema, that.defaultSchema)
        && writeColumns.equals(that.writeColumns)
        && lifecycle == that.lifecycle
        && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        operation,
        datasetNamespace,
        inputs,
        outputs,
        sqlText,
        outputSchema,
        defaultCatalog,
        defaultSchema,
        writeColumns,
        lifecycle,
        success,
        message);
  }
}
