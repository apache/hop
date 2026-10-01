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

package org.apache.hop.lineage.openlineage;

import io.openlineage.sql.ColumnLineage;
import io.openlineage.sql.ColumnMeta;
import io.openlineage.sql.DbTableMeta;
import io.openlineage.sql.OpenLineageSql;
import io.openlineage.sql.SqlMeta;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.model.RelationalTable;

/**
 * Extracts the input and output tables of a SQL statement using the OpenLineage SQL parser ({@code
 * openlineage-sql-java}: a Rust core reached over JNI). Used to recover the source tables of a
 * Table Input {@code SELECT} — which the Hop engine cannot know without parsing — into the neutral
 * {@link RelationalTable} identity the mapper turns into OpenLineage datasets.
 *
 * <p>Strictly best-effort: a disabled parser, a blank statement, an unavailable native library, a
 * statement the parser rejects, or any native error all yield {@link Tables#EMPTY} and never throw.
 * A read whose SQL cannot be parsed therefore produces no relational input lineage rather than
 * failing delivery.
 */
final class RelationalSqlParser {

  private final boolean enabled;
  private final ILogChannel log;

  private RelationalSqlParser(boolean enabled, ILogChannel log) {
    this.enabled = enabled;
    this.log = log;
  }

  /** Creates a parser; when {@code enabled} is false every {@link #parse} returns empty. */
  static RelationalSqlParser create(boolean enabled, ILogChannel log) {
    return new RelationalSqlParser(enabled, log);
  }

  /**
   * The tables a statement reads ({@code inputs}) and writes ({@code outputs}), plus
   * per-output-field {@code columnLineage} when the parser resolved it (e.g. {@code INSERT …
   * SELECT}).
   */
  record Tables(
      List<RelationalTable> inputs, List<RelationalTable> outputs, List<ColumnEdge> columnLineage) {
    static final Tables EMPTY = new Tables(List.of(), List.of(), List.of());

    boolean isEmpty() {
      return inputs.isEmpty() && outputs.isEmpty();
    }
  }

  /** One output field and the input table columns it derives from. */
  record ColumnEdge(String outputField, List<FieldRef> inputs) {}

  /** A reference to a column of a specific relational table. */
  record FieldRef(RelationalTable table, String field) {}

  /**
   * Parses {@code sql} for the given database {@code dialect} (e.g. {@code postgres}, {@code
   * mysql}; may be null for the parser's default). Returns the referenced tables, or {@link
   * Tables#EMPTY} on any failure.
   */
  Tables parse(String sql, String dialect) {
    if (!enabled || Utils.isEmpty(sql)) {
      return Tables.EMPTY;
    }
    if (OpenLineageSql.loadError != null && OpenLineageSql.loadError.isPresent()) {
      logDebug(
          "OpenLineage SQL parser native library unavailable: " + OpenLineageSql.loadError.get());
      return Tables.EMPTY;
    }
    try {
      Optional<SqlMeta> parsed = OpenLineageSql.parse(List.of(sql), dialect);
      if (parsed.isEmpty()) {
        return Tables.EMPTY;
      }
      SqlMeta meta = parsed.get();
      if (meta.errors() != null && !meta.errors().isEmpty()) {
        logDebug("OpenLineage SQL parser reported errors: " + meta.errors());
      }
      return new Tables(
          toTables(meta.inTables()),
          toTables(meta.outTables()),
          toColumnEdges(meta.columnLineage()));
    } catch (Throwable t) {
      // Native parse failures can surface as Errors (e.g. UnsatisfiedLinkError), not Exceptions.
      logDebug(
          "OpenLineage SQL parse failed; skipping relational input lineage: " + t.getMessage());
      return Tables.EMPTY;
    }
  }

  private static List<RelationalTable> toTables(List<DbTableMeta> tables) {
    if (tables == null || tables.isEmpty()) {
      return List.of();
    }
    List<RelationalTable> result = new ArrayList<>(tables.size());
    for (DbTableMeta table : tables) {
      if (table != null && !Utils.isEmpty(table.name())) {
        result.add(new RelationalTable(table.database(), table.schema(), table.name()));
      }
    }
    return result;
  }

  /**
   * Converts the parser's per-column lineage to neutral {@link ColumnEdge}s. Only source columns
   * whose origin table the parser resolved are kept (an unresolved origin cannot reference a
   * dataset); edges left with no resolvable source are dropped.
   */
  private static List<ColumnEdge> toColumnEdges(List<ColumnLineage> columnLineage) {
    if (columnLineage == null || columnLineage.isEmpty()) {
      return List.of();
    }
    List<ColumnEdge> edges = new ArrayList<>(columnLineage.size());
    for (ColumnLineage cl : columnLineage) {
      ColumnMeta descendant = cl.descendant();
      if (descendant == null || Utils.isEmpty(descendant.name())) {
        continue;
      }
      List<FieldRef> refs = new ArrayList<>();
      for (ColumnMeta source : cl.lineage()) {
        if (source == null || source.origin() == null || source.origin().isEmpty()) {
          continue;
        }
        if (Utils.isEmpty(source.name())) {
          continue;
        }
        DbTableMeta origin = source.origin().get();
        refs.add(
            new FieldRef(
                new RelationalTable(origin.database(), origin.schema(), origin.name()),
                source.name()));
      }
      if (!refs.isEmpty()) {
        edges.add(new ColumnEdge(descendant.name(), refs));
      }
    }
    return edges;
  }

  private void logDebug(String message) {
    if (log != null && log.isDebug()) {
      log.logDebug(message);
    }
  }
}
