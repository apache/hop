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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RelationalWriteColumn;

/**
 * Correlates relational reads and writes within a pipeline run to produce column-level lineage for
 * the stream path — e.g. a Table Input {@code SELECT id, amount FROM source} feeding a Table Output
 * that writes {@code target(id, amount)}.
 *
 * <p>Each read registers, per transform, the mapping from its output stream fields to the source
 * table column they came from (from the read's parsed SQL column lineage). Each write then carries,
 * per target column, the stream field and the transform that produced it ({@code
 * IValueMeta.getOrigin()}). Matching the write column's origin transform to a registered read
 * resolves {@code targetColumn → sourceColumn}, even through pass-through transforms (which
 * preserve the field origin). It is deliberately conservative: a column whose origin is not a
 * registered read (a computed field, a join without a resolvable source, …) yields no lineage
 * rather than a guess.
 *
 * <p>State is held per pipeline run in a bounded least-recently-used map. It is deliberately not
 * dropped when the run reports COMPLETE: a transform emits its relational I/O when it finishes, and
 * that can reach the sink after the pipeline's own completion event, so clearing on completion
 * silently cost those writes their column lineage. Eviction by age instead of by run lifecycle
 * keeps memory bounded without depending on the order events happen to arrive in.
 */
final class RelationalColumnLineageCorrelator {

  /**
   * How many pipeline runs to keep read state for. Well above any realistic number of concurrently
   * running pipelines, and small enough that the retained mappings stay negligible.
   */
  static final int MAX_RUNS = 128;

  /** What a single read exposes: how each stream field maps back to a source table column. */
  private static final class ReadInfo {
    /** Stream field name → source column reference, from the read's parsed column lineage. */
    private final Map<String, RelationalSqlParser.FieldRef> byStreamField = new LinkedHashMap<>();

    /** The single source table, used to name-match stream fields the parser did not resolve. */
    private final RelationalTable singleSource;

    ReadInfo(List<RelationalSqlParser.ColumnEdge> columnEdges, List<RelationalTable> inputs) {
      for (RelationalSqlParser.ColumnEdge edge : columnEdges) {
        if (edge.outputField() != null && !edge.inputs().isEmpty()) {
          byStreamField.putIfAbsent(edge.outputField(), edge.inputs().get(0));
        }
      }
      this.singleSource = inputs != null && inputs.size() == 1 ? inputs.get(0) : null;
    }

    RelationalSqlParser.FieldRef sourceFor(String streamField) {
      RelationalSqlParser.FieldRef ref = byStreamField.get(streamField);
      if (ref != null) {
        return ref;
      }
      // Fall back to a direct column of the sole source table (name match) when the parser gave no
      // per-column lineage for this field (e.g. SELECT *).
      if (singleSource != null && !Utils.isEmpty(streamField)) {
        return new RelationalSqlParser.FieldRef(singleSource, streamField);
      }
      return null;
    }
  }

  /** Least-recently-used run state, oldest run evicted once {@link #MAX_RUNS} is exceeded. */
  private static final class RunCache extends LinkedHashMap<String, Map<String, ReadInfo>> {
    RunCache() {
      super(16, 0.75f, true);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Map<String, ReadInfo>> eldest) {
      return size() > MAX_RUNS;
    }
  }

  private final Map<String, Map<String, ReadInfo>> readsByRun =
      Collections.synchronizedMap(new RunCache());

  /**
   * Registers a read's per-field source columns under its run and transform. No-op when the run,
   * transform, or column information is missing.
   */
  void registerRead(
      String runId,
      String transformName,
      List<RelationalSqlParser.ColumnEdge> columnEdges,
      List<RelationalTable> inputs) {
    if (Utils.isEmpty(runId) || Utils.isEmpty(transformName)) {
      return;
    }
    readsByRun
        .computeIfAbsent(runId, k -> new ConcurrentHashMap<>())
        .put(transformName, new ReadInfo(columnEdges, inputs));
  }

  /**
   * Resolves each written column back to its source column via the read registered for its origin
   * transform, producing {@link RelationalSqlParser.ColumnEdge}s ({@code targetColumn ←
   * sourceColumn}) for the sink to render as a {@code columnLineage} facet. Columns whose origin is
   * not a registered read are omitted.
   */
  List<RelationalSqlParser.ColumnEdge> correlate(
      String runId, List<RelationalWriteColumn> writeColumns) {
    if (Utils.isEmpty(runId) || writeColumns == null || writeColumns.isEmpty()) {
      return List.of();
    }
    Map<String, ReadInfo> reads = readsByRun.get(runId);
    if (reads == null || reads.isEmpty()) {
      return List.of();
    }
    List<RelationalSqlParser.ColumnEdge> edges = new ArrayList<>();
    for (RelationalWriteColumn column : writeColumns) {
      if (Utils.isEmpty(column.getTargetColumn()) || Utils.isEmpty(column.getOriginTransform())) {
        continue;
      }
      ReadInfo read = reads.get(column.getOriginTransform());
      if (read == null) {
        continue;
      }
      RelationalSqlParser.FieldRef source = read.sourceFor(column.getStreamField());
      if (source == null || Utils.isEmpty(source.field())) {
        continue;
      }
      edges.add(new RelationalSqlParser.ColumnEdge(column.getTargetColumn(), List.of(source)));
    }
    return edges;
  }
}
