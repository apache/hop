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

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hop.lineage.LineageRowCounts;

/**
 * The rows the transforms of one pipeline run read from and wrote to each dataset, for the
 * statistics facets of its datasets.
 *
 * <p>Hop counts rows per transform, not per dataset. A count belongs to a dataset only when every
 * transform that wrote it (or read it) wrote (or read) nothing else: then the sum over those
 * transforms and their copies is the dataset's count. A transform that writes several files or
 * tables gives them no count rather than a wrong one.
 */
final class RowStatistics {

  /** What a run did to a dataset it wrote, as DataHub names it, and the rows that took. */
  record Write(String namespace, String name, String operationType, Long rows) {}

  /** The counters of each copy of each transform, from their terminal events. */
  private final Map<String, Map<String, Map<String, String>>> counters = new LinkedHashMap<>();

  private final Map<String, Set<String>> written = new LinkedHashMap<>();
  private final Map<String, Set<String>> read = new LinkedHashMap<>();

  /** Every dataset written, by key, with the facets of its last observation. */
  private final Map<String, OutputDataset> outputs = new LinkedHashMap<>();

  /** The datasets each transform deleted rows from (a relational DELETE). */
  private final Map<String, Set<String>> deletedBy = new LinkedHashMap<>();

  /** The transforms whose counters don't belong to one dataset, such as a script of statements. */
  private final Set<String> uncounted = new LinkedHashSet<>();

  /** Remembers the counters of a transform copy's terminal event. */
  synchronized void counters(String transform, String copy, Map<String, String> attributes) {
    if (transform == null
        || LineageRowCounts.get(attributes, LineageRowCounts.LINES_OUTPUT) == null) {
      return;
    }
    counters
        .computeIfAbsent(transform, t -> new LinkedHashMap<>())
        .put(copy == null ? "0" : copy, attributes);
  }

  /** Remembers which datasets a transform read and wrote. */
  synchronized void io(String transform, List<InputDataset> inputs, List<OutputDataset> outputs) {
    if (transform == null) {
      return;
    }
    for (InputDataset input : inputs) {
      read.computeIfAbsent(transform, t -> new LinkedHashSet<>())
          .add(key(input.getNamespace(), input.getName()));
    }
    for (OutputDataset output : outputs) {
      String key = key(output.getNamespace(), output.getName());
      written.computeIfAbsent(transform, t -> new LinkedHashSet<>()).add(key);
      this.outputs.put(key, output);
    }
  }

  /** Remembers that a transform deleted rows from these datasets. */
  synchronized void deleted(String transform, List<OutputDataset> outputs) {
    if (transform == null) {
      return;
    }
    for (OutputDataset output : outputs) {
      deletedBy
          .computeIfAbsent(transform, t -> new LinkedHashSet<>())
          .add(key(output.getNamespace(), output.getName()));
    }
  }

  /** Remembers that the counters of a transform can't be given to the datasets it touched. */
  synchronized void uncounted(String transform) {
    if (transform != null) {
      uncounted.add(transform);
    }
  }

  /** Whether a transform's counters can be given to the datasets it touched. */
  synchronized boolean isCounted(String transform) {
    return !uncounted.contains(transform);
  }

  /**
   * Whether a transform's counters belong to this one dataset it wrote: it didn't delete from it.
   */
  synchronized boolean countsWritesTo(String transform, String namespace, String name) {
    return !uncounted.contains(transform)
        && !deletedBy.getOrDefault(transform, Set.of()).contains(key(namespace, name));
  }

  /**
   * The rows the run wrote to a dataset (output plus updated), or null when it can't tell. Rows a
   * transform deleted from a dataset are not rows written to it.
   */
  synchronized Long rowsWritten(String namespace, String name) {
    String key = key(namespace, name);
    return rows(written, key, RowStatistics::rowsWritten, t -> !deleted(t, key));
  }

  /** The rows the run read from a dataset, or null when it can't tell. */
  synchronized Long rowsRead(String namespace, String name) {
    return rows(read, key(namespace, name), RowStatistics::rowsRead, t -> true);
  }

  /** Every dataset the run wrote, once, with what it did and the rows that took. */
  synchronized List<Write> writes() {
    List<Write> writes = new ArrayList<>();
    for (Map.Entry<String, OutputDataset> entry : outputs.entrySet()) {
      String key = entry.getKey();
      OutputDataset output = entry.getValue();
      boolean anyInserter = false;
      for (Map.Entry<String, Set<String>> writer : written.entrySet()) {
        if (writer.getValue().contains(key) && !deleted(writer.getKey(), key)) {
          anyInserter = true;
        }
      }
      String type;
      Long rows;
      if (!anyInserter) {
        // Every transform that wrote it deleted from it
        type = "DELETE";
        rows = rows(written, key, RowStatistics::rowsUpdated, t -> deleted(t, key));
      } else {
        // A table emptied and reloaded in one run was loaded: the rows are the ones written
        rows = rows(written, key, RowStatistics::rowsWritten, t -> !deleted(t, key));
        Long inserted = rows(written, key, RowStatistics::rowsOutput, t -> !deleted(t, key));
        String lifecycle = lifecycleOperation(output);
        if (lifecycle != null) {
          type = lifecycle;
        } else if (isOverwrite(output) && rows != null && rows == 0) {
          // Truncated and nothing written: the data is gone, how many rows is not known
          type = "DELETE";
          rows = null;
        } else if (inserted != null && inserted == 0 && rows != null && rows > 0) {
          type = "UPDATE";
        } else {
          type = "INSERT";
        }
      }
      writes.add(new Write(output.getNamespace(), output.getName(), type, rows));
    }
    return writes;
  }

  private boolean deleted(String transform, String dataset) {
    return deletedBy.getOrDefault(transform, Set.of()).contains(dataset);
  }

  private static boolean isOverwrite(OutputDataset output) {
    OpenLineage.LifecycleStateChangeDatasetFacet facet =
        output.getFacets() == null ? null : output.getFacets().getLifecycleStateChange();
    return facet != null
        && (facet.getLifecycleStateChange()
                == OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE
            || facet.getLifecycleStateChange()
                == OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.TRUNCATE);
  }

  private static String lifecycleOperation(OutputDataset output) {
    OpenLineage.LifecycleStateChangeDatasetFacet facet =
        output.getFacets() == null ? null : output.getFacets().getLifecycleStateChange();
    if (facet == null || facet.getLifecycleStateChange() == null) {
      return null;
    }
    return switch (facet.getLifecycleStateChange()) {
      case CREATE -> "CREATE";
      case DROP -> "DROP";
      case ALTER, RENAME -> "ALTER";
      default -> null;
    };
  }

  /**
   * The sum of a counter over the transforms that touched a dataset and are included, or null when
   * one of them touched other datasets too, never reported its counters, or counts several things
   * at once.
   */
  private Long rows(
      Map<String, Set<String>> datasets,
      String dataset,
      Function<Map<String, String>, Long> count,
      Predicate<String> included) {
    long total = 0;
    boolean any = false;
    for (Map.Entry<String, Set<String>> entry : datasets.entrySet()) {
      if (!entry.getValue().contains(dataset) || !included.test(entry.getKey())) {
        continue;
      }
      Map<String, Map<String, String>> copies = counters.get(entry.getKey());
      if (entry.getValue().size() != 1 || copies == null || uncounted.contains(entry.getKey())) {
        return null;
      }
      for (Map<String, String> attributes : copies.values()) {
        Long rows = count.apply(attributes);
        if (rows == null) {
          return null;
        }
        total += rows;
      }
      any = true;
    }
    return any ? total : null;
  }

  /**
   * The rows a transform copy wrote to datasets: output plus updated. None when it wrote nothing
   * while it did receive rows: some transforms don't count what they write.
   */
  static Long rowsWritten(Map<String, String> attributes) {
    Long output = LineageRowCounts.get(attributes, LineageRowCounts.LINES_OUTPUT);
    Long updated = LineageRowCounts.get(attributes, LineageRowCounts.LINES_UPDATED);
    if (output == null) {
      return null;
    }
    long rows = output + (updated == null ? 0 : updated);
    return rows == 0 && positive(attributes, LineageRowCounts.LINES_READ) ? null : rows;
  }

  /**
   * The rows a transform copy read from datasets. None when it read nothing while it did pass rows
   * on: some transforms don't count what they read.
   */
  static Long rowsRead(Map<String, String> attributes) {
    Long input = LineageRowCounts.get(attributes, LineageRowCounts.LINES_INPUT);
    return input != null && input == 0 && positive(attributes, LineageRowCounts.LINES_WRITTEN)
        ? null
        : input;
  }

  private static Long rowsOutput(Map<String, String> attributes) {
    return LineageRowCounts.get(attributes, LineageRowCounts.LINES_OUTPUT);
  }

  private static Long rowsUpdated(Map<String, String> attributes) {
    return LineageRowCounts.get(attributes, LineageRowCounts.LINES_UPDATED);
  }

  private static boolean positive(Map<String, String> attributes, String counter) {
    Long value = LineageRowCounts.get(attributes, counter);
    return value != null && value > 0;
  }

  /** The outputs, each with an output statistics facet when its row count is known. */
  static List<OutputDataset> withRowCounts(
      OpenLineage ol, List<OutputDataset> outputs, Function<OutputDataset, Long> rowCount) {
    List<OutputDataset> result = new ArrayList<>(outputs.size());
    for (OutputDataset output : outputs) {
      Long rows = rowCount.apply(output);
      result.add(
          rows == null
              ? output
              : ol.newOutputDatasetBuilder()
                  .namespace(output.getNamespace())
                  .name(output.getName())
                  .facets(output.getFacets())
                  .outputFacets(
                      ol.newOutputDatasetOutputFacetsBuilder()
                          .outputStatistics(
                              ol.newOutputStatisticsOutputDatasetFacet(rows, null, null))
                          .build())
                  .build());
    }
    return result;
  }

  /** The inputs, each with an input statistics facet when its row count is known. */
  static List<InputDataset> withInputRowCounts(
      OpenLineage ol, List<InputDataset> inputs, Function<InputDataset, Long> rowCount) {
    List<InputDataset> result = new ArrayList<>(inputs.size());
    for (InputDataset input : inputs) {
      Long rows = rowCount.apply(input);
      result.add(
          rows == null
              ? input
              : ol.newInputDatasetBuilder()
                  .namespace(input.getNamespace())
                  .name(input.getName())
                  .facets(input.getFacets())
                  .inputFacets(
                      ol.newInputDatasetInputFacetsBuilder()
                          .inputStatistics(ol.newInputStatisticsInputDatasetFacet(rows, null, null))
                          .build())
                  .build());
    }
    return result;
  }

  private static String key(String namespace, String name) {
    return namespace + '\n' + name;
  }
}
