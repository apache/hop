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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;

/**
 * Groups pipeline engine components into metrics-tree rows. Several copies of one transform become
 * one aggregate parent plus a child per copy. A single copy stays a leaf. Numeric metrics are
 * summed. Duration is the wall-clock span. Speed is the sum of the per-copy samples.
 */
final class PipelineMetricsGrouper {

  static final int COLUMN_NAME = 0;
  static final int COLUMN_COPY = 1;
  static final int LEADING_COLUMNS = 2;
  static final int TRAILING_COLUMNS = 3;

  private static final String SPEED_PATTERN = "#,###,###,###,##0";

  /**
   * Highest priority first. A parent shows the first of these that any copy is in, so one halted
   * copy is not hidden behind the copies that are still running.
   */
  private static final List<ComponentExecutionStatus> STATUS_PRIORITY =
      List.of(
          ComponentExecutionStatus.STATUS_HALTED,
          ComponentExecutionStatus.STATUS_HALTING,
          ComponentExecutionStatus.STATUS_STOPPED,
          ComponentExecutionStatus.STATUS_PAUSED,
          ComponentExecutionStatus.STATUS_RUNNING,
          ComponentExecutionStatus.STATUS_IDLE,
          ComponentExecutionStatus.STATUS_INIT,
          ComponentExecutionStatus.STATUS_FINISHED,
          ComponentExecutionStatus.STATUS_DISPOSED,
          ComponentExecutionStatus.STATUS_EMPTY);

  private PipelineMetricsGrouper() {}

  static List<PipelineMetricsNode> group(
      List<IEngineComponent> components,
      EngineMetrics engineMetrics,
      List<IEngineMetric> usedMetrics) {
    return group(components, engineMetrics, usedMetrics, System.currentTimeMillis());
  }

  static List<PipelineMetricsNode> group(
      List<IEngineComponent> components,
      EngineMetrics engineMetrics,
      List<IEngineMetric> usedMetrics,
      long nowMs) {
    Map<String, List<IEngineComponent>> groups = new LinkedHashMap<>();
    if (components != null) {
      for (IEngineComponent component : components) {
        if (component == null) {
          continue;
        }
        String name = component.getName() == null ? "" : component.getName();
        groups.computeIfAbsent(name, key -> new ArrayList<>()).add(component);
      }
    }

    List<IEngineMetric> metrics = usedMetrics == null ? List.of() : usedMetrics;
    List<PipelineMetricsNode> nodes = new ArrayList<>();
    for (Map.Entry<String, List<IEngineComponent>> entry : groups.entrySet()) {
      List<IEngineComponent> copies = new ArrayList<>(entry.getValue());
      copies.sort(Comparator.comparingInt(IEngineComponent::getCopyNr));
      if (copies.size() == 1) {
        nodes.add(leaf(copies.get(0), engineMetrics, metrics, nowMs));
      } else {
        List<PipelineMetricsNode> children = new ArrayList<>();
        for (IEngineComponent copy : copies) {
          children.add(leaf(copy, engineMetrics, metrics, nowMs));
        }
        nodes.add(parent(entry.getKey(), copies, children, engineMetrics, metrics, nowMs));
      }
    }
    return nodes;
  }

  /**
   * Sorts parent rows only. Children stay in copy-number order. Equal values keep pipeline order. A
   * negative column leaves the list as it is.
   */
  static void sortParents(
      List<PipelineMetricsNode> nodes,
      int column,
      boolean descending,
      List<IEngineMetric> usedMetrics) {
    if (column < 0 || nodes == null || nodes.size() < 2) {
      return;
    }
    List<IEngineMetric> metrics = usedMetrics == null ? List.of() : usedMetrics;
    nodes.sort(
        (left, right) -> {
          int compared = compare(left, right, column, metrics);
          return descending ? -compared : compared;
        });
  }

  static int durationColumn(List<IEngineMetric> usedMetrics) {
    return LEADING_COLUMNS + (usedMetrics == null ? 0 : usedMetrics.size());
  }

  private static PipelineMetricsNode leaf(
      IEngineComponent component,
      EngineMetrics engineMetrics,
      List<IEngineMetric> usedMetrics,
      long nowMs) {
    List<IEngineComponent> copies = List.of(component);
    SpeedAggregate speed = speed(copies, engineMetrics);
    String name = component.getName() == null ? "" : component.getName();
    return new PipelineMetricsNode(
        name,
        component.getCopyNr(),
        1,
        false,
        sumMetrics(copies, engineMetrics, usedMetrics),
        durationMs(copies, nowMs),
        speed.value,
        speed.text,
        statusText(engineMetrics, component),
        List.of());
  }

  private static PipelineMetricsNode parent(
      String name,
      List<IEngineComponent> copies,
      List<PipelineMetricsNode> children,
      EngineMetrics engineMetrics,
      List<IEngineMetric> usedMetrics,
      long nowMs) {
    SpeedAggregate speed = speed(copies, engineMetrics);
    int firstCopy = copies.get(0).getCopyNr();
    return new PipelineMetricsNode(
        name,
        firstCopy,
        copies.size(),
        true,
        sumMetrics(copies, engineMetrics, usedMetrics),
        durationMs(copies, nowMs),
        speed.value,
        speed.text,
        aggregateStatus(statusTexts(copies, engineMetrics)),
        children);
  }

  private static Map<String, Long> sumMetrics(
      List<IEngineComponent> copies, EngineMetrics engineMetrics, List<IEngineMetric> usedMetrics) {
    Map<String, Long> sums = new LinkedHashMap<>();
    if (engineMetrics == null) {
      return sums;
    }
    for (IEngineMetric metric : usedMetrics) {
      if (metric == null || !metric.isNumeric() || metric.getCode() == null) {
        continue;
      }
      long sum = 0L;
      boolean any = false;
      for (IEngineComponent component : copies) {
        Long value = engineMetrics.getComponentMetric(component, metric);
        if (value != null) {
          any = true;
          sum += value;
        }
      }
      if (any) {
        sums.put(metric.getCode(), sum);
      }
    }
    return sums;
  }

  private static Long durationMs(List<IEngineComponent> copies, long nowMs) {
    Date start = null;
    Date end = null;
    boolean stillOpen = false;
    for (IEngineComponent component : copies) {
      Date first = component.getFirstRowReadDate();
      if (first == null) {
        continue;
      }
      if (start == null || first.before(start)) {
        start = first;
      }
      Date last = component.getLastRowWrittenDate();
      if (last == null) {
        stillOpen = true;
      } else if (end == null || last.after(end)) {
        end = last;
      }
    }
    if (start == null) {
      return null;
    }
    long endMs = stillOpen || end == null ? nowMs : end.getTime();
    return Math.max(0L, endMs - start.getTime());
  }

  private static SpeedAggregate speed(List<IEngineComponent> copies, EngineMetrics engineMetrics) {
    if (copies.size() == 1) {
      String raw = speedRaw(engineMetrics, copies.get(0));
      Double sample = parseSpeed(raw);
      String text = raw == null || raw.isBlank() ? "" : raw.trim();
      Long value = sample == null ? null : Math.round(sample);
      return new SpeedAggregate(value, text);
    }
    double total = 0d;
    boolean any = false;
    for (IEngineComponent component : copies) {
      Double sample = parseSpeed(speedRaw(engineMetrics, component));
      if (sample != null) {
        any = true;
        total += sample;
      }
    }
    if (!any) {
      return new SpeedAggregate(null, "-");
    }
    long rounded = Math.round(total);
    return new SpeedAggregate(rounded, new DecimalFormat(SPEED_PATTERN).format(rounded));
  }

  private static String speedRaw(EngineMetrics engineMetrics, IEngineComponent component) {
    if (engineMetrics == null || engineMetrics.getComponentSpeedMap() == null) {
      return null;
    }
    return engineMetrics.getComponentSpeedMap().get(component);
  }

  /**
   * {@code -} and blank are "no sample", not zero. Grouped integers ({@code 1,000}) and decimals
   * are accepted. A trailing unit such as {@code rows/s} is ignored.
   */
  static Double parseSpeed(String raw) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty() || "-".equals(trimmed)) {
      return null;
    }
    trimmed = trimmed.replace(",", "");
    trimmed = trimmed.replaceAll("(?i)\\s+(rows/s|rows|runs|flushes|r)$", "");
    if (trimmed.isEmpty() || "-".equals(trimmed)) {
      return null;
    }
    try {
      return Double.parseDouble(trimmed);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String statusText(EngineMetrics engineMetrics, IEngineComponent component) {
    if (engineMetrics == null || engineMetrics.getComponentStatusMap() == null) {
      return "";
    }
    String status = engineMetrics.getComponentStatusMap().get(component);
    return status == null ? "" : status;
  }

  private static List<String> statusTexts(
      List<IEngineComponent> copies, EngineMetrics engineMetrics) {
    List<String> texts = new ArrayList<>();
    for (IEngineComponent component : copies) {
      texts.add(statusText(engineMetrics, component));
    }
    return texts;
  }

  static String aggregateStatus(List<String> descriptions) {
    String bestText = null;
    int bestRank = Integer.MAX_VALUE;
    String firstUnknown = null;
    int knownCount = 0;
    if (descriptions != null) {
      for (String description : descriptions) {
        if (description == null || description.isEmpty()) {
          continue;
        }
        ComponentExecutionStatus parsed =
            ComponentExecutionStatus.getStatusFromDescription(description);
        boolean known =
            parsed.getDescription() != null
                && parsed.getDescription().equalsIgnoreCase(description);
        if (!known) {
          if (firstUnknown == null) {
            firstUnknown = description;
          }
          continue;
        }
        knownCount++;
        int rank = STATUS_PRIORITY.indexOf(parsed);
        if (rank >= 0 && rank < bestRank) {
          bestRank = rank;
          bestText = description;
        }
      }
    }
    if (knownCount > 0) {
      return bestText == null ? "" : bestText;
    }
    return firstUnknown == null ? "" : firstUnknown;
  }

  private static int compare(
      PipelineMetricsNode left,
      PipelineMetricsNode right,
      int column,
      List<IEngineMetric> usedMetrics) {
    if (column == COLUMN_NAME) {
      return left.getTransformName().compareToIgnoreCase(right.getTransformName());
    }
    if (column == COLUMN_COPY) {
      return Integer.compare(copySortKey(left), copySortKey(right));
    }
    int durationColumn = durationColumn(usedMetrics);
    if (column == durationColumn) {
      return Long.compare(durationSortKey(left), durationSortKey(right));
    }
    if (column == durationColumn + 1) {
      return Long.compare(speedSortKey(left), speedSortKey(right));
    }
    if (column == durationColumn + 2) {
      return left.getStatus().compareToIgnoreCase(right.getStatus());
    }
    int metricIndex = column - LEADING_COLUMNS;
    if (metricIndex >= 0 && metricIndex < usedMetrics.size()) {
      IEngineMetric metric = usedMetrics.get(metricIndex);
      String code = metric == null ? null : metric.getCode();
      return Long.compare(metricSortKey(left, code), metricSortKey(right, code));
    }
    return 0;
  }

  private static int copySortKey(PipelineMetricsNode node) {
    return node.isAggregate() ? node.getCopyCount() : node.getCopyNr();
  }

  private static long durationSortKey(PipelineMetricsNode node) {
    return node.getDurationMs() == null ? 0L : node.getDurationMs();
  }

  private static long speedSortKey(PipelineMetricsNode node) {
    return node.getSpeedValue() == null ? 0L : node.getSpeedValue();
  }

  private static long metricSortKey(PipelineMetricsNode node, String code) {
    if (code == null) {
      return 0L;
    }
    Long value = node.getMetricSums().get(code);
    return value == null ? 0L : value;
  }

  private static final class SpeedAggregate {
    private final Long value;
    private final String text;

    private SpeedAggregate(Long value, String text) {
      this.value = value;
      this.text = text;
    }
  }
}
