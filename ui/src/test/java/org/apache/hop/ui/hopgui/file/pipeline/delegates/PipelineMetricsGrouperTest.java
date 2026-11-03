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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.EngineMetric;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.junit.jupiter.api.Test;

class PipelineMetricsGrouperTest {

  private static final IEngineMetric INPUT = Pipeline.METRIC_INPUT;
  private static final IEngineMetric ERRORS = Pipeline.METRIC_ERROR;
  private static final long NOW = 10_000L;

  @Test
  void singleCopyStaysALeaf() {
    EngineComponent only = component("Read", 0);
    EngineMetrics metrics = metrics(only, INPUT, 12L, "Running", " 40");

    List<PipelineMetricsNode> nodes =
        PipelineMetricsGrouper.group(List.of(only), metrics, List.of(INPUT), NOW);

    assertEquals(1, nodes.size());
    PipelineMetricsNode leaf = nodes.get(0);
    assertFalse(leaf.isAggregate());
    assertTrue(leaf.getChildren().isEmpty());
    assertEquals(0, leaf.getCopyNr());
    assertEquals(12L, leaf.getMetricSums().get(Pipeline.METRIC_NAME_INPUT));
    assertEquals("40", leaf.getSpeedText());
    assertEquals("Running", leaf.getStatus());
  }

  @Test
  void severalCopiesCollapseToTotalsAndKeepPerCopyRows() {
    EngineComponent second = component("Rest", 2);
    EngineComponent first = component("Rest", 0);
    EngineComponent third = component("Rest", 1);
    EngineComponent fourth = component("Rest", 3);
    EngineMetrics metrics = new EngineMetrics();
    metric(metrics, second, INPUT, 5L);
    metric(metrics, first, INPUT, 10L);
    metric(metrics, fourth, INPUT, null);
    metric(metrics, third, ERRORS, 2L);
    metrics.setComponentSpeed(first, " 1,000");
    metrics.setComponentSpeed(third, "-");
    metrics.setComponentSpeed(second, "250");
    metrics.setComponentSpeed(fourth, "500");
    metrics.setComponentStatus(first, status(ComponentExecutionStatus.STATUS_FINISHED));
    metrics.setComponentStatus(third, status(ComponentExecutionStatus.STATUS_RUNNING));
    metrics.setComponentStatus(second, status(ComponentExecutionStatus.STATUS_FINISHED));
    metrics.setComponentStatus(fourth, status(ComponentExecutionStatus.STATUS_FINISHED));

    List<PipelineMetricsNode> nodes =
        PipelineMetricsGrouper.group(
            List.of(second, first, third, fourth), metrics, List.of(INPUT, ERRORS), NOW);

    assertEquals(1, nodes.size());
    PipelineMetricsNode parent = nodes.get(0);
    assertTrue(parent.isAggregate());
    assertEquals(4, parent.getCopyCount());
    assertEquals(15L, parent.getMetricSums().get(Pipeline.METRIC_NAME_INPUT));
    assertEquals(2L, parent.getMetricSums().get(Pipeline.METRIC_NAME_ERROR));
    assertEquals(1750L, parent.getSpeedValue());
    assertEquals(new DecimalFormat("#,###,###,###,##0").format(1750), parent.getSpeedText());
    assertEquals(status(ComponentExecutionStatus.STATUS_RUNNING), parent.getStatus());

    assertEquals(4, parent.getChildren().size());
    for (int copy = 0; copy < 4; copy++) {
      PipelineMetricsNode child = parent.getChildren().get(copy);
      assertFalse(child.isAggregate());
      assertEquals(copy, child.getCopyNr());
      assertEquals("Rest", child.getTransformName());
    }
    assertNull(parent.getChildren().get(3).getMetricSums().get(Pipeline.METRIC_NAME_INPUT));
    assertFalse(parent.getMetricSums().containsKey("not-a-metric"));
  }

  @Test
  void allMissingValuesStayBlankAndSpeedWithoutSamplesIsUnavailable() {
    EngineComponent first = component("Rest", 0);
    EngineComponent second = component("Rest", 1);
    EngineMetrics metrics = new EngineMetrics();
    metrics.setComponentSpeed(first, "-");
    metrics.setComponentSpeed(second, " ");

    List<PipelineMetricsNode> nodes =
        PipelineMetricsGrouper.group(List.of(first, second), metrics, List.of(INPUT), NOW);

    PipelineMetricsNode parent = nodes.get(0);
    assertNull(parent.getMetricSums().get(Pipeline.METRIC_NAME_INPUT));
    assertNull(parent.getSpeedValue());
    assertEquals("-", parent.getSpeedText());
    assertNull(parent.getDurationMs());
  }

  @Test
  void durationUsesTheEarliestStartAndStaysOpenWhileACopyIsRunning() {
    EngineComponent finished = component("Rest", 0);
    finished.setFirstRowReadDate(new Date(1_000L));
    finished.setLastRowWrittenDate(new Date(4_000L));
    EngineComponent running = component("Rest", 1);
    running.setFirstRowReadDate(new Date(2_000L));
    EngineMetrics metrics = new EngineMetrics();

    PipelineMetricsNode parent =
        PipelineMetricsGrouper.group(List.of(finished, running), metrics, List.of(INPUT), NOW)
            .get(0);

    assertEquals(NOW - 1_000L, parent.getDurationMs());
    assertEquals(3_000L, parent.getChildren().get(0).getDurationMs());
    assertEquals(NOW - 2_000L, parent.getChildren().get(1).getDurationMs());
  }

  @Test
  void durationUsesTheLatestFinishWhenEveryCopyHasClosed() {
    EngineComponent first = component("Rest", 0);
    first.setFirstRowReadDate(new Date(1_000L));
    first.setLastRowWrittenDate(new Date(4_000L));
    EngineComponent second = component("Rest", 1);
    second.setFirstRowReadDate(new Date(500L));
    second.setLastRowWrittenDate(new Date(8_000L));

    PipelineMetricsNode parent =
        PipelineMetricsGrouper.group(
                List.of(first, second), new EngineMetrics(), List.of(INPUT), NOW)
            .get(0);

    assertEquals(7_500L, parent.getDurationMs());
  }

  @Test
  void haltedBeatsRunningAndAnUnknownStatusIsKeptOnlyOnItsOwn() {
    assertEquals(
        status(ComponentExecutionStatus.STATUS_HALTED),
        PipelineMetricsGrouper.aggregateStatus(
            List.of(
                status(ComponentExecutionStatus.STATUS_RUNNING),
                status(ComponentExecutionStatus.STATUS_HALTED))));
    assertEquals(
        status(ComponentExecutionStatus.STATUS_FINISHED),
        PipelineMetricsGrouper.aggregateStatus(
            List.of(status(ComponentExecutionStatus.STATUS_FINISHED), "not-a-status")));
    assertEquals("not-a-status", PipelineMetricsGrouper.aggregateStatus(List.of("not-a-status")));
  }

  @Test
  void oneRemainingCopyIsALeaf() {
    EngineComponent only = component("Rest", 3);
    List<PipelineMetricsNode> nodes =
        PipelineMetricsGrouper.group(List.of(only), new EngineMetrics(), List.of(INPUT), NOW);

    assertEquals(1, nodes.size());
    assertFalse(nodes.get(0).isAggregate());
    assertEquals(3, nodes.get(0).getCopyNr());
  }

  @Test
  void nonNumericMetricsAreNotSummed() {
    IEngineMetric note = new EngineMetric("note", "Note", "", "999", false);
    EngineComponent first = component("Rest", 0);
    EngineComponent second = component("Rest", 1);
    EngineMetrics metrics = new EngineMetrics();
    metrics.setComponentMetric(first, note, 1L);
    metrics.setComponentMetric(second, note, 1L);

    PipelineMetricsNode parent =
        PipelineMetricsGrouper.group(List.of(first, second), metrics, List.of(note), NOW).get(0);

    assertTrue(parent.getMetricSums().isEmpty());
  }

  @Test
  void parentSortDoesNotReorderChildrenAndKeepsPipelineOrderOnTies() {
    EngineComponent beta = component("Beta", 0);
    EngineComponent alphaHigh = component("Alpha", 1);
    EngineComponent alphaLow = component("Alpha", 0);
    EngineComponent gamma = component("Gamma", 2);
    EngineMetrics metrics = new EngineMetrics();
    metric(metrics, beta, INPUT, 5L);
    metric(metrics, alphaHigh, INPUT, 5L);
    metric(metrics, alphaLow, INPUT, 9L);
    metric(metrics, gamma, INPUT, 5L);

    List<PipelineMetricsNode> nodes =
        new ArrayList<>(
            PipelineMetricsGrouper.group(
                List.of(beta, alphaHigh, alphaLow, gamma), metrics, List.of(INPUT), NOW));
    assertEquals(List.of("Beta", "Alpha", "Gamma"), names(nodes));
    assertEquals(List.of(0, 1), copyNumbers(nodes.get(1)));

    PipelineMetricsGrouper.sortParents(
        nodes, PipelineMetricsGrouper.COLUMN_NAME, true, List.of(INPUT));
    assertEquals(List.of("Gamma", "Beta", "Alpha"), names(nodes));
    assertEquals(List.of(0, 1), copyNumbers(nodes.get(2)));

    List<PipelineMetricsNode> byInput =
        new ArrayList<>(
            PipelineMetricsGrouper.group(
                List.of(beta, alphaHigh, alphaLow, gamma), metrics, List.of(INPUT), NOW));
    PipelineMetricsGrouper.sortParents(
        byInput, PipelineMetricsGrouper.LEADING_COLUMNS, false, List.of(INPUT));
    // Beta and Gamma both total 5, so they stay in pipeline order ahead of Alpha's 14.
    assertEquals(List.of("Beta", "Gamma", "Alpha"), names(byInput));
    assertEquals(List.of(0, 1), copyNumbers(byInput.get(2)));
  }

  @Test
  void childLabelAndCopyCountAreFormattedForTheTree() {
    EngineComponent first = component("Rest", 0);
    EngineComponent second = component("Rest", 1);
    EngineMetrics metrics = new EngineMetrics();
    metric(metrics, first, INPUT, 10L);
    metric(metrics, second, INPUT, 20L);
    PipelineMetricsNode parent =
        PipelineMetricsGrouper.group(List.of(first, second), metrics, List.of(INPUT), NOW).get(0);

    List<String> parentCells = PipelineMetricsTree.cellTexts(parent, false, List.of(INPUT), false);
    List<String> childCells =
        PipelineMetricsTree.cellTexts(parent.getChildren().get(0), true, List.of(INPUT), false);

    assertEquals("Rest", parentCells.get(0));
    assertEquals("2", parentCells.get(1));
    assertEquals("copy 0", childCells.get(0));
    assertEquals("0", childCells.get(1));
    assertTrue(parentCells.get(2).replace(",", "").replace(" ", "").contains("30"));
  }

  private static List<String> names(List<PipelineMetricsNode> nodes) {
    List<String> names = new ArrayList<>();
    for (PipelineMetricsNode node : nodes) {
      names.add(node.getTransformName());
    }
    return names;
  }

  private static List<Integer> copyNumbers(PipelineMetricsNode node) {
    List<Integer> copies = new ArrayList<>();
    for (PipelineMetricsNode child : node.getChildren()) {
      copies.add(child.getCopyNr());
    }
    return copies;
  }

  private static String status(ComponentExecutionStatus executionStatus) {
    return executionStatus.getDescription();
  }

  private static EngineComponent component(String name, int copyNr) {
    return new EngineComponent(name, copyNr);
  }

  private static EngineMetrics metrics(
      EngineComponent component, IEngineMetric metric, long value, String status, String speed) {
    EngineMetrics metrics = new EngineMetrics();
    metric(metrics, component, metric, value);
    metrics.setComponentStatus(component, status);
    metrics.setComponentSpeed(component, speed);
    return metrics;
  }

  private static void metric(
      EngineMetrics metrics, IEngineComponent component, IEngineMetric metric, Long value) {
    metrics.addComponent(component);
    if (value != null) {
      metrics.setComponentMetric(component, metric, value);
    }
  }
}
