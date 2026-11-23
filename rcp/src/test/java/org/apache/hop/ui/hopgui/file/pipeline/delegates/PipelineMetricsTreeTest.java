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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The metrics tree starts collapsed and keeps an expanded transform across a refresh. */
@Tag("uitest")
class PipelineMetricsTreeTest extends SwtBotTestBase {

  private static final String TREE_NAME = "PipelineMetricsTreeTest";
  private static final List<IEngineMetric> METRICS =
      List.of(Pipeline.METRIC_INPUT, Pipeline.METRIC_ERROR);

  @Test
  void multiCopyTransformIsCollapsedWithTotalsAndExpandSurvivesRefresh() {
    AtomicReference<Tree> treeRef = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          TreeMemory.getInstance().clearTree(TREE_NAME);
          Tree tree = new Tree(shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI);
          tree.setHeaderVisible(true);
          int columns =
              PipelineMetricsGrouper.LEADING_COLUMNS
                  + METRICS.size()
                  + PipelineMetricsGrouper.TRAILING_COLUMNS;
          for (int i = 0; i < columns; i++) {
            TreeColumn column = new TreeColumn(tree, SWT.LEFT);
            column.setText("c" + i);
            column.setWidth(80);
          }
          TreeMemory.addTreeListener(tree, TREE_NAME);
          treeRef.set(tree);
          PipelineMetricsTree.sync(tree, rows(10L), METRICS, false, TREE_NAME);
        },
        bot ->
            display.syncExec(
                () -> {
                  Tree tree = treeRef.get();
                  tree.getShell().layout(true, true);

                  TreeItem before = tree.getItem(0);
                  TreeItem rest = tree.getItem(1);
                  TreeItem after = tree.getItem(2);
                  assertEquals("Before", before.getText(0));
                  assertEquals("After", after.getText(0));
                  assertFalse(rest.getExpanded());
                  assertEquals(4, rest.getItemCount());
                  assertEquals("4", rest.getText(1));
                  List<String> totals =
                      PipelineMetricsTree.cellTexts(rows(10L).get(1), false, METRICS, false);
                  assertEquals(totals.get(0), rest.getText(0));
                  assertEquals(totals.get(2), rest.getText(2));
                  for (int copy = 0; copy < 4; copy++) {
                    assertEquals("copy " + copy, rest.getItem(copy).getText(0));
                    assertEquals(Integer.toString(copy), rest.getItem(copy).getText(1));
                  }
                  assertNotNull(rest.getData(PipelineMetricsTree.COPY_TOOLTIP_KEY));

                  rest.setExpanded(true);
                  Event expanded = new Event();
                  expanded.item = rest;
                  tree.notifyListeners(SWT.Expand, expanded);
                  assertTrue(rest.getExpanded());

                  PipelineMetricsTree.sync(tree, rows(25L), METRICS, false, TREE_NAME);
                  rest = tree.getItem(1);
                  assertTrue(
                      rest.getExpanded(), "an expanded transform stays open on the next refresh");
                  assertEquals(
                      PipelineMetricsTree.cellTexts(rows(25L).get(1), false, METRICS, false).get(2),
                      rest.getText(2));

                  // A rebuilt tree (sort changed the parent order) still restores the open row.
                  List<PipelineMetricsNode> reordered = new ArrayList<>(rows(25L));
                  PipelineMetricsNode moved = reordered.remove(1);
                  reordered.add(moved);
                  PipelineMetricsTree.sync(tree, reordered, METRICS, false, TREE_NAME);
                  TreeItem restored = tree.getItem(2);
                  assertEquals("Rest", restored.getText(0));
                  assertTrue(restored.getExpanded());

                  TreeItem clearRow = tree.getItem(0);
                  int clearHeight = clearRow.getBounds().height;
                  int restHeight = restored.getBounds().height;
                  int followingHeight = tree.getItem(1).getBounds().height;
                  assertTrue(clearHeight > 0, "row has a height");
                  assertTrue(
                      restHeight < clearHeight * 2 + 8,
                      "error row height " + restHeight + " vs " + clearHeight);
                  assertTrue(
                      followingHeight < clearHeight * 2 + 8,
                      "row after an error row height " + followingHeight + " vs " + clearHeight);
                }));
  }

  /**
   * Before (no errors), Rest with four copies (errors on the total), After (no errors). The input
   * value is the same on every Rest copy so the total is four times {@code perCopy}.
   */
  private static List<PipelineMetricsNode> rows(long perCopy) {
    List<EngineComponent> copies = new ArrayList<>();
    EngineMetrics metrics = new EngineMetrics();
    for (int copy = 0; copy < 4; copy++) {
      EngineComponent component = new EngineComponent("Rest", copy);
      copies.add(component);
      metrics.addComponent(component);
      metrics.setComponentMetric(component, Pipeline.METRIC_INPUT, perCopy);
      if (copy == 0) {
        metrics.setComponentMetric(component, Pipeline.METRIC_ERROR, 2L);
      }
    }
    EngineComponent before = new EngineComponent("Before", 0);
    EngineComponent after = new EngineComponent("After", 0);
    metrics.addComponent(before);
    metrics.addComponent(after);
    metrics.setComponentMetric(before, Pipeline.METRIC_INPUT, 1L);
    metrics.setComponentMetric(after, Pipeline.METRIC_INPUT, 1L);

    List<IEngineComponent> shown = new ArrayList<>();
    shown.add(before);
    shown.addAll(copies);
    shown.add(after);
    return PipelineMetricsGrouper.group(shown, metrics, METRICS, 0L);
  }
}
