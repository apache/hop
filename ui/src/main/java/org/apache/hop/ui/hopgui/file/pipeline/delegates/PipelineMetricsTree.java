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
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.PipelineMetricDisplayUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/** Fills the pipeline metrics tree from grouped rows and keeps expand state across refreshes. */
final class PipelineMetricsTree {

  private static final Class<?> PKG = HopGui.class;

  private static final String METRICS_PATTERN = " ###,###,###,###";

  /** Item data key for the "number of copies" tooltip shown on an aggregate row. */
  static final String COPY_TOOLTIP_KEY = "pipelineMetricsCopyCountTooltip";

  private static final String ERROR_PAINT_KEY = "pipelineMetricsErrorPaint";

  private PipelineMetricsTree() {}

  /**
   * Identity of one tree row. A child carries the transform name so open-transform does not parse
   * the label.
   */
  static final class RowKey {
    final String transformName;
    final int copyNr;
    final boolean aggregate;

    RowKey(String transformName, int copyNr, boolean aggregate) {
      this.transformName = transformName == null ? "" : transformName;
      this.copyNr = copyNr;
      this.aggregate = aggregate;
    }

    boolean matches(RowKey other) {
      return other != null
          && aggregate == other.aggregate
          && copyNr == other.copyNr
          && transformName.equals(other.transformName);
    }
  }

  /**
   * Updates {@code tree} to show {@code nodes}. When the names and copy numbers already match, only
   * changed cell text is written so the scroll position and which rows are expanded stay put.
   * Otherwise the tree is rebuilt and expand state comes from {@link TreeMemory}. New parents stay
   * collapsed.
   */
  static void sync(
      Tree tree,
      List<PipelineMetricsNode> nodes,
      List<IEngineMetric> usedMetrics,
      boolean showUnits,
      String treeMemoryName) {
    if (tree == null || tree.isDisposed()) {
      return;
    }
    List<PipelineMetricsNode> rows = nodes == null ? List.of() : nodes;
    List<IEngineMetric> metrics = usedMetrics == null ? List.of() : usedMetrics;
    if (sameStructure(tree, rows)) {
      tree.setRedraw(false);
      try {
        updateItems(tree.getItems(), rows, metrics, showUnits);
      } finally {
        if (!tree.isDisposed()) {
          tree.setRedraw(true);
        }
      }
      return;
    }

    List<RowKey> selected = captureSelection(tree);
    RowKey top = captureTop(tree);
    tree.setRedraw(false);
    try {
      tree.removeAll();
      for (PipelineMetricsNode node : rows) {
        TreeItem item = new TreeItem(tree, SWT.NONE);
        fillItem(item, node, false, metrics, showUnits);
        for (PipelineMetricsNode child : node.getChildren()) {
          TreeItem childItem = new TreeItem(item, SWT.NONE);
          fillItem(childItem, child, true, metrics, showUnits);
        }
      }
      if (treeMemoryName != null) {
        TreeMemory.setExpandedFromMemory(tree, treeMemoryName);
      }
      restoreSelection(tree, selected);
      restoreTop(tree, top);
    } finally {
      if (!tree.isDisposed()) {
        tree.setRedraw(true);
      }
    }
  }

  /**
   * Header row plus one TSV line per selected item. With nothing selected, every row is included,
   * collapsed children too.
   */
  static String copyText(Tree tree) {
    if (tree == null || tree.isDisposed() || tree.getColumnCount() == 0) {
      return null;
    }
    TreeItem[] selected = tree.getSelection();
    List<TreeItem> items = new ArrayList<>();
    if (selected == null || selected.length == 0) {
      collect(tree.getItems(), items);
    } else {
      for (TreeItem item : selected) {
        if (item != null && !item.isDisposed()) {
          items.add(item);
        }
      }
    }
    StringBuilder text = new StringBuilder();
    appendRow(text, header(tree));
    for (TreeItem item : items) {
      appendRow(text, itemTexts(item));
    }
    return text.toString();
  }

  static List<String> cellTexts(
      PipelineMetricsNode node,
      boolean childRow,
      List<IEngineMetric> usedMetrics,
      boolean showUnits) {
    List<IEngineMetric> metrics = usedMetrics == null ? List.of() : usedMetrics;
    List<String> cells = new ArrayList<>();
    if (childRow) {
      cells.add(BaseMessages.getString(PKG, "PipelineLog.Tree.Copy", node.getCopyNr()));
    } else {
      cells.add(node.getTransformName());
    }
    if (node.isAggregate()) {
      cells.add(Integer.toString(node.getCopyCount()));
    } else {
      cells.add(Integer.toString(node.getCopyNr()));
    }

    DecimalFormat metricFormat = new DecimalFormat(METRICS_PATTERN);
    for (IEngineMetric metric : metrics) {
      String code = metric == null ? null : metric.getCode();
      Long value = code == null ? null : node.getMetricSums().get(code);
      cells.add(formatMetric(metric, value, metricFormat, showUnits));
    }
    cells.add(formatDuration(node.getDurationMs(), showUnits));
    cells.add(formatSpeed(node.getSpeedText(), showUnits));
    cells.add(node.getStatus());
    return cells;
  }

  /** Shows the copy-count tooltip while the pointer is over that cell of an aggregate row. */
  static void installCopyCountTooltip(Tree tree) {
    tree.addListener(
        SWT.MouseMove,
        event -> {
          if (tree.isDisposed()) {
            return;
          }
          TreeItem item = tree.getItem(new Point(event.x, event.y));
          String tooltip = null;
          if (item != null && !item.isDisposed()) {
            int column = columnAt(item, event.x);
            if (column == PipelineMetricsGrouper.COLUMN_COPY) {
              Object value = item.getData(COPY_TOOLTIP_KEY);
              if (value instanceof String text && !text.isEmpty()) {
                tooltip = text;
              }
            }
          }
          if ((tooltip == null && tree.getToolTipText() != null)
              || (tooltip != null && !tooltip.equals(tree.getToolTipText()))) {
            tree.setToolTipText(tooltip);
          }
        });
    tree.addListener(
        SWT.MouseExit,
        event -> {
          if (!tree.isDisposed() && tree.getToolTipText() != null) {
            tree.setToolTipText(null);
          }
        });
  }

  private static boolean sameStructure(Tree tree, List<PipelineMetricsNode> nodes) {
    TreeItem[] items = tree.getItems();
    if (items.length != nodes.size()) {
      return false;
    }
    for (int i = 0; i < nodes.size(); i++) {
      if (!sameItem(items[i], nodes.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean sameItem(TreeItem item, PipelineMetricsNode node) {
    if (!(item.getData() instanceof RowKey key) || !key.matches(rowKey(node))) {
      return false;
    }
    TreeItem[] children = item.getItems();
    List<PipelineMetricsNode> expected = node.getChildren();
    if (children.length != expected.size()) {
      return false;
    }
    for (int i = 0; i < expected.size(); i++) {
      if (!(children[i].getData() instanceof RowKey childKey)
          || !childKey.matches(rowKey(expected.get(i)))) {
        return false;
      }
    }
    return true;
  }

  private static void updateItems(
      TreeItem[] items,
      List<PipelineMetricsNode> nodes,
      List<IEngineMetric> usedMetrics,
      boolean showUnits) {
    for (int i = 0; i < nodes.size(); i++) {
      PipelineMetricsNode node = nodes.get(i);
      fillItem(items[i], node, false, usedMetrics, showUnits);
      List<PipelineMetricsNode> children = node.getChildren();
      for (int c = 0; c < children.size(); c++) {
        fillItem(items[i].getItem(c), children.get(c), true, usedMetrics, showUnits);
      }
    }
  }

  private static void fillItem(
      TreeItem item,
      PipelineMetricsNode node,
      boolean childRow,
      List<IEngineMetric> usedMetrics,
      boolean showUnits) {
    item.setData(rowKey(node));
    if (node.isAggregate()) {
      item.setData(
          COPY_TOOLTIP_KEY, BaseMessages.getString(PKG, "PipelineLog.Tree.CopyCount.Tooltip"));
    } else if (item.getData(COPY_TOOLTIP_KEY) != null) {
      item.setData(COPY_TOOLTIP_KEY, null);
    }
    applyTexts(item, cellTexts(node, childRow, usedMetrics, showUnits));
    paintError(item, node);
  }

  private static RowKey rowKey(PipelineMetricsNode node) {
    return new RowKey(node.getTransformName(), node.getCopyNr(), node.isAggregate());
  }

  private static void applyTexts(TreeItem item, List<String> texts) {
    Tree tree = item.getParent();
    int columns = Math.min(texts.size(), tree.getColumnCount());
    for (int column = 0; column < columns; column++) {
      String next = Const.NVL(texts.get(column), "");
      if (!next.equals(item.getText(column))) {
        item.setText(column, next);
      }
    }
  }

  /**
   * Whole-row background, the same light red the flat grid used. Per-cell {@code setBackground} on
   * GTK installs a cell renderer that stretches later rows, so this stays on the item.
   */
  private static void paintError(TreeItem item, PipelineMetricsNode node) {
    Long errors = node.getMetricSums().get(Pipeline.METRIC_NAME_ERROR);
    boolean wantError = errors != null && errors > 0;
    boolean painted = Boolean.TRUE.equals(item.getData(ERROR_PAINT_KEY));
    if (wantError == painted) {
      return;
    }
    if (wantError) {
      item.setBackground(GuiResource.getInstance().getColorLightRed());
    } else {
      item.setBackground(null);
    }
    item.setData(ERROR_PAINT_KEY, wantError);
  }

  private static String formatMetric(
      IEngineMetric metric, Long value, DecimalFormat metricFormat, boolean showUnits) {
    if (metric == null) {
      return "";
    }
    String code = metric.getCode();
    if (Pipeline.METRIC_NAME_DATA_VOLUME.equals(code)
        || Pipeline.METRIC_NAME_DATA_VOLUME_IN.equals(code)
        || Pipeline.METRIC_NAME_DATA_VOLUME_OUT.equals(code)) {
      return value == null ? "" : PipelineMetricDisplayUtil.formatDataVolume(value);
    }
    if (value == null) {
      return "";
    }
    String unit = showUnits ? PipelineMetricDisplayUtil.getUnitForMetricCell(metric) : null;
    String text = metricFormat.format(value);
    if (unit != null && !unit.isEmpty()) {
      text = text + " " + unit;
    }
    return text;
  }

  private static String formatDuration(Long durationMs, boolean showUnits) {
    if (durationMs == null) {
      return "";
    }
    String text = Utils.getDurationHMS(durationMs / 1000.0);
    return showUnits ? text + " (h:m:s)" : text;
  }

  private static String formatSpeed(String speedText, boolean showUnits) {
    if (speedText == null || speedText.isEmpty()) {
      return "";
    }
    String trimmed = speedText.trim();
    if (showUnits && !"-".equals(trimmed)) {
      return trimmed + " rows/s";
    }
    return trimmed;
  }

  private static List<RowKey> captureSelection(Tree tree) {
    List<RowKey> keys = new ArrayList<>();
    TreeItem[] selection = tree.getSelection();
    if (selection == null) {
      return keys;
    }
    for (TreeItem item : selection) {
      if (item.getData() instanceof RowKey key) {
        keys.add(key);
      }
    }
    return keys;
  }

  private static RowKey captureTop(Tree tree) {
    TreeItem top = tree.getTopItem();
    if (top != null && top.getData() instanceof RowKey key) {
      return key;
    }
    return null;
  }

  private static void restoreSelection(Tree tree, List<RowKey> selected) {
    if (selected.isEmpty()) {
      return;
    }
    List<TreeItem> matches = new ArrayList<>();
    collectMatching(tree.getItems(), selected, matches);
    if (!matches.isEmpty()) {
      tree.setSelection(matches.toArray(new TreeItem[0]));
    }
  }

  private static void restoreTop(Tree tree, RowKey top) {
    if (top == null) {
      return;
    }
    TreeItem match = find(tree.getItems(), top);
    if (match != null) {
      tree.setTopItem(match);
    }
  }

  private static void collectMatching(
      TreeItem[] items, List<RowKey> selected, List<TreeItem> matches) {
    for (TreeItem item : items) {
      if (item.getData() instanceof RowKey key && contains(selected, key)) {
        matches.add(item);
      }
      collectMatching(item.getItems(), selected, matches);
    }
  }

  private static boolean contains(List<RowKey> selected, RowKey key) {
    for (RowKey candidate : selected) {
      if (candidate.matches(key)) {
        return true;
      }
    }
    return false;
  }

  private static TreeItem find(TreeItem[] items, RowKey key) {
    for (TreeItem item : items) {
      if (item.getData() instanceof RowKey candidate && candidate.matches(key)) {
        return item;
      }
      TreeItem nested = find(item.getItems(), key);
      if (nested != null) {
        return nested;
      }
    }
    return null;
  }

  private static void collect(TreeItem[] items, List<TreeItem> into) {
    for (TreeItem item : items) {
      into.add(item);
      collect(item.getItems(), into);
    }
  }

  private static List<String> header(Tree tree) {
    List<String> cells = new ArrayList<>();
    for (int column = 0; column < tree.getColumnCount(); column++) {
      cells.add(Const.NVL(tree.getColumn(column).getText(), ""));
    }
    return cells;
  }

  private static List<String> itemTexts(TreeItem item) {
    List<String> cells = new ArrayList<>();
    Tree tree = item.getParent();
    for (int column = 0; column < tree.getColumnCount(); column++) {
      cells.add(Const.NVL(item.getText(column), ""));
    }
    return cells;
  }

  private static void appendRow(StringBuilder text, List<String> cells) {
    for (int column = 0; column < cells.size(); column++) {
      if (column > 0) {
        text.append('\t');
      }
      text.append(cells.get(column));
    }
    text.append(Const.CR);
  }

  private static int columnAt(TreeItem item, int x) {
    Tree tree = item.getParent();
    for (int column = 0; column < tree.getColumnCount(); column++) {
      Rectangle bounds = item.getBounds(column);
      if (x >= bounds.x && x < bounds.x + bounds.width) {
        return column;
      }
    }
    return -1;
  }
}
