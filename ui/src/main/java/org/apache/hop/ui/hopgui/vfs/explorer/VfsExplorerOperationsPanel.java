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

package org.apache.hop.ui.hopgui.vfs.explorer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

/**
 * One-line status for the file explorer, expandable to the recent listings and file changes. Stop
 * interrupts the worker. It does not close the VFS file system.
 */
@GuiPlugin
public class VfsExplorerOperationsPanel extends Composite {

  public static final Class<?> PKG = VfsFileExplorer.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "VfsExplorerOperations-Toolbar";
  public static final String TOOLBAR_ITEM_STOP = "VfsExplorerOperations-Toolbar-10000-Stop";
  public static final String TOOLBAR_ITEM_CLEAR = "VfsExplorerOperations-Toolbar-10005-Clear";
  public static final String TOOLBAR_ITEM_MINIMIZE = "VfsExplorerOperations-Toolbar-10010-Minimize";

  public static final String GUI_PLUGIN_STATUS_TOOLBAR_PARENT_ID =
      "VfsExplorerOperationsStatus-Toolbar";
  public static final String TOOLBAR_ITEM_STATUS_STOP =
      "VfsExplorerOperationsStatus-Toolbar-10000-Stop";
  public static final String TOOLBAR_ITEM_STATUS_EXPAND =
      "VfsExplorerOperationsStatus-Toolbar-10010-Expand";

  static final int MAX_FINISHED_OPERATIONS = 50;

  private final VfsFileExplorer explorer;
  private final List<VfsExplorerOperation> operations = new ArrayList<>();
  private VfsListingCounts listingCounts = VfsListingCounts.NONE;
  private final Table table;
  private final GuiToolbarWidgets toolBarWidgets;
  private final GuiToolbarWidgets statusToolBarWidgets;
  @Getter private final Composite statusBar;
  private final Label statusLabel;
  private final Runnable tickElapsed = this::refreshElapsed;
  private boolean timerArmed;
  private boolean expanded;
  private Consumer<Boolean> expandedListener;

  public VfsExplorerOperationsPanel(
      Composite sashParent, Composite statusParent, VfsFileExplorer explorer) {
    super(sashParent, SWT.NONE);
    this.explorer = explorer;
    PropsUi.setLook(this);
    setLayout(new FormLayout());

    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(this, SWT.WRAP | SWT.RIGHT | SWT.HORIZONTAL);
    Control toolBar = toolBarContainer.getControl();
    toolBar.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(toolBar, PropsUi.WIDGET_STYLE_TOOLBAR);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolBar.pack();

    table = new Table(this, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE);
    PropsUi.setLook(table);
    table.setHeaderVisible(true);
    table.setLinesVisible(true);
    table.setLayoutData(
        new FormDataBuilder().top(toolBar, PropsUi.getMargin()).bottom().fullWidth().result());
    table.addListener(SWT.Selection, e -> updateStopEnablement());
    table.addListener(
        SWT.MouseMove,
        e -> {
          TableItem item = table.getItem(new Point(e.x, e.y));
          String tip = "";
          if (item != null && item.getData() instanceof VfsExplorerOperation operation) {
            tip = Const.NVL(operation.getDetail(), Const.NVL(operation.getErrorMessage(), ""));
          }
          if (!Objects.equals(tip, table.getToolTipText())) {
            table.setToolTipText(tip);
          }
        });

    addColumn(BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Column.Description"), 240);
    addColumn(BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Column.Location"), 180);
    addColumn(BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Column.Status"), 90);
    addColumn(BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Column.Elapsed"), 70);
    addColumn(BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Column.Error"), 220);

    statusBar = new Composite(statusParent, SWT.NONE);
    statusBar.setLayout(new FormLayout());
    PropsUi.setLook(statusBar, PropsUi.WIDGET_STYLE_TOOLBAR);

    IToolbarContainer statusToolBarContainer =
        ToolbarFacade.createToolbarContainer(statusBar, SWT.WRAP | SWT.RIGHT | SWT.HORIZONTAL);
    Control statusToolBar = statusToolBarContainer.getControl();
    statusToolBar.setLayoutData(new FormDataBuilder().top().right().bottom().result());
    PropsUi.setLook(statusToolBar, PropsUi.WIDGET_STYLE_TOOLBAR);
    statusToolBarWidgets = new GuiToolbarWidgets();
    statusToolBarWidgets.registerGuiPluginObject(this);
    statusToolBarWidgets.createToolbarWidgets(
        statusToolBarContainer, GUI_PLUGIN_STATUS_TOOLBAR_PARENT_ID);
    statusToolBar.pack();

    statusLabel = new Label(statusBar, SWT.LEFT);
    PropsUi.setLook(statusLabel, PropsUi.WIDGET_STYLE_TOOLBAR);
    statusLabel.setLayoutData(
        new FormDataBuilder()
            .left()
            .top()
            .bottom()
            .right(statusToolBar, -PropsUi.getMargin())
            .result());

    updateStatusLine();
    updateStopEnablement();
  }

  private void addColumn(String title, int width) {
    TableColumn column = new TableColumn(table, SWT.NONE);
    column.setText(title);
    column.setWidth(width);
  }

  public void setExpandedListener(Consumer<Boolean> expandedListener) {
    this.expandedListener = expandedListener;
  }

  public boolean isExpanded() {
    return expanded;
  }

  public void setExpanded(boolean expanded) {
    if (this.expanded == expanded) {
      return;
    }
    this.expanded = expanded;
    if (expandedListener != null) {
      expandedListener.accept(expanded);
    }
  }

  public VfsExplorerOperation addOperation(String description, String location) {
    VfsExplorerOperation operation = new VfsExplorerOperation(description, location);
    operations.add(0, operation);
    trimFinished(operations, MAX_FINISHED_OPERATIONS);
    rebuildTable();
    table.setSelection(0);
    updateStatusLine();
    updateStopEnablement();
    armTimer();
    return operation;
  }

  public void setListingCounts(VfsListingCounts listingCounts) {
    this.listingCounts = listingCounts == null ? VfsListingCounts.NONE : listingCounts;
  }

  public void refresh() {
    if (isDisposed()) {
      return;
    }
    for (int i = 0; i < operations.size() && i < table.getItemCount(); i++) {
      fillItem(table.getItem(i), operations.get(i));
    }
    updateStatusLine();
    updateStopEnablement();
    armTimer();
  }

  public void cancelRunning() {
    for (VfsExplorerOperation operation : operations) {
      if (!operation.isFinished()) {
        operation.cancel();
      }
    }
    refresh();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_STOP,
      toolTip = "i18n::VfsFileExplorer.Operations.Stop.Tooltip",
      image = "ui/images/stop.svg")
  public void stopSelected() {
    VfsExplorerOperation selected = selectedOperation();
    if (selected != null && !selected.isFinished()) {
      explorer.cancelListings();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CLEAR,
      toolTip = "i18n::VfsFileExplorer.Operations.Clear.Tooltip",
      image = "ui/images/clear.svg")
  public void clearFinished() {
    operations.removeIf(VfsExplorerOperation::isFinished);
    rebuildTable();
    updateStatusLine();
    updateStopEnablement();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_MINIMIZE,
      toolTip = "i18n::VfsFileExplorer.Operations.Minimize.Tooltip",
      image = "ui/images/minimize-panel.svg")
  public void minimize() {
    setExpanded(false);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_STATUS_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_STATUS_STOP,
      toolTip = "i18n::VfsFileExplorer.Operations.Stop.Tooltip",
      image = "ui/images/stop.svg")
  public void stopCurrent() {
    if (currentOperation() != null) {
      explorer.cancelListings();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_STATUS_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_STATUS_EXPAND,
      toolTip = "i18n::VfsFileExplorer.Operations.Expand.Tooltip",
      image = "ui/images/maximize-panel.svg")
  public void expand() {
    setExpanded(true);
  }

  private void rebuildTable() {
    if (table.isDisposed()) {
      return;
    }
    table.removeAll();
    for (VfsExplorerOperation operation : operations) {
      TableItem item = new TableItem(table, SWT.NONE);
      fillItem(item, operation);
    }
  }

  static void trimFinished(List<VfsExplorerOperation> operations, int maxFinished) {
    int finished = 0;
    for (int i = 0; i < operations.size(); i++) {
      if (!operations.get(i).isFinished()) {
        continue;
      }
      finished++;
      if (finished > maxFinished) {
        operations.remove(i);
        i--;
      }
    }
  }

  private void refreshElapsed() {
    timerArmed = false;
    if (isDisposed()) {
      return;
    }
    boolean anyRunning = false;
    for (int i = 0; i < operations.size() && i < table.getItemCount(); i++) {
      VfsExplorerOperation operation = operations.get(i);
      table.getItem(i).setText(3, formatElapsed(operation.elapsedMillis()));
      if (!operation.isFinished()) {
        anyRunning = true;
      }
    }
    updateStatusLine();
    if (anyRunning) {
      armTimer();
    }
  }

  private void armTimer() {
    if (timerArmed || isDisposed()) {
      return;
    }
    boolean anyRunning = operations.stream().anyMatch(operation -> !operation.isFinished());
    if (!anyRunning) {
      return;
    }
    timerArmed = true;
    Display display = getDisplay();
    display.timerExec(500, tickElapsed);
  }

  private void fillItem(TableItem item, VfsExplorerOperation operation) {
    item.setText(0, operation.getDescription());
    item.setText(1, operation.getLocation());
    item.setText(2, statusLabel(operation));
    item.setText(3, formatElapsed(operation.elapsedMillis()));
    item.setText(4, Const.NVL(operation.getErrorMessage(), ""));
    item.setData(operation);
  }

  private void updateStatusLine() {
    if (statusLabel.isDisposed()) {
      return;
    }
    String text = formatStatusLine(currentOperation(), listingCounts);
    statusLabel.setText(text);
    statusLabel.setToolTipText(text);
  }

  VfsExplorerOperation currentOperation() {
    for (VfsExplorerOperation operation : operations) {
      if (!operation.isFinished()) {
        return operation;
      }
    }
    return operations.isEmpty() ? null : operations.get(0);
  }

  static String statusLabel(VfsExplorerOperation operation) {
    return switch (operation.getStatus()) {
      case RUNNING -> BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Status.Running");
      case DONE -> BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Status.Done");
      case FAILED -> BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Status.Failed");
      case CANCELLED -> BaseMessages.getString(PKG, "VfsFileExplorer.Operations.Status.Cancelled");
    };
  }

  static String formatStatusLine(VfsExplorerOperation operation) {
    return formatStatusLine(operation, VfsListingCounts.NONE);
  }

  /**
   * {@code Listing hdfs://some/folder - Done - 890ms - 200 files (2.1GB) - 10 files selected
   * (120MB)}. Milliseconds have no space before {@code ms}. Longer durations keep the operations
   * table format.
   */
  static String formatStatusLine(VfsExplorerOperation operation, VfsListingCounts counts) {
    if (operation == null) {
      return counts == null ? "" : stripLeadingSeparator(counts.statusSuffix());
    }
    StringBuilder line = new StringBuilder();
    line.append(Const.NVL(operation.getDescription(), ""));
    line.append(" - ").append(statusLabel(operation));
    line.append(" - ").append(formatStatusElapsed(operation.elapsedMillis()));
    if (counts != null) {
      line.append(counts.statusSuffix());
    }
    if (operation.getStatus() == VfsExplorerOperation.Status.FAILED
        && !Utils.isEmpty(operation.getErrorMessage())) {
      line.append(" - ").append(operation.getErrorMessage());
    }
    return line.toString();
  }

  static String formatStatusElapsed(long millis) {
    if (millis < 1000) {
      return millis + "ms";
    }
    return formatElapsed(millis);
  }

  private static String stripLeadingSeparator(String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return "";
    }
    return suffix.startsWith(" - ") ? suffix.substring(3) : suffix;
  }

  static String formatElapsed(long millis) {
    if (millis < 1000) {
      return millis + " ms";
    }
    if (millis < 60_000) {
      return String.format(Locale.ROOT, "%.1f s", millis / 1000.0);
    }
    long seconds = millis / 1000;
    return String.format(Locale.ROOT, "%d:%02d", seconds / 60, seconds % 60);
  }

  private VfsExplorerOperation selectedOperation() {
    TableItem[] selection = table.getSelection();
    if (selection.length != 1) {
      return null;
    }
    Object data = selection[0].getData();
    return data instanceof VfsExplorerOperation operation ? operation : null;
  }

  private void updateStopEnablement() {
    VfsExplorerOperation selected = selectedOperation();
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_STOP, selected != null && !selected.isFinished());
    VfsExplorerOperation current = currentOperation();
    statusToolBarWidgets.enableToolbarItem(
        TOOLBAR_ITEM_STATUS_STOP, current != null && !current.isFinished());
  }
}
