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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

/**
 * Database operations: a one-line status by default, expandable to a table with kill.
 *
 * <p>The table lives in the workbench sash. The status bar is a sibling composite so it stays
 * visible when the sash maximizes the editor tabs.
 */
@GuiPlugin
public class DatabaseOperationsPanel extends Composite {

  public static final Class<?> PKG = DatabasePerspective.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "DatabaseOperationsPanel-Toolbar";
  public static final String TOOLBAR_ITEM_KILL = "DatabaseOperations-Toolbar-10000-Kill";
  public static final String TOOLBAR_ITEM_MINIMIZE = "DatabaseOperations-Toolbar-10010-Minimize";

  public static final String GUI_PLUGIN_STATUS_TOOLBAR_PARENT_ID =
      "DatabaseOperationsStatus-Toolbar";
  public static final String TOOLBAR_ITEM_STATUS_KILL =
      "DatabaseOperationsStatus-Toolbar-10000-Kill";
  public static final String TOOLBAR_ITEM_STATUS_EXPAND =
      "DatabaseOperationsStatus-Toolbar-10010-Expand";

  private final List<DatabaseOperation> operations = new ArrayList<>();
  private final Table table;
  private final GuiToolbarWidgets toolBarWidgets;
  private final GuiToolbarWidgets statusToolBarWidgets;
  @Getter private final Composite statusBar;
  private final Label statusLabel;
  private final Runnable tickElapsed = this::refreshElapsed;
  private boolean timerArmed;
  private boolean expanded;
  private Consumer<Boolean> expandedListener;

  public DatabaseOperationsPanel(Composite sashParent, Composite statusParent) {
    super(sashParent, SWT.NONE);
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
    table.addListener(SWT.Selection, e -> updateKillEnablement());

    addColumn(
        BaseMessages.getString(PKG, "DatabasePerspective.Operations.Column.Description"), 280);
    addColumn(BaseMessages.getString(PKG, "DatabasePerspective.Operations.Column.Connection"), 140);
    addColumn(BaseMessages.getString(PKG, "DatabasePerspective.Operations.Column.Status"), 100);
    addColumn(BaseMessages.getString(PKG, "DatabasePerspective.Operations.Column.Elapsed"), 80);

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
    updateKillEnablement();
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

  public void addOperation(DatabaseOperation operation) {
    operations.add(0, operation);
    TableItem item = new TableItem(table, SWT.NONE, 0);
    fillItem(item, operation);
    table.setSelection(0);
    updateStatusLine();
    updateKillEnablement();
    armTimer();
  }

  public void refresh() {
    if (isDisposed()) {
      return;
    }
    for (int i = 0; i < operations.size() && i < table.getItemCount(); i++) {
      fillItem(table.getItem(i), operations.get(i));
    }
    updateStatusLine();
    updateKillEnablement();
    armTimer();
  }

  private void refreshElapsed() {
    timerArmed = false;
    if (isDisposed()) {
      return;
    }
    boolean anyRunning = false;
    for (int i = 0; i < operations.size() && i < table.getItemCount(); i++) {
      DatabaseOperation operation = operations.get(i);
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
    boolean anyRunning = operations.stream().anyMatch(op -> !op.isFinished());
    if (!anyRunning) {
      return;
    }
    timerArmed = true;
    Display display = getDisplay();
    display.timerExec(500, tickElapsed);
  }

  private void fillItem(TableItem item, DatabaseOperation operation) {
    item.setText(0, operation.getDescription());
    item.setText(1, operation.getConnectionName());
    item.setText(2, statusLabel(operation));
    item.setText(3, formatElapsed(operation.elapsedMillis()));
    item.setData(operation);
  }

  private void updateStatusLine() {
    if (statusLabel == null || statusLabel.isDisposed()) {
      return;
    }
    String text = formatStatusLine(currentOperation());
    statusLabel.setText(text);
    statusLabel.setToolTipText(text);
  }

  DatabaseOperation currentOperation() {
    for (DatabaseOperation operation : operations) {
      if (!operation.isFinished()) {
        return operation;
      }
    }
    return operations.isEmpty() ? null : operations.get(0);
  }

  static String statusLabel(DatabaseOperation operation) {
    return switch (operation.getStatus()) {
      case RUNNING -> BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Running");
      case DONE -> BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Done");
      case FAILED -> BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Failed");
      case CANCELLED ->
          BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Cancelled");
    };
  }

  static String formatStatusLine(DatabaseOperation operation) {
    if (operation == null) {
      return "";
    }
    StringBuilder line = new StringBuilder();
    line.append(Const.NVL(operation.getDescription(), ""));
    if (!Utils.isEmpty(operation.getConnectionName())) {
      line.append(" - ").append(operation.getConnectionName());
    }
    line.append(" - ").append(statusLabel(operation));
    line.append(" - ").append(formatElapsed(operation.elapsedMillis()));
    return line.toString();
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

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_KILL,
      toolTip = "i18n::DatabasePerspective.Operations.Kill.Tooltip",
      image = "ui/images/stop.svg")
  public void killSelected() {
    DatabaseOperation operation = selectedOperation();
    if (operation != null && !operation.isFinished()) {
      operation.cancel();
      refresh();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_MINIMIZE,
      toolTip = "i18n::DatabasePerspective.Operations.Minimize.Tooltip",
      image = "ui/images/minimize-panel.svg")
  public void minimize() {
    setExpanded(false);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_STATUS_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_STATUS_KILL,
      toolTip = "i18n::DatabasePerspective.Operations.KillCurrent.Tooltip",
      image = "ui/images/stop.svg")
  public void killCurrent() {
    DatabaseOperation operation = currentOperation();
    if (operation != null && !operation.isFinished()) {
      operation.cancel();
      refresh();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_STATUS_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_STATUS_EXPAND,
      toolTip = "i18n::DatabasePerspective.Operations.Expand.Tooltip",
      image = "ui/images/maximize-panel.svg")
  public void expand() {
    setExpanded(true);
  }

  private DatabaseOperation selectedOperation() {
    TableItem[] selection = table.getSelection();
    if (selection.length != 1) {
      return null;
    }
    Object data = selection[0].getData();
    return data instanceof DatabaseOperation operation ? operation : null;
  }

  private void updateKillEnablement() {
    DatabaseOperation selected = selectedOperation();
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_KILL, selected != null && !selected.isFinished());
    DatabaseOperation current = currentOperation();
    statusToolBarWidgets.enableToolbarItem(
        TOOLBAR_ITEM_STATUS_KILL, current != null && !current.isFinished());
  }

  public void cancelAll() {
    for (DatabaseOperation operation : operations) {
      if (!operation.isFinished()) {
        operation.cancel();
      }
    }
  }
}
