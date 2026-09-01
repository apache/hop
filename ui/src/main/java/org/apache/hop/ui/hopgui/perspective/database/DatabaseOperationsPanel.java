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
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
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
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

/** Bottom-of-right-hand-side list of long-running database operations with kill. */
@GuiPlugin
public class DatabaseOperationsPanel extends Composite {

  public static final Class<?> PKG = DatabasePerspective.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "DatabaseOperationsPanel-Toolbar";
  public static final String TOOLBAR_ITEM_KILL = "DatabaseOperations-Toolbar-10000-Kill";

  private final List<DatabaseOperation> operations = new ArrayList<>();
  private final Table table;
  private final GuiToolbarWidgets toolBarWidgets;
  private final Runnable tickElapsed = this::refreshElapsed;
  private boolean timerArmed;

  public DatabaseOperationsPanel(Composite parent) {
    super(parent, SWT.NONE);
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

    updateKillEnablement();
  }

  private void addColumn(String title, int width) {
    TableColumn column = new TableColumn(table, SWT.NONE);
    column.setText(title);
    column.setWidth(width);
  }

  public void addOperation(DatabaseOperation operation) {
    operations.add(0, operation);
    TableItem item = new TableItem(table, SWT.NONE, 0);
    fillItem(item, operation);
    table.setSelection(0);
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

  private String statusLabel(DatabaseOperation operation) {
    return switch (operation.getStatus()) {
      case RUNNING -> BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Running");
      case DONE -> BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Done");
      case FAILED -> BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Failed");
      case CANCELLED ->
          BaseMessages.getString(PKG, "DatabasePerspective.Operations.Status.Cancelled");
    };
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

  private DatabaseOperation selectedOperation() {
    TableItem[] selection = table.getSelection();
    if (selection.length != 1) {
      return null;
    }
    Object data = selection[0].getData();
    return data instanceof DatabaseOperation operation ? operation : null;
  }

  private void updateKillEnablement() {
    DatabaseOperation operation = selectedOperation();
    toolBarWidgets.enableToolbarItem(
        TOOLBAR_ITEM_KILL, operation != null && !operation.isFinished());
  }

  public void cancelAll() {
    for (DatabaseOperation operation : operations) {
      if (!operation.isFinished()) {
        operation.cancel();
      }
    }
  }
}
