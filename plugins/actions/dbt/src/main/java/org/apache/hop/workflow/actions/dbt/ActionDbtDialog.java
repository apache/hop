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

package org.apache.hop.workflow.actions.dbt;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * The fields of this dialog are built from the {@code @GuiWidgetElement} annotations on {@link
 * ActionDbt}, which lays them out on the "dbt project", "Selection" and "Execution" tabs. The two
 * name/value tables are not annotated widgets, so they are added to a fourth tab through {@link
 * GuiCompositeWidgets#registerExtraGroup}.
 */
public class ActionDbtDialog extends ActionDialog implements IActionDialog {

  private static final Class<?> PKG = ActionDbt.class;

  /**
   * Height of each name/value table. FormLayout reads it when the shell is packed, which is what
   * decides the size the dialog opens at; the bottom attachment on the second table then takes over
   * when the dialog is resized.
   */
  private static final int TABLE_HEIGHT = 150;

  private ActionDbt action;
  private boolean changed;

  private GuiCompositeWidgets widgets;
  private TableView wVars;
  private TableView wEnvVars;

  public ActionDbtDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionDbt) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDbt.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionDbtDialog.Title"), action);
    changed = action.hasChanged();

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    // The tab folder fills everything between the action name line and the button bar.
    //
    Composite area = new Composite(shell, SWT.NONE);
    PropsUi.setLook(area);
    area.setLayout(new FormLayout());
    FormData fdArea = new FormData();
    fdArea.left = new FormAttachment(0, 0);
    fdArea.top = new FormAttachment(wSpacer, margin);
    fdArea.right = new FormAttachment(100, 0);
    fdArea.bottom = new FormAttachment(wOk, -2 * margin);
    area.setLayoutData(fdArea);

    widgets = new GuiCompositeWidgets(variables);
    widgets.registerExtraGroup(
        BaseMessages.getString(PKG, "ActionDbt.Group.Variables"), "40", null, this::addTables);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              action.setChanged();
            }
          }
        });
    widgets.createCompositeWidgets(
        action, null, area, ActionDbt.GUI_PLUGIN_ELEMENT_PARENT_ID, null);

    getData();
    action.setChanged(changed);
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** The two name/value tables of the Variables tab, stacked on the tab's own composite. */
  private void addTables(Composite parent) {
    ModifyListener lsMod = e -> action.setChanged();

    Label wlVars = new Label(parent, SWT.LEFT);
    wlVars.setText(BaseMessages.getString(PKG, "ActionDbt.Vars.Label"));
    PropsUi.setLook(wlVars);
    FormData fdlVars = new FormData();
    fdlVars.left = new FormAttachment(0, 0);
    fdlVars.top = new FormAttachment(0, margin);
    wlVars.setLayoutData(fdlVars);

    wVars = pairTable(parent, action.getVars().size(), lsMod);
    FormData fdVars = new FormData();
    fdVars.left = new FormAttachment(0, 0);
    fdVars.top = new FormAttachment(wlVars, margin);
    fdVars.right = new FormAttachment(100, 0);
    fdVars.height = TABLE_HEIGHT;
    wVars.setLayoutData(fdVars);

    Label wlEnvVars = new Label(parent, SWT.LEFT);
    wlEnvVars.setText(BaseMessages.getString(PKG, "ActionDbt.EnvVars.Label"));
    PropsUi.setLook(wlEnvVars);
    FormData fdlEnvVars = new FormData();
    fdlEnvVars.left = new FormAttachment(0, 0);
    fdlEnvVars.top = new FormAttachment(wVars, margin);
    wlEnvVars.setLayoutData(fdlEnvVars);

    wEnvVars = pairTable(parent, action.getEnvVars().size(), lsMod);
    FormData fdEnvVars = new FormData();
    fdEnvVars.left = new FormAttachment(0, 0);
    fdEnvVars.top = new FormAttachment(wlEnvVars, margin);
    fdEnvVars.right = new FormAttachment(100, 0);
    fdEnvVars.bottom = new FormAttachment(100, 0);
    fdEnvVars.height = TABLE_HEIGHT;
    wEnvVars.setLayoutData(fdEnvVars);
  }

  private TableView pairTable(Composite parent, int rows, ModifyListener lsMod) {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionDbt.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionDbt.Column.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    return new TableView(
        variables,
        parent,
        SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
        columns,
        rows,
        lsMod,
        props);
  }

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    widgets.setWidgetsContents(action, shell, ActionDbt.GUI_PLUGIN_ELEMENT_PARENT_ID);
    fillTable(wVars, action.getVars());
    fillTable(wEnvVars, action.getEnvVars());
  }

  private void fillTable(TableView table, List<DbtNameValue> pairs) {
    for (int i = 0; i < pairs.size(); i++) {
      DbtNameValue pair = pairs.get(i);
      TableItem item = table.table.getItem(i);
      item.setText(1, Const.NVL(pair.getName(), ""));
      item.setText(2, Const.NVL(pair.getValue(), ""));
    }
    table.optimizeTableView();
  }

  private void readTable(TableView table, List<DbtNameValue> pairs) {
    pairs.clear();
    for (TableItem item : table.getNonEmptyItems()) {
      pairs.add(new DbtNameValue(item.getText(1), item.getText(2)));
    }
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      return;
    }
    String operation = operationText();
    if (!Utils.isEmpty(operation) && DbtOperation.fromNullableCode(operation) == null) {
      // The generated combo cannot be read-only, and an unrecognised operation would quietly run
      // 'dbt run' instead of what was typed.
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "ActionDbt.UnknownOperation.Title"));
      box.setMessage(BaseMessages.getString(PKG, "ActionDbt.UnknownOperation.Message", operation));
      box.open();
      return;
    }
    action.setName(wName.getText());
    widgets.getWidgetsContents(action, ActionDbt.GUI_PLUGIN_ELEMENT_PARENT_ID);
    readTable(wVars, action.getVars());
    readTable(wEnvVars, action.getEnvVars());
    action.setChanged();
    dispose();
  }

  /** What the operation combo shows; empty when the widget could not be built. */
  private String operationText() {
    Control control = widgets.getWidgetsMap().get(ActionDbt.WIDGET_OPERATION);
    return control instanceof Combo combo ? combo.getText() : "";
  }
}
