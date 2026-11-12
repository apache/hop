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

package org.apache.hop.workflow.actions.checkdatacatalog;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * The options of this dialog come from the {@code @GuiWidgetElement} annotations on {@link
 * ActionCheckDataCatalog}. The datasets table is not an annotated widget, so it is added as a tab
 * of its own through {@link GuiCompositeWidgets#registerExtraGroup}.
 */
public class ActionCheckDataCatalogDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionCheckDataCatalog.class;

  private ActionCheckDataCatalog action;
  private boolean changed;
  private GuiCompositeWidgets widgets;
  private TableView wDatasets;

  public ActionCheckDataCatalogDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionCheckDataCatalog) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionCheckDataCatalog.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionCheckDataCatalog.Name"), action);
    changed = action.hasChanged();
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    // The tabs fill everything between the action name line and the button bar
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
        BaseMessages.getString(PKG, "ActionCheckDataCatalog.Group.Datasets"),
        "20",
        null,
        this::addDatasetsTable);
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
        action, null, area, ActionCheckDataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID, null);

    getData();
    action.setChanged(changed);
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return action;
  }

  private void addDatasetsTable(Composite parent) {
    String[] connections = connectionNames();
    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "ActionCheckDataCatalog.Column.Type"),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          IEnumHasCodeAndDescription.getDescriptions(CatalogDatasetType.class),
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ActionCheckDataCatalog.Column.Connection"),
          ColumnInfo.COLUMN_TYPE_CCOMBO,
          connections,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ActionCheckDataCatalog.Column.Schema"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "ActionCheckDataCatalog.Column.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
    };
    columns[0].setToolTip(
        BaseMessages.getString(PKG, "ActionCheckDataCatalog.Column.Type.Tooltip"));
    columns[1].setToolTip(
        BaseMessages.getString(PKG, "ActionCheckDataCatalog.Column.Connection.Tooltip"));
    columns[1].setUsingVariables(true);
    columns[2].setUsingVariables(true);
    columns[3].setToolTip(
        BaseMessages.getString(PKG, "ActionCheckDataCatalog.Column.Name.Tooltip"));
    columns[3].setUsingVariables(true);

    wDatasets =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            action.getDatasets().size(),
            e -> action.setChanged(),
            props);
    FormData fdDatasets = new FormData();
    fdDatasets.left = new FormAttachment(0, 0);
    fdDatasets.top = new FormAttachment(0, margin);
    fdDatasets.right = new FormAttachment(100, 0);
    fdDatasets.bottom = new FormAttachment(100, 0);
    wDatasets.setLayoutData(fdDatasets);
  }

  private String[] connectionNames() {
    try {
      List<String> names = metadataProvider.getSerializer(DatabaseMeta.class).listObjectNames();
      return names.toArray(new String[0]);
    } catch (Exception e) {
      return new String[0];
    }
  }

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    widgets.setWidgetsContents(action, shell, ActionCheckDataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID);
    List<CatalogDataset> datasets = action.getDatasets();
    for (int i = 0; i < datasets.size(); i++) {
      CatalogDataset dataset = datasets.get(i);
      TableItem item = wDatasets.table.getItem(i);
      CatalogDatasetType type =
          dataset.getType() == null ? CatalogDatasetType.TABLE : dataset.getType();
      item.setText(1, type.getDescription());
      item.setText(2, Const.NVL(dataset.getConnection(), ""));
      item.setText(3, Const.NVL(dataset.getSchema(), ""));
      item.setText(4, Const.NVL(dataset.getName(), ""));
    }
    wDatasets.optimizeTableView();
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
    action.setName(wName.getText());
    widgets.getWidgetsContents(action, ActionCheckDataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID);
    List<CatalogDataset> datasets = action.getDatasets();
    datasets.clear();
    for (TableItem item : wDatasets.getNonEmptyItems()) {
      datasets.add(
          new CatalogDataset(
              IEnumHasCodeAndDescription.lookupDescription(
                  CatalogDatasetType.class, item.getText(1), CatalogDatasetType.TABLE),
              item.getText(2),
              item.getText(3),
              item.getText(4)));
    }
    action.setChanged();
    dispose();
  }
}
