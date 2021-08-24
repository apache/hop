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

package org.apache.hop.neo4j.actions.index;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;

public class Neo4jIndexDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = Neo4jIndexDialog.class; // For Translator

  private Shell shell;

  private Neo4jIndex meta;

  private boolean changed;

  private Text wName;
  private MetaSelectionLine<NeoConnection> wConnection;
  private TableView wUpdates;

  private Button wCancel;

  public Neo4jIndexDialog(
      Shell parent, IAction iAction, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.meta = (Neo4jIndex) iAction;

    if (this.meta.getName() == null) {
      this.meta.setName(BaseMessages.getString(PKG, "Neo4jIndexDialog.Action.Name"));
    }
  }

  @Override
  public IAction open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, meta);

    ModifyListener lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "Neo4jIndexDialog.Dialog.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "Neo4jIndexDialog.ActionName.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    wConnection =
        new MetaSelectionLine<>(
            variables,
            getMetadataProvider(),
            NeoConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "Neo4jIndexDialog.NeoConnection.Label"),
            BaseMessages.getString(PKG, "Neo4jIndexDialog.NeoConnection.Tooltip"));
    props.setLook(wConnection);
    wConnection.addModifyListener(lsMod);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(lastControl, margin);
    wConnection.setLayoutData(fdConnection);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of connections", e);
    }

    // Add buttons first, then the script field can use dynamic sizing
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    Label wlUpdates = new Label(shell, SWT.LEFT);
    wlUpdates.setText(BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Label"));
    props.setLook(wlUpdates);
    FormData fdlCypher = new FormData();
    fdlCypher.left = new FormAttachment(0, 0);
    fdlCypher.right = new FormAttachment(100, 0);
    fdlCypher.top = new FormAttachment(wConnection, margin);
    wlUpdates.setLayoutData(fdlCypher);

    // The columns
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.UpdateType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              UpdateType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.ObjectType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ObjectType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.IndexName"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.ObjectName"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.ObjectProperties"),
              ColumnInfo.COLUMN_TYPE_TEXT),
        };

    wUpdates =
        new TableView(
            variables, shell, SWT.NONE, columns, meta.getIndexUpdates().size(), false, null, props);
    props.setLook(wUpdates);
    wUpdates.addModifyListener(lsMod);
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment(0, 0);
    fdCypher.right = new FormAttachment(100, 0);
    fdCypher.top = new FormAttachment(wlUpdates, margin);
    fdCypher.bottom = new FormAttachment(wOk, -margin * 2);
    wUpdates.setLayoutData(fdCypher);

    // Put these buttons at the bottom
    //
    BaseTransformDialog.positionBottomButtons(
        shell,
        new Button[] {
          wOk, wCancel,
        },
        margin,
        null);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return meta;
  }

  private void cancel() {
    meta.setChanged(changed);
    meta = null;
    dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(meta.getName(), ""));
    if (meta.getConnection() != null) {
      wConnection.setText(Const.NVL(meta.getConnection().getName(), ""));
    }
    for (int i = 0; i < meta.getIndexUpdates().size(); i++) {
      TableItem item = wUpdates.table.getItem(i);
      IndexUpdate indexUpdate = meta.getIndexUpdates().get(i);

      if (indexUpdate.getType() != null) {
        item.setText(1, indexUpdate.getType().name());
      }
      if (indexUpdate.getObjectType() != null) {
        item.setText(2, indexUpdate.getObjectType().name());
      }
      item.setText(3, Const.NVL(indexUpdate.getIndexName(), ""));
      item.setText(4, Const.NVL(indexUpdate.getObjectName(), ""));
      item.setText(5, Const.NVL(indexUpdate.getObjectProperties(), ""));
    }
    wUpdates.optimizeTableView();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "Neo4jIndexDialog.MissingName.Warning.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "Neo4jIndexDialog.MissingName.Warning.Message"));
      mb.open();
      return;
    }
    meta.setName(wName.getText());

    // Grab the connection
    //

    String connectionName = wConnection.getText();
    if (StringUtils.isEmpty(connectionName)) {
      meta.setConnection(null);
    } else {
      try {
        meta.setConnection(
            metadataProvider.getSerializer(NeoConnection.class).load(connectionName));
      } catch (Exception e) {
        new ErrorDialog(shell, "Error", "Error finding connection " + connectionName, e);
        return;
      }
    }

    List<TableItem> items = wUpdates.getNonEmptyItems();
    meta.getIndexUpdates().clear();
    for (TableItem item : items) {
      UpdateType type = UpdateType.getType(item.getText(1));
      ObjectType objectType = ObjectType.getType(item.getText(2));
      String indexName = item.getText(3);
      String objectName = item.getText(4);
      String objectProperties = item.getText(5);
      meta.getIndexUpdates()
          .add(new IndexUpdate(type, objectType, indexName, objectName, objectProperties));
    }

    dispose();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
