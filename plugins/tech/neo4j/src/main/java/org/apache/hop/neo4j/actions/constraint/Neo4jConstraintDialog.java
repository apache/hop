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

package org.apache.hop.neo4j.actions.constraint;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class Neo4jConstraintDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = Neo4jConstraintDialog.class;

  private Neo4jConstraint meta;

  private boolean changed;

  private MetaSelectionLine<NeoConnection> wConnection;
  private TableView wUpdates;

  public Neo4jConstraintDialog(
      Shell parent, IAction iAction, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.meta = (Neo4jConstraint) iAction;

    if (this.meta.getName() == null) {
      this.meta.setName(BaseMessages.getString(PKG, "Neo4jConstraintDialog.Action.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "Neo4jConstraintDialog.Dialog.Title"), meta);
    ModifyListener lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    int middle = this.middle;
    int margin = this.margin;

    wConnection =
        new MetaSelectionLine<>(
            variables,
            getMetadataProvider(),
            NeoConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "Neo4jConstraintDialog.NeoConnection.Label"),
            BaseMessages.getString(PKG, "Neo4jConstraintDialog.NeoConnection.Tooltip"));
    PropsUi.setLook(wConnection);
    wConnection.addModifyListener(lsMod);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(wSpacer, margin);
    wConnection.setLayoutData(fdConnection);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of connections", e);
    }

    Label wlUpdates = new Label(shell, SWT.LEFT);
    wlUpdates.setText(BaseMessages.getString(PKG, "Neo4jConstraintDialog.ConstraintUpdates.Label"));
    PropsUi.setLook(wlUpdates);
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
              BaseMessages.getString(
                  PKG, "Neo4jConstraintDialog.ConstraintUpdates.Column.UpdateType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              UpdateType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "Neo4jConstraintDialog.ConstraintUpdates.Column.ObjectType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ObjectType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "Neo4jConstraintDialog.ConstraintUpdates.Column.ConstraintType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ConstraintType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "Neo4jConstraintDialog.ConstraintUpdates.Column.ConstraintName"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "Neo4jConstraintDialog.ConstraintUpdates.Column.ObjectName"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "Neo4jConstraintDialog.ConstraintUpdates.Column.ObjectProperties"),
              ColumnInfo.COLUMN_TYPE_TEXT),
        };

    wUpdates =
        new TableView(
            variables,
            shell,
            SWT.NONE,
            columns,
            meta.getConstraintUpdates().size(),
            false,
            null,
            props);
    PropsUi.setLook(wUpdates);
    wUpdates.addModifyListener(lsMod);
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment(0, 0);
    fdCypher.right = new FormAttachment(100, 0);
    fdCypher.top = new FormAttachment(wlUpdates, margin);
    wUpdates.setLayoutData(fdCypher);

    buildButtonBar()
        .custom(
            BaseMessages.getString(PKG, "Neo4jConstraintDialog.Button.ShowCypher"),
            e -> showCypherPreview())
        .ok(e -> ok())
        .cancel(e -> cancel())
        .build(wUpdates);

    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return meta;
  }

  private void showCypherPreview() {
    // Get current data from dialog
    List<TableItem> items = wUpdates.getNonEmptyItems();
    if (items.isEmpty()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setText(BaseMessages.getString(PKG, "Neo4jConstraintDialog.NoConstraints.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "Neo4jConstraintDialog.NoConstraints.Message"));
      mb.open();
      return;
    }

    // Build Cypher preview for all constraint updates
    StringBuilder cypherPreview = new StringBuilder();
    cypherPreview
        .append("-- Generated Cypher statements for constraint operations")
        .append(Const.CR)
        .append(Const.CR);

    for (int i = 0; i < items.size(); i++) {
      TableItem item = items.get(i);
      try {
        UpdateType type = UpdateType.getType(item.getText(1));
        ObjectType objectType = ObjectType.getType(item.getText(2));
        ConstraintType constraintType = ConstraintType.getType(item.getText(3));
        String constraintName = item.getText(4);
        String objectName = item.getText(5);
        String objectProperties = item.getText(6);

        ConstraintUpdate constraintUpdate =
            new ConstraintUpdate(
                type, objectType, constraintType, constraintName, objectName, objectProperties);

        String cypher;
        if (type == UpdateType.CREATE) {
          cypher = Neo4jConstraint.generateCreateConstraintCypher(constraintUpdate);
        } else {
          cypher = Neo4jConstraint.generateDropConstraintCypher(constraintUpdate);
        }

        cypherPreview.append("-- Constraint ").append(i + 1).append(Const.CR);
        cypherPreview.append(cypher).append(Const.CR).append(Const.CR);
      } catch (Exception e) {
        cypherPreview
            .append("-- Error generating Cypher for constraint ")
            .append(i + 1)
            .append(": ")
            .append(e.getMessage())
            .append(Const.CR)
            .append(Const.CR);
      }
    }

    // Show in read-only dialog
    EnterTextDialog dialog =
        new EnterTextDialog(
            shell,
            BaseMessages.getString(PKG, "Neo4jConstraintDialog.ShowCypher.Title"),
            BaseMessages.getString(PKG, "Neo4jConstraintDialog.ShowCypher.Message"),
            cypherPreview.toString(),
            true);
    dialog.setReadOnly();
    dialog.open();
  }

  @Override
  protected void onActionNameModified() {
    meta.setChanged();
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
    for (int i = 0; i < meta.getConstraintUpdates().size(); i++) {
      TableItem item = wUpdates.table.getItem(i);
      ConstraintUpdate constraintUpdate = meta.getConstraintUpdates().get(i);

      if (constraintUpdate.getUpdateType() != null) {
        item.setText(1, constraintUpdate.getUpdateType().name());
      }
      if (constraintUpdate.getObjectType() != null) {
        item.setText(2, constraintUpdate.getObjectType().name());
      }
      if (constraintUpdate.getConstraintType() != null) {
        item.setText(3, constraintUpdate.getConstraintType().name());
      }
      item.setText(4, Const.NVL(constraintUpdate.getConstraintName(), ""));
      item.setText(5, Const.NVL(constraintUpdate.getObjectName(), ""));
      item.setText(6, Const.NVL(constraintUpdate.getObjectProperties(), ""));
    }
    wUpdates.optimizeTableView();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "Neo4jConstraintDialog.MissingName.Warning.Title"));
      mb.setMessage(
          BaseMessages.getString(PKG, "Neo4jConstraintDialog.MissingName.Warning.Message"));
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
    meta.getConstraintUpdates().clear();
    for (TableItem item : items) {
      UpdateType type = UpdateType.getType(item.getText(1));
      ObjectType objectType = ObjectType.getType(item.getText(2));
      ConstraintType constraintType = ConstraintType.getType(item.getText(3));
      String constraintName = item.getText(4);
      String objectName = item.getText(5);
      String objectProperties = item.getText(6);
      meta.getConstraintUpdates()
          .add(
              new ConstraintUpdate(
                  type, objectType, constraintType, constraintName, objectName, objectProperties));
    }

    dispose();
  }
}
