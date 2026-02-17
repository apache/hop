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

package org.apache.hop.neo4j.actions.check;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class CheckConnectionsDialog extends ActionDialog implements IActionDialog {

  private static final Class<?> PKG = CheckConnectionsDialog.class;

  private CheckConnections action;

  private boolean changed;

  private TableView wConnections;

  public CheckConnectionsDialog(
      Shell parent, IAction iAction, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (CheckConnections) iAction;

    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "CheckConnectionsDialog.Dialog.Title"));
    }
  }

  @Override
  public IAction open() {
    createShell("Check Neo4j Connections", action);
    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    int middle = this.middle;
    int margin = this.margin;

    Label wlConnections = new Label(shell, SWT.LEFT);
    wlConnections.setText(BaseMessages.getString(PKG, "CheckConnectionsDialog.Connections.Label"));
    PropsUi.setLook(wlConnections);
    FormData fdlConnections = new FormData();
    fdlConnections.left = new FormAttachment(0, 0);
    fdlConnections.right = new FormAttachment(middle, -margin);
    fdlConnections.top = new FormAttachment(wSpacer, margin);
    wlConnections.setLayoutData(fdlConnections);

    String[] availableConnectionNames;
    try {
      IHopMetadataSerializer<NeoConnection> connectionSerializer =
          getMetadataProvider().getSerializer(NeoConnection.class);
      List<String> names = connectionSerializer.listObjectNames();
      Collections.sort(names);
      availableConnectionNames = names.toArray(new String[0]);
    } catch (HopException e) {
      availableConnectionNames = new String[] {};
    }

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "CheckConnectionsDialog.ConnectionName.Column.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              availableConnectionNames,
              false),
        };
    columns[0].setUsingVariables(true);
    wConnections =
        new TableView(
            action,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            action.getConnectionNames().size(),
            false,
            lsMod,
            props);
    FormData fdConnections = new FormData();
    fdConnections.left = new FormAttachment(0, 0);
    fdConnections.top = new FormAttachment(wlConnections, margin);
    fdConnections.right = new FormAttachment(100, 0);
    wConnections.setLayoutData(fdConnections);

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build(wConnections);

    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
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

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    wConnections.removeAll();
    for (int i = 0; i < action.getConnectionNames().size(); i++) {
      wConnections.add(Const.NVL(action.getConnectionNames().get(i), ""));
    }
    wConnections.removeEmptyRows();
    wConnections.setRowNums();
    wConnections.optWidth(true);
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "CheckConnectionsDialog.MissingName.Warning.Title"));
      mb.setMessage(
          BaseMessages.getString(PKG, "CheckConnectionsDialog.MissingName.Warning.Message"));
      mb.open();
      return;
    }
    action.setName(wName.getText());

    int nrItems = wConnections.nrNonEmpty();
    action.setConnectionNames(new ArrayList<>());
    for (int i = 0; i < nrItems; i++) {
      String connectionName = wConnections.getNonEmpty(i).getText(1);
      action.getConnectionNames().add(connectionName);
    }

    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
