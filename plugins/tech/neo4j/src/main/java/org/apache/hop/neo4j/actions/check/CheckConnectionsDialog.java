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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CheckConnectionsDialog extends ActionDialog implements IActionDialog {

  private static Class<?> PKG = CheckConnectionsDialog.class; // For Translator

  private Shell shell;

  private CheckConnections action;

  private boolean changed;

  private Text wName;
  private TableView wConnections;

  private Button wOk, wCancel;

  private String[] availableConnectionNames;

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

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod =
        new ModifyListener() {
          public void modifyText(ModifyEvent e) {
            action.setChanged();
          }
        };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Check Neo4j Connections");

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Add buttons first, then the list of connections dynamically sizing
    // Put these buttons at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell,
        new Button[] {
          wOk, wCancel,
        },
        margin,
        null);

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText("Action name");
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

    Label wlConnections = new Label(shell, SWT.LEFT);
    wlConnections.setText(BaseMessages.getString(PKG, "CheckConnectionsDialog.Connections.Label"));
    props.setLook(wlConnections);
    FormData fdlConnections = new FormData();
    fdlConnections.left = new FormAttachment(0, 0);
    fdlConnections.right = new FormAttachment(middle, -margin);
    fdlConnections.top = new FormAttachment(wName, margin);
    wlConnections.setLayoutData(fdlConnections);

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
    fdConnections.bottom = new FormAttachment(wOk, -margin * 2);
    wConnections.setLayoutData(fdConnections);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
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

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
