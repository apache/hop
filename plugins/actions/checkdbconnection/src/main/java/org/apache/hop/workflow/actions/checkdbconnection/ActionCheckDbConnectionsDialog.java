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

package org.apache.hop.workflow.actions.checkdbconnection;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit the check database connection action settings. */
public class ActionCheckDbConnectionsDialog extends ActionDialog {
  private static final Class<?> PKG = ActionCheckDbConnectionsDialog.class;

  private Text wName;

  private ActionCheckDbConnections action;

  private boolean changed;

  private TableView wFields;

  public ActionCheckDbConnectionsDialog(
      Shell parent,
      ActionCheckDbConnections action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionCheckDbConnections.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = (ModifyEvent e) -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionCheckDbConnections.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons at the bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, (Event e) -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, (Event e) -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionCheckDbConnections.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionCheckDbConnections.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wName, 2 * margin);
    wlFields.setLayoutData(fdlFields);

    // Buttons to the right of the screen...
    Button wbGetConnections = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbGetConnections);
    wbGetConnections.setText(
        BaseMessages.getString(PKG, "ActionCheckDbConnections.GetConnections"));
    wbGetConnections.setToolTipText(
        BaseMessages.getString(PKG, "ActionCheckDbConnections.GetConnections.Tooltip"));
    FormData fdbGetConnections = new FormData();
    fdbGetConnections.right = new FormAttachment(100, -margin);
    fdbGetConnections.top = new FormAttachment(wlFields, margin);
    wbGetConnections.setLayoutData(fdbGetConnections);

    // Buttons to the right of the screen...
    Button wbdSourceFileFolder = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdSourceFileFolder);
    wbdSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionCheckDbConnections.DeleteEntry"));
    wbdSourceFileFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionCheckDbConnections.DeleteSourceFileButton.Label"));
    FormData fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment(100, -margin);
    fdbdSourceFileFolder.top = new FormAttachment(wbGetConnections, margin);
    wbdSourceFileFolder.setLayoutData(fdbdSourceFileFolder);

    final int fieldsRows = action.getConnections().size();

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCheckDbConnections.Fields.Argument.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              this.getWorkflowMeta().getDatabaseNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCheckDbConnections.Fields.WaitFor.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionCheckDbConnections.Fields.WaitForTime.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ActionCheckDbConnections.WaitTimeUnit.getDescriptions(),
              false),
        };

    columns[0].setToolTip(BaseMessages.getString(PKG, "ActionCheckDbConnections.Fields.Column"));
    columns[1].setUsingVariables(true);
    columns[1].setToolTip(BaseMessages.getString(PKG, "ActionCheckDbConnections.WaitFor.ToolTip"));

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            fieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wbGetConnections, -margin);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    // Delete files from the list of files...
    wbdSourceFileFolder.addListener(
        SWT.Selection,
        e -> {
          int[] idx = wFields.getSelectionIndices();
          wFields.remove(idx);
          wFields.removeEmptyRows();
          wFields.setRowNums();
        });

    // get connections...
    wbGetConnections.addListener(SWT.Selection, e -> getDatabases());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  public void getDatabases() {
    wFields.removeAll();
    List<DatabaseMeta> databases = this.getWorkflowMeta().getDatabases();
    for (DatabaseMeta ci : databases) {
      if (ci != null) {
        wFields.add(
            ci.getName(), "0", ActionCheckDbConnections.WaitTimeUnit.MILLISECOND.getDescription());
      }
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));

    for (int i = 0; i < action.getConnections().size(); i++) {
      ActionCheckDbConnections.CDConnection connection = action.getConnections().get(i);
      TableItem ti = wFields.table.getItem(i);
      ti.setText(1, Const.NVL(connection.getName(), ""));
      ti.setText(2, Const.NVL(connection.getWaitTime(), ""));
      ti.setText(3, connection.getWaitTimeUnit().getDescription());
    }
    wFields.optimizeTableView();

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }

    try {
      IHopMetadataSerializer<DatabaseMeta> serializer =
          metadataProvider.getSerializer(DatabaseMeta.class);

      action.setName(wName.getText());

      action.getConnections().clear();
      for (TableItem item : wFields.getNonEmptyItems()) {
        String databaseName = item.getText(1);
        String waitTime = item.getText(2);
        ActionCheckDbConnections.WaitTimeUnit unit =
            ActionCheckDbConnections.WaitTimeUnit.lookupDescription(item.getText(3));

        ActionCheckDbConnections.CDConnection connection =
            new ActionCheckDbConnections.CDConnection();
        connection.setName(databaseName);
        connection.setWaitTime(waitTime);
        connection.setWaitTimeUnit(unit);
        action.getConnections().add(connection);
      }

      dispose();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting dialog information", e);
    }
  }
}
