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

package org.apache.hop.workflow.actions.mql;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the MongoDB QL action settings. (select the connection and the
 * json instructions to be executed)
 */
public class ActionMqlDialog extends ActionDialog {
  private static final Class<?> PKG = ActionMql.class;

  private Text wName;

  private MetaSelectionLine<MongoDbConnection> wConnection;

  private Label wlMql;

  private TextComposite wMql;

  private Label wlPosition;

  private ActionMql action;

  private Button wUseExternalFile;

  private TextVar wCommandFile;

  private Button wbBrowse;

  private Button wUseSubs;

  public ActionMqlDialog(
      Shell parent, ActionMql action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionMQL.Name.Default"));
    }
  }

  @Override
  public IAction open() {

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionMQL.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Action name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionMQL.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Connection line
    Label wlConnection = new Label(shell, SWT.RIGHT);
    wlConnection.setText(BaseMessages.getString(PKG, "ActionMQL.MongoDbConnectionLabelText"));
    PropsUi.setLook(wlConnection);

    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment(0, 0);
    fdlConnection.right = new FormAttachment(middle, -margin);
    fdlConnection.top = new FormAttachment(wName, 2 * margin);
    wlConnection.setLayoutData(fdlConnection);

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            MongoDbConnection.class,
            shell,
            SWT.NONE,
            BaseMessages.getString(PKG, "ActionMQL.MongoDbConnectionLabelText"),
            BaseMessages.getString(PKG, "ActionMQL.MongoDbConnectionTooltipText"));

    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(wName, margin);
    wConnection.setLayoutData(fdConnection);

    try {
      wConnection.fillItems();
    } catch (HopException e) {
      new HopException(BaseMessages.getString(PKG, "ActionMQL.ErrorLoadingMongoDBConnections"));
    }

    // Position label
    wlPosition = new Label(shell, SWT.NONE);
    wlPosition.setText(BaseMessages.getString(PKG, "ActionMQL.LineNr.Label", "0"));
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(100, 0);
    fdlPosition.bottom = new FormAttachment(wOk, -margin);
    wlPosition.setLayoutData(fdlPosition);

    // External file line
    Label wlUseExternalFile = new Label(shell, SWT.RIGHT);
    wlUseExternalFile.setText(BaseMessages.getString(PKG, "ActionMQL.MQLFromFile.Label"));

    FormData fdlUseExternal = new FormData();
    fdlUseExternal.left = new FormAttachment(0, 0);
    fdlUseExternal.top = new FormAttachment(wConnection, 2 * margin);
    fdlUseExternal.right = new FormAttachment(middle, -margin);
    wlUseExternalFile.setLayoutData(fdlUseExternal);

    wUseExternalFile = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wUseExternalFile);
    FormData fdUseExternal = new FormData();
    fdUseExternal.left = new FormAttachment(middle, 0);
    fdUseExternal.top = new FormAttachment(wlUseExternalFile, 0, SWT.CENTER);
    fdUseExternal.right = new FormAttachment(100, 0);
    wUseExternalFile.setLayoutData(fdUseExternal);

    wUseExternalFile.addListener(SWT.Selection, e -> updateFileOption());

    Label wlFile = new Label(shell, SWT.RIGHT);
    wlFile.setText(BaseMessages.getString(PKG, "ActionMQL.Filename.Label"));
    PropsUi.setLook(wlFile);

    FormData fdlFile = new FormData();
    fdlFile.left = new FormAttachment(0, 0);
    fdlFile.right = new FormAttachment(middle, -margin);
    fdlFile.top = new FormAttachment(wUseExternalFile, 2 * margin);
    wlFile.setLayoutData(fdlFile);

    wbBrowse = new Button(shell, SWT.PUSH | SWT.CENTER);
    wbBrowse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    PropsUi.setLook(wbBrowse);

    FormData fdbBrowse = new FormData();
    fdbBrowse.right = new FormAttachment(100, 0);
    fdbBrowse.top = new FormAttachment(wlFile, 0, SWT.CENTER);
    wbBrowse.setLayoutData(fdbBrowse);

    wbBrowse.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wCommandFile,
                variables,
                new String[] {"*.json", "*.txt", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "ActionMQL.Filetype.Json"),
                  BaseMessages.getString(PKG, "ActionMQL.Filetype.Text"),
                  BaseMessages.getString(PKG, "ActionMQL.Filetype.All")
                },
                true));

    // wCommandFile = new StyledTextComp(variables, shell, SWT.SINGLE | SWT.BORDER);
    wCommandFile = new TextVar(variables, shell, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wCommandFile);

    FormData fdFile = new FormData();
    fdFile.left = new FormAttachment(middle, 0);
    fdFile.right = new FormAttachment(80, -margin);
    fdFile.top = new FormAttachment(wUseExternalFile, margin);
    wCommandFile.setLayoutData(fdFile);

    // Use variable substitution
    Label wlUseSubs = new Label(shell, SWT.RIGHT);
    wlUseSubs.setText(BaseMessages.getString(PKG, "ActionMQL.UseVariableSubst.Label"));
    PropsUi.setLook(wlUseSubs);

    FormData fdlUseSubs = new FormData();
    fdlUseSubs.left = new FormAttachment(0, 0);
    fdlUseSubs.right = new FormAttachment(middle, -margin);
    fdlUseSubs.top = new FormAttachment(wCommandFile, 2 * margin);
    wlUseSubs.setLayoutData(fdlUseSubs);

    wUseSubs = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wUseSubs);
    wUseSubs.setToolTipText(BaseMessages.getString(PKG, "ActionMQL.UseVariableSubst.Tooltip"));

    FormData fdUseSubs = new FormData();
    fdUseSubs.left = new FormAttachment(middle, 0);
    fdUseSubs.top = new FormAttachment(wCommandFile, 2 * margin);
    fdUseSubs.right = new FormAttachment(100, 0);
    wUseSubs.setLayoutData(fdUseSubs);

    // Script line
    wlMql = new Label(shell, SWT.NONE);
    wlMql.setText(BaseMessages.getString(PKG, "ActionMQL.Script.Label"));
    PropsUi.setLook(wlMql);
    FormData fdlMql = new FormData();
    fdlMql.left = new FormAttachment(0, 0);
    fdlMql.top = new FormAttachment(wUseSubs, 2 * margin);
    wlMql.setLayoutData(fdlMql);

    wMql =
        new StyledTextComp(
            variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);

    PropsUi.setLook(wMql, Props.WIDGET_STYLE_FIXED);
    FormData fdMql = new FormData();
    fdMql.left = new FormAttachment(0, 0);
    fdMql.top = new FormAttachment(wlMql, margin);
    fdMql.right = new FormAttachment(100, -margin);
    fdMql.bottom = new FormAttachment(wlPosition, -margin);
    wMql.setLayoutData(fdMql);
    wMql.addListener(SWT.Modify, e -> setPosition());
    wMql.addListener(SWT.KeyDown, e -> setPosition());
    wMql.addListener(SWT.KeyUp, e -> setPosition());
    wMql.addListener(SWT.FocusIn, e -> setPosition());
    wMql.addListener(SWT.FocusOut, e -> setPosition());
    wMql.addListener(SWT.MouseDoubleClick, e -> setPosition());
    wMql.addListener(SWT.MouseDown, e -> setPosition());
    wMql.addListener(SWT.MouseUp, e -> setPosition());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void updateFileOption() {
    boolean useFile = wUseExternalFile.getSelection();
    wCommandFile.setEnabled(useFile);
    wbBrowse.setEnabled(useFile);
    wMql.setEnabled(!useFile);
  }

  public void setPosition() {
    int lineNumber = wMql.getLineNumber();
    int columnNumber = wMql.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "ActionMQL.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wMql.setText(Const.nullToEmpty(action.getMql()));
    wConnection.setText(Const.nullToEmpty(action.getConnection()));

    wName.selectAll();
    wName.setFocus();

    wUseExternalFile.setSelection(action.isUseExternalFile());
    wCommandFile.setText(Const.nullToEmpty(action.getCommandFile()));
    updateFileOption();
    wUseSubs.setSelection(action.isUseVariableSubstitution());
  }

  private void cancel() {
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

    action.setName(wName.getText());
    action.setConnection(wConnection.getText());
    action.setMql(wMql.getText());

    action.setUseExternalFile(wUseExternalFile.getSelection());
    action.setCommandFile(wCommandFile.getText());
    action.setUseVariableSubstitution(wUseSubs.getSelection());

    action.setChanged();

    dispose();
  }
}
