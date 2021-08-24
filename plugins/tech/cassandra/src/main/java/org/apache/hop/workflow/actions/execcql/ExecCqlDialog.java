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

package org.apache.hop.workflow.actions.execcql;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ExecCqlDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ExecCqlDialog.class; // For Translator

  private Shell shell;

  private ExecCql execCql;

  private Text wName;
  private MetaSelectionLine<CassandraConnection> wConnection;
  private TextVar wScript;
  private Button wReplaceVariables;

  public ExecCqlDialog(
      Shell parent, IAction iAction, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.execCql = (ExecCql) iAction;

    if (this.execCql.getName() == null) {
      this.execCql.setName(BaseMessages.getString(PKG, "ExecCqlDialog.Action.Name"));
    }
  }

  @Override
  public IAction open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, execCql);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ExecCqlDialog.Dialog.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ExecCqlDialog.ActionName.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
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
            CassandraConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ExecCqlDialog.NeoConnection.Label"),
            BaseMessages.getString(PKG, "ExecCqlDialog.NeoConnection.Tooltip"));
    props.setLook(wConnection);
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
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    Label wlReplaceVariables = new Label(shell, SWT.LEFT);
    wlReplaceVariables.setText(BaseMessages.getString(PKG, "ExecCqlDialog.ReplaceVariables.Label"));
    props.setLook(wlReplaceVariables);
    FormData fdlReplaceVariables = new FormData();
    fdlReplaceVariables.left = new FormAttachment(0, 0);
    fdlReplaceVariables.right = new FormAttachment(middle, -margin);
    fdlReplaceVariables.bottom = new FormAttachment(wOk, -margin * 2);
    wlReplaceVariables.setLayoutData(fdlReplaceVariables);
    wReplaceVariables = new Button(shell, SWT.CHECK | SWT.BORDER);
    props.setLook(wReplaceVariables);
    FormData fdReplaceVariables = new FormData();
    fdReplaceVariables.left = new FormAttachment(middle, 0);
    fdReplaceVariables.right = new FormAttachment(100, 0);
    fdReplaceVariables.top = new FormAttachment(wlReplaceVariables, 0, SWT.CENTER);
    wReplaceVariables.setLayoutData(fdReplaceVariables);

    Label wlScript = new Label(shell, SWT.LEFT);
    wlScript.setText(BaseMessages.getString(PKG, "ExecCqlDialog.CypherScript.Label"));
    props.setLook(wlScript);
    FormData fdlCypher = new FormData();
    fdlCypher.left = new FormAttachment(0, 0);
    fdlCypher.right = new FormAttachment(100, 0);
    fdlCypher.top = new FormAttachment(wConnection, margin);
    wlScript.setLayoutData(fdlCypher);
    wScript =
        new TextVar(
            variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wScript.getTextWidget().setFont(GuiResource.getInstance().getFontFixed());
    props.setLook(wScript);
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment(0, 0);
    fdCypher.right = new FormAttachment(100, 0);
    fdCypher.top = new FormAttachment(wlScript, margin);
    fdCypher.bottom = new FormAttachment(wReplaceVariables, -margin * 2);
    wScript.setLayoutData(fdCypher);

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

    return execCql;
  }

  private void cancel() {
    execCql = null;
    dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(execCql.getName(), ""));
    wConnection.setText(Const.NVL(execCql.getConnectionName(), ""));
    wScript.setText(Const.NVL(execCql.getScript(), ""));
    wReplaceVariables.setSelection(execCql.isReplacingVariables());
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "ExecCqlDialog.MissingName.Warning.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "ExecCqlDialog.MissingName.Warning.Message"));
      mb.open();
      return;
    }
    execCql.setName(wName.getText());
    execCql.setConnectionName(wConnection.getText());
    execCql.setScript(wScript.getText());
    execCql.setReplacingVariables(wReplaceVariables.getSelection());

    dispose();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
