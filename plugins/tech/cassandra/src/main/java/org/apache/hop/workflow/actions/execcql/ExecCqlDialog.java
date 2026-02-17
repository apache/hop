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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class ExecCqlDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ExecCqlDialog.class;

  private ExecCql execCql;

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
    createShell(BaseMessages.getString(PKG, "ExecCqlDialog.Dialog.Title"), execCql);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    wConnection =
        new MetaSelectionLine<>(
            variables,
            getMetadataProvider(),
            CassandraConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ExecCqlDialog.NeoConnection.Label"),
            BaseMessages.getString(PKG, "ExecCqlDialog.NeoConnection.Tooltip"));
    PropsUi.setLook(wConnection);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, margin);
    fdConnection.right = new FormAttachment(100, -margin);
    fdConnection.top = new FormAttachment(wSpacer, margin);
    wConnection.setLayoutData(fdConnection);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of connections", e);
    }

    Label wlScript = new Label(shell, SWT.LEFT);
    wlScript.setText(BaseMessages.getString(PKG, "ExecCqlDialog.CypherScript.Label"));
    PropsUi.setLook(wlScript);
    FormData fdlCypher = new FormData();
    fdlCypher.left = new FormAttachment(0, margin);
    fdlCypher.right = new FormAttachment(100, -margin);
    fdlCypher.top = new FormAttachment(wConnection, margin);
    wlScript.setLayoutData(fdlCypher);

    wScript =
        new TextVar(
            variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wScript.getTextWidget().setFont(GuiResource.getInstance().getFontFixed());
    PropsUi.setLook(wScript);

    Label wlReplaceVariables = new Label(shell, SWT.LEFT);
    wlReplaceVariables.setText(BaseMessages.getString(PKG, "ExecCqlDialog.ReplaceVariables.Label"));
    PropsUi.setLook(wlReplaceVariables);
    FormData fdlReplaceVariables = new FormData();
    fdlReplaceVariables.left = new FormAttachment(0, margin);
    fdlReplaceVariables.right = new FormAttachment(middle, -margin);
    fdlReplaceVariables.bottom = new FormAttachment(wOk, -margin);
    wlReplaceVariables.setLayoutData(fdlReplaceVariables);
    wReplaceVariables = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wReplaceVariables);
    FormData fdReplaceVariables = new FormData();
    fdReplaceVariables.left = new FormAttachment(middle, 0);
    fdReplaceVariables.right = new FormAttachment(100, -margin);
    fdReplaceVariables.bottom = new FormAttachment(wOk, -margin);
    wReplaceVariables.setLayoutData(fdReplaceVariables);

    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment(0, margin);
    fdCypher.right = new FormAttachment(100, -margin);
    fdCypher.top = new FormAttachment(wlScript, margin);
    fdCypher.bottom = new FormAttachment(wReplaceVariables, -margin);
    fdCypher.height = 200;
    wScript.setLayoutData(fdCypher);

    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return execCql;
  }

  @Override
  protected void onActionNameModified() {
    execCql.setChanged();
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

  @Override
  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
