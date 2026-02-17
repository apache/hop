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

package org.apache.hop.neo4j.actions.cypherscript;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class CypherScriptDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = CypherScriptDialog.class;

  private CypherScript cypherScript;

  private boolean changed;

  private MetaSelectionLine<NeoConnection> wConnection;
  private TextVar wScript;
  private Button wReplaceVariables;

  public CypherScriptDialog(
      Shell parent, IAction iAction, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.cypherScript = (CypherScript) iAction;

    if (this.cypherScript.getName() == null) {
      this.cypherScript.setName(BaseMessages.getString(PKG, "CypherScriptDialog.Action.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "CypherScriptDialog.Dialog.Title"), cypherScript);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
    ModifyListener lsMod = e -> cypherScript.setChanged();
    changed = cypherScript.hasChanged();

    wConnection =
        new MetaSelectionLine<>(
            variables,
            getMetadataProvider(),
            NeoConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "CypherScriptDialog.NeoConnection.Label"),
            BaseMessages.getString(PKG, "CypherScriptDialog.NeoConnection.Tooltip"));
    PropsUi.setLook(wConnection);
    wConnection.addModifyListener(lsMod);
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
    wlScript.setText(BaseMessages.getString(PKG, "CypherScriptDialog.CypherScript.Label"));
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
    wScript.addModifyListener(lsMod);

    Label wlReplaceVariables = new Label(shell, SWT.LEFT);
    wlReplaceVariables.setText(
        BaseMessages.getString(PKG, "CypherScriptDialog.ReplaceVariables.Label"));
    PropsUi.setLook(wlReplaceVariables);
    FormData fdlReplaceVariables = new FormData();
    fdlReplaceVariables.left = new FormAttachment(0, margin);
    fdlReplaceVariables.right = new FormAttachment(middle, -margin);
    fdlReplaceVariables.bottom = new FormAttachment(wOk, -margin);
    wlReplaceVariables.setLayoutData(fdlReplaceVariables);
    wReplaceVariables = new Button(shell, SWT.CHECK | SWT.BORDER);
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

    return cypherScript;
  }

  @Override
  protected void onActionNameModified() {
    cypherScript.setChanged();
  }

  private void cancel() {
    cypherScript.setChanged(changed);
    cypherScript = null;
    dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(cypherScript.getName(), ""));
    wConnection.setText(Const.NVL(cypherScript.getConnectionName(), ""));
    wScript.setText(Const.NVL(cypherScript.getScript(), ""));
    wReplaceVariables.setSelection(cypherScript.isReplacingVariables());
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "CypherScriptDialog.MissingName.Warning.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "CypherScriptDialog.MissingName.Warning.Message"));
      mb.open();
      return;
    }
    cypherScript.setName(wName.getText());
    cypherScript.setConnectionName(wConnection.getText());
    cypherScript.setScript(wScript.getText());
    cypherScript.setReplacingVariables(wReplaceVariables.getSelection());

    dispose();
  }
}
