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

package org.apache.hop.workflow.actions.setpasswordencoder;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Dialog for {@link ActionSetPasswordEncoder}. */
public class ActionSetPasswordEncoderDialog extends ActionDialog {
  private static final Class<?> PKG = ActionSetPasswordEncoder.class;

  private ActionSetPasswordEncoder action;
  private boolean changed;

  private Combo wEncoderPluginId;
  private TextVar wKeyVariable;
  private TextVar wKeyFile;

  public ActionSetPasswordEncoderDialog(
      Shell parent,
      ActionSetPasswordEncoder action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionSetPasswordEncoder.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionSetPasswordEncoder.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();
    ModifyListener lsMod = e -> action.setChanged();

    // Encoder plugin ID
    Label wlEncoderPluginId = new Label(shell, SWT.RIGHT);
    wlEncoderPluginId.setText(
        BaseMessages.getString(PKG, "ActionSetPasswordEncoder.EncoderPluginId.Label"));
    PropsUi.setLook(wlEncoderPluginId);
    FormData fdlEncoderPluginId = new FormData();
    fdlEncoderPluginId.left = new FormAttachment(0, 0);
    fdlEncoderPluginId.right = new FormAttachment(middle, -margin);
    fdlEncoderPluginId.top = new FormAttachment(wSpacer, margin);
    wlEncoderPluginId.setLayoutData(fdlEncoderPluginId);

    wEncoderPluginId = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEncoderPluginId.setItems(new String[] {"Hop", "AES2", "AES"});
    PropsUi.setLook(wEncoderPluginId);
    wEncoderPluginId.addModifyListener(lsMod);
    FormData fdEncoderPluginId = new FormData();
    fdEncoderPluginId.left = new FormAttachment(middle, 0);
    fdEncoderPluginId.top = new FormAttachment(wlEncoderPluginId, 0, SWT.CENTER);
    fdEncoderPluginId.right = new FormAttachment(100, 0);
    wEncoderPluginId.setLayoutData(fdEncoderPluginId);

    // Key variable name
    Label wlKeyVariable = new Label(shell, SWT.RIGHT);
    wlKeyVariable.setText(
        BaseMessages.getString(PKG, "ActionSetPasswordEncoder.KeyVariable.Label"));
    PropsUi.setLook(wlKeyVariable);
    FormData fdlKeyVariable = new FormData();
    fdlKeyVariable.left = new FormAttachment(0, 0);
    fdlKeyVariable.top = new FormAttachment(wEncoderPluginId, margin);
    fdlKeyVariable.right = new FormAttachment(middle, -margin);
    wlKeyVariable.setLayoutData(fdlKeyVariable);

    wKeyVariable = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyVariable);
    wKeyVariable.addModifyListener(lsMod);
    FormData fdKeyVariable = new FormData();
    fdKeyVariable.left = new FormAttachment(middle, 0);
    fdKeyVariable.top = new FormAttachment(wEncoderPluginId, margin);
    fdKeyVariable.right = new FormAttachment(100, 0);
    wKeyVariable.setLayoutData(fdKeyVariable);
    wKeyVariable.setToolTipText(
        BaseMessages.getString(PKG, "ActionSetPasswordEncoder.KeyVariable.Tooltip"));

    // Key file
    Label wlKeyFile = new Label(shell, SWT.RIGHT);
    wlKeyFile.setText(BaseMessages.getString(PKG, "ActionSetPasswordEncoder.KeyFile.Label"));
    PropsUi.setLook(wlKeyFile);
    FormData fdlKeyFile = new FormData();
    fdlKeyFile.left = new FormAttachment(0, 0);
    fdlKeyFile.top = new FormAttachment(wKeyVariable, margin);
    fdlKeyFile.right = new FormAttachment(middle, -margin);
    wlKeyFile.setLayoutData(fdlKeyFile);

    wKeyFile = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyFile);
    wKeyFile.addModifyListener(lsMod);
    FormData fdKeyFile = new FormData();
    fdKeyFile.left = new FormAttachment(middle, 0);
    fdKeyFile.top = new FormAttachment(wKeyVariable, margin);
    fdKeyFile.right = new FormAttachment(100, 0);
    wKeyFile.setLayoutData(fdKeyFile);
    wKeyFile.setToolTipText(
        BaseMessages.getString(PKG, "ActionSetPasswordEncoder.KeyFile.Tooltip"));

    getData();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wEncoderPluginId.setText(Const.NVL(action.getEncoderPluginId(), "AES2"));
    wKeyVariable.setText(Const.NVL(action.getKeyVariable(), Const.HOP_AES_ENCODER_KEY));
    wKeyFile.setText(Const.nullToEmpty(action.getKeyFile()));
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
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setEncoderPluginId(wEncoderPluginId.getText());
    action.setKeyVariable(wKeyVariable.getText());
    action.setKeyFile(wKeyFile.getText());
    dispose();
  }
}
