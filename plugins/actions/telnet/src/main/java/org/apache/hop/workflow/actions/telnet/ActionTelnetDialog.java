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

package org.apache.hop.workflow.actions.telnet;

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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the Telnet action settings. */
public class ActionTelnetDialog extends ActionDialog {
  private static final Class<?> PKG = ActionTelnet.class;

  private TextVar wHostname;

  private TextVar wTimeOut;

  private TextVar wPort;

  private ActionTelnet action;

  private boolean changed;

  public ActionTelnetDialog(
      Shell parent, ActionTelnet action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionTelnet.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionTelnet.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();

    ModifyListener lsMod = e -> action.setChanged();

    // hostname line
    Label wlHostname = new Label(shell, SWT.RIGHT);
    wlHostname.setText(BaseMessages.getString(PKG, "ActionTelnet.Hostname.Label"));
    PropsUi.setLook(wlHostname);
    FormData fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment(0, 0);
    fdlHostname.top = new FormAttachment(wSpacer, margin);
    fdlHostname.right = new FormAttachment(middle, -margin);
    wlHostname.setLayoutData(fdlHostname);

    wHostname = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wHostname);
    wHostname.addModifyListener(lsMod);
    FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment(middle, 0);
    fdHostname.top = new FormAttachment(wlHostname, 0, SWT.CENTER);
    fdHostname.right = new FormAttachment(100, 0);
    wHostname.setLayoutData(fdHostname);

    // Whenever something changes, set the tooltip to the expanded version:
    wHostname.addModifyListener(
        e -> wHostname.setToolTipText(variables.resolve(wHostname.getText())));

    Label wlPort = new Label(shell, SWT.RIGHT);
    wlPort.setText(BaseMessages.getString(PKG, "ActionTelnet.Port.Label"));
    PropsUi.setLook(wlPort);
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment(0, -margin);
    fdlPort.right = new FormAttachment(middle, -margin);
    fdlPort.top = new FormAttachment(wHostname, margin);
    wlPort.setLayoutData(fdlPort);

    wPort = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPort);
    wPort.addModifyListener(lsMod);
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(middle, 0);
    fdPort.top = new FormAttachment(wHostname, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);

    Label wlTimeOut = new Label(shell, SWT.RIGHT);
    wlTimeOut.setText(BaseMessages.getString(PKG, "ActionTelnet.TimeOut.Label"));
    PropsUi.setLook(wlTimeOut);
    FormData fdlTimeOut = new FormData();
    fdlTimeOut.left = new FormAttachment(0, -margin);
    fdlTimeOut.right = new FormAttachment(middle, -margin);
    fdlTimeOut.top = new FormAttachment(wPort, margin);
    wlTimeOut.setLayoutData(fdlTimeOut);

    wTimeOut = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTimeOut);
    wTimeOut.addModifyListener(lsMod);
    FormData fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment(middle, 0);
    fdTimeOut.top = new FormAttachment(wPort, margin);
    fdTimeOut.right = new FormAttachment(100, 0);
    wTimeOut.setLayoutData(fdTimeOut);

    getData();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    if (action.getHostname() != null) {
      wHostname.setText(action.getHostname());
    }

    wPort.setText(Const.NVL(action.getPort(), String.valueOf(ActionTelnet.DEFAULT_PORT)));
    wTimeOut.setText(Const.NVL(action.getTimeout(), String.valueOf(ActionTelnet.DEFAULT_TIME_OUT)));
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
    action.setHostname(wHostname.getText());
    action.setPort(wPort.getText());
    action.setTimeout(wTimeOut.getText());

    dispose();
  }
}
