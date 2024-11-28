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

package org.apache.hop.workflow.actions.as400command;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit a Action As400Command metadata. */
public class ActionAs400CommandDialog extends ActionDialog {
  private static final Class<?> PKG = ActionAs400CommandDialog.class;

  private ActionAs400Command action;

  private boolean changed;

  private Text wName;

  private LabelTextVar wServerName;

  private LabelTextVar wUserName;

  private LabelTextVar wPassword;

  private LabelTextVar wProxyHost;

  private LabelTextVar wProxyPort;

  private LabelTextVar wCommand;

  public ActionAs400CommandDialog(
      Shell parent, ActionAs400Command action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionAs400CommandDialog.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    shell.setMinimumSize(new Point(600, 400));
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = (ModifyEvent e) -> action.setChanged();
    changed = action.hasChanged();

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionAs400CommandDialog.Shell.Title"));

    Label wicon = new Label(shell, SWT.RIGHT);
    wicon.setImage(shell.getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wicon.setLayoutData(fdlicon);
    PropsUi.setLook(wicon);

    Label wlName = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ActionAs400CommandDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.top = new FormAttachment(wicon, 0, SWT.CENTER);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);

    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.right = new FormAttachment(wicon, -margin);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(wlName, margin);
    wName.setLayoutData(fdName);

    Label spacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wicon, 0);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    Group systemGroup = new Group(shell, SWT.SHADOW_NONE);
    systemGroup.setText(BaseMessages.getString(PKG, "ActionAs400CommandDialog.System.Group.Label"));
    FormLayout systemGroupLayout = new FormLayout();
    systemGroupLayout.marginWidth = PropsUi.getFormMargin();
    systemGroupLayout.marginHeight = PropsUi.getFormMargin();
    systemGroup.setLayout(systemGroupLayout);
    systemGroup.setLayoutData(
        new FormDataBuilder().top(spacer, PropsUi.getFormMargin()).fullWidth().result());
    PropsUi.setLook(systemGroup);

    // Widget ServerName
    wServerName =
        new LabelTextVar(
            variables,
            systemGroup,
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.Server.Label"),
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.Server.Tooltip"));
    wServerName.addModifyListener(lsMod);
    wServerName.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(wServerName);

    // Widget UserName
    wUserName =
        new LabelTextVar(
            variables,
            systemGroup,
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.User.Label"),
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.User.Tooltip"));
    wUserName.setLayoutData(new FormDataBuilder().top(wServerName).fullWidth().result());
    wUserName.addModifyListener(lsMod);
    PropsUi.setLook(wUserName);

    // Widget Password
    wPassword =
        new LabelTextVar(
            variables,
            systemGroup,
            SWT.LEFT | SWT.BORDER | SWT.PASSWORD,
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.Password.Label"),
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.Password.Tooltip"));
    wPassword.setLayoutData(new FormDataBuilder().top(wUserName).fullWidth().result());
    wPassword.addModifyListener(lsMod);
    PropsUi.setLook(wPassword);

    Group proxyGroup = new Group(shell, SWT.SHADOW_NONE);
    proxyGroup.setText(BaseMessages.getString(PKG, "ActionAs400CommandDialog.Proxy.Group.Label"));
    FormLayout proxyGroupLayout = new FormLayout();
    proxyGroupLayout.marginWidth = PropsUi.getFormMargin();
    proxyGroupLayout.marginHeight = PropsUi.getFormMargin();
    proxyGroup.setLayout(proxyGroupLayout);
    proxyGroup.setLayoutData(
        new FormDataBuilder().top(systemGroup, PropsUi.getFormMargin()).fullWidth().result());
    PropsUi.setLook(proxyGroup);

    // Widget proxy host
    wProxyHost =
        new LabelTextVar(
            variables,
            proxyGroup,
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.ProxyHost.Label"),
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.ProxyHost.Tooltip"));
    wProxyHost.addModifyListener(lsMod);
    wProxyHost.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(wProxyHost);

    // Widget UserName
    wProxyPort =
        new LabelTextVar(
            variables,
            proxyGroup,
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.ProxyPort.Label"),
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.ProxyPort.Tooltip"));
    wProxyPort.setLayoutData(new FormDataBuilder().top(wProxyHost).fullWidth().result());
    wProxyPort.addModifyListener(lsMod);
    PropsUi.setLook(wProxyPort);

    Group commandGroup = new Group(shell, SWT.SHADOW_NONE);
    commandGroup.setText(
        BaseMessages.getString(PKG, "ActionAs400CommandDialog.Command.Group.Label"));
    FormLayout commandGroupLayout = new FormLayout();
    commandGroupLayout.marginWidth = PropsUi.getFormMargin();
    commandGroupLayout.marginHeight = PropsUi.getFormMargin();
    commandGroup.setLayout(commandGroupLayout);
    commandGroup.setLayoutData(
        new FormDataBuilder().top(proxyGroup, PropsUi.getFormMargin()).fullWidth().result());
    PropsUi.setLook(commandGroup);

    // Widget Command
    wCommand =
        new LabelTextVar(
            variables,
            commandGroup,
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.Command.Label"),
            BaseMessages.getString(PKG, "ActionAs400CommandDialog.Command.Tooltip"));
    wCommand.setLayoutData(new FormDataBuilder().fullWidth().result());
    wCommand.addModifyListener(lsMod);
    PropsUi.setLook(wCommand);

    // at the bottom
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, (Event e) -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, (Event e) -> cancel());
    Button wTest = new Button(shell, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "ActionAs400CommandDialog.TestConnection.Label"));
    wTest.addListener(SWT.Selection, (Event e) -> onTest());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wTest, wOk, wCancel}, PropsUi.getMargin(), null);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    wServerName.setText(Const.NVL(action.getServer(), ""));
    wUserName.setText(Const.NVL(action.getUser(), ""));
    wPassword.setText(Const.NVL(action.getPassword(), ""));
    wCommand.setText(Const.NVL(action.getCommand(), ""));
    wProxyHost.setText(Const.NVL(action.getProxyHost(), ""));
    wProxyPort.setText(Const.NVL(action.getProxyPort(), ""));

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

    action.setName(wName.getText());
    action.setServer(wServerName.getText());
    action.setUser(wUserName.getText());
    action.setPassword(wPassword.getText());
    action.setCommand(wCommand.getText());
    action.setProxyHost(wProxyHost.getText());
    action.setProxyPort(wProxyPort.getText());

    dispose();
  }

  protected void onTest() {
    String server = wServerName.getText();
    String user = wUserName.getText();
    String password = wPassword.getText();
    String proxyHost = wProxyHost.getText();
    String proxyPort = wProxyPort.getText();

    try {
      this.action.test(variables, server, user, password, proxyHost, proxyPort);

      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setText(
          BaseMessages.getString(PKG, "ActionAs400CommandDialog.TestConnection.Shell.Title"));
      mb.setMessage(
          BaseMessages.getString(PKG, "ActionAs400CommandDialog.TestConnection.Success", server));
      mb.open();

    } catch (Exception e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(
          BaseMessages.getString(PKG, "ActionAs400CommandDialog.TestConnection.Shell.Title"));
      mb.setMessage(
          BaseMessages.getString(
              PKG, "ActionAs400CommandDialog.TestConnection.Failed", server, e.getMessage()));
      mb.open();
    }
  }
}
