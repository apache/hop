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

package org.apache.hop.workflow.actions.webserviceavailable;

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

/** This dialog allows you to edit the webservice available action. */
public class ActionWebServiceAvailableDialog extends ActionDialog {
  private static final Class<?> PKG = ActionWebServiceAvailable.class;

  private TextVar wURL;

  private TextVar wConnectTimeOut;

  private TextVar wReadTimeOut;

  private ActionWebServiceAvailable action;

  private boolean changed;

  public ActionWebServiceAvailableDialog(
      Shell parent,
      ActionWebServiceAvailable action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionWebServiceAvailable.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionWebServiceAvailable.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();

    ModifyListener lsMod = e -> action.setChanged();

    // URL line
    Label wlURL = new Label(shell, SWT.RIGHT);
    wlURL.setText(BaseMessages.getString(PKG, "ActionWebServiceAvailable.URL.Label"));
    PropsUi.setLook(wlURL);
    FormData fdlURL = new FormData();
    fdlURL.left = new FormAttachment(0, 0);
    fdlURL.top = new FormAttachment(wSpacer, margin);
    fdlURL.right = new FormAttachment(middle, -margin);
    wlURL.setLayoutData(fdlURL);

    wURL = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wURL);
    wURL.addModifyListener(lsMod);
    FormData fdURL = new FormData();
    fdURL.left = new FormAttachment(middle, 0);
    fdURL.top = new FormAttachment(wlURL, 0, SWT.CENTER);
    fdURL.right = new FormAttachment(100, -margin);
    wURL.setLayoutData(fdURL);

    // Whenever something changes, set the tooltip to the expanded version:
    wURL.addModifyListener(e -> wURL.setToolTipText(variables.resolve(wURL.getText())));

    // connect timeout line
    Label wlConnectTimeOut = new Label(shell, SWT.RIGHT);
    wlConnectTimeOut.setText(
        BaseMessages.getString(PKG, "ActionWebServiceAvailable.ConnectTimeOut.Label"));
    PropsUi.setLook(wlConnectTimeOut);
    FormData fdlConnectTimeOut = new FormData();
    fdlConnectTimeOut.left = new FormAttachment(0, 0);
    fdlConnectTimeOut.top = new FormAttachment(wURL, margin);
    fdlConnectTimeOut.right = new FormAttachment(middle, -margin);
    wlConnectTimeOut.setLayoutData(fdlConnectTimeOut);

    wConnectTimeOut = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wConnectTimeOut.setToolTipText(
        BaseMessages.getString(PKG, "ActionWebServiceAvailable.ConnectTimeOut.Tooltip"));
    PropsUi.setLook(wConnectTimeOut);
    wConnectTimeOut.addModifyListener(lsMod);
    FormData fdConnectTimeOut = new FormData();
    fdConnectTimeOut.left = new FormAttachment(middle, 0);
    fdConnectTimeOut.top = new FormAttachment(wURL, margin);
    fdConnectTimeOut.right = new FormAttachment(100, -margin);
    wConnectTimeOut.setLayoutData(fdConnectTimeOut);

    // Whenever something changes, set the tooltip to the expanded version:
    wConnectTimeOut.addModifyListener(
        e -> wConnectTimeOut.setToolTipText(variables.resolve(wConnectTimeOut.getText())));

    // Read timeout line
    Label wlReadTimeOut = new Label(shell, SWT.RIGHT);
    wlReadTimeOut.setText(
        BaseMessages.getString(PKG, "ActionWebServiceAvailable.ReadTimeOut.Label"));
    PropsUi.setLook(wlReadTimeOut);
    FormData fdlReadTimeOut = new FormData();
    fdlReadTimeOut.left = new FormAttachment(0, 0);
    fdlReadTimeOut.top = new FormAttachment(wConnectTimeOut, margin);
    fdlReadTimeOut.right = new FormAttachment(middle, -margin);
    wlReadTimeOut.setLayoutData(fdlReadTimeOut);

    wReadTimeOut = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wReadTimeOut.setToolTipText(
        BaseMessages.getString(PKG, "ActionWebServiceAvailable.ReadTimeOut.Tooltip"));
    PropsUi.setLook(wReadTimeOut);
    wReadTimeOut.addModifyListener(lsMod);
    FormData fdReadTimeOut = new FormData();
    fdReadTimeOut.left = new FormAttachment(middle, 0);
    fdReadTimeOut.top = new FormAttachment(wConnectTimeOut, margin);
    fdReadTimeOut.right = new FormAttachment(100, -margin);
    wReadTimeOut.setLayoutData(fdReadTimeOut);

    // Whenever something changes, set the tooltip to the expanded version:
    wReadTimeOut.addModifyListener(
        e -> wReadTimeOut.setToolTipText(variables.resolve(wReadTimeOut.getText())));

    getData();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wURL.setText(Const.nullToEmpty(action.getUrl()));
    wConnectTimeOut.setText(Const.NVL(action.getConnectTimeOut(), "0"));
    wReadTimeOut.setText(Const.NVL(action.getReadTimeOut(), "0"));
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
    action.setUrl(wURL.getText());
    action.setConnectTimeOut(wConnectTimeOut.getText());
    action.setReadTimeOut(wReadTimeOut.getText());
    dispose();
  }
}
