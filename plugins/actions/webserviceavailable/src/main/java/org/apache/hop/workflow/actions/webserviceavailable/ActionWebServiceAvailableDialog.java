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
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit the webservice available action. */
public class ActionWebServiceAvailableDialog extends ActionDialog {
  private static final Class<?> PKG = ActionWebServiceAvailable.class;

  private Text wName;

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

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    shell.setMinimumSize(400, 220);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionWebServiceAvailable.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "System.ActionName.Label"));
    wlName.setToolTipText(BaseMessages.getString(PKG, "System.ActionName.Tooltip"));
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

    // URL line
    Label wlURL = new Label(shell, SWT.RIGHT);
    wlURL.setText(BaseMessages.getString(PKG, "ActionWebServiceAvailable.URL.Label"));
    PropsUi.setLook(wlURL);
    FormData fdlURL = new FormData();
    fdlURL.left = new FormAttachment(0, 0);
    fdlURL.top = new FormAttachment(wName, margin);
    fdlURL.right = new FormAttachment(middle, -margin);
    wlURL.setLayoutData(fdlURL);

    wURL = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wURL);
    wURL.addModifyListener(lsMod);
    FormData fdURL = new FormData();
    fdURL.left = new FormAttachment(middle, 0);
    fdURL.top = new FormAttachment(wName, margin);
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

    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);
    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wURL.setText(Const.nullToEmpty(action.getUrl()));
    wConnectTimeOut.setText(Const.NVL(action.getConnectTimeOut(), "0"));
    wReadTimeOut.setText(Const.NVL(action.getReadTimeOut(), "0"));

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
    action.setUrl(wURL.getText());
    action.setConnectTimeOut(wConnectTimeOut.getText());
    action.setReadTimeOut(wReadTimeOut.getText());
    dispose();
  }
}
