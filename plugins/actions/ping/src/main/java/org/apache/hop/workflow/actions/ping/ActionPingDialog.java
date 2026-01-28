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

package org.apache.hop.workflow.actions.ping;

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
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the ping action settings. */
public class ActionPingDialog extends ActionDialog {
  private static final Class<?> PKG = ActionPing.class;

  private TextVar wHostname;

  private Label wlTimeOut;
  private TextVar wTimeOut;

  private ActionPing action;

  private CCombo wPingType;

  private Label wlNbrPackets;
  private TextVar wNbrPackets;

  private boolean changed;

  public ActionPingDialog(
      Shell parent, ActionPing action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionPing.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionPing.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();

    ModifyListener lsMod = e -> action.setChanged();

    // hostname line
    Label wlHostname = new Label(shell, SWT.RIGHT);
    wlHostname.setText(BaseMessages.getString(PKG, "ActionPing.Hostname.Label"));
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

    Label wlPingType = new Label(shell, SWT.RIGHT);
    wlPingType.setText(BaseMessages.getString(PKG, "ActionPing.PingType.Label"));
    PropsUi.setLook(wlPingType);
    FormData fdlPingType = new FormData();
    fdlPingType.left = new FormAttachment(0, 0);
    fdlPingType.right = new FormAttachment(middle, -margin);
    fdlPingType.top = new FormAttachment(wHostname, margin);
    wlPingType.setLayoutData(fdlPingType);
    wPingType = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wPingType.add(BaseMessages.getString(PKG, "ActionPing.ClassicPing.Label"));
    wPingType.add(BaseMessages.getString(PKG, "ActionPing.SystemPing.Label"));
    wPingType.add(BaseMessages.getString(PKG, "ActionPing.BothPings.Label"));
    wPingType.select(1); // +1: starts at -1
    PropsUi.setLook(wPingType);
    FormData fdPingType = new FormData();
    fdPingType.left = new FormAttachment(middle, 0);
    fdPingType.top = new FormAttachment(wHostname, margin);
    fdPingType.right = new FormAttachment(100, 0);
    wPingType.setLayoutData(fdPingType);
    wPingType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setPingType();
            action.setChanged();
          }
        });

    // Timeout
    wlTimeOut = new Label(shell, SWT.RIGHT);
    wlTimeOut.setText(BaseMessages.getString(PKG, "ActionPing.TimeOut.Label"));
    PropsUi.setLook(wlTimeOut);
    FormData fdlTimeOut = new FormData();
    fdlTimeOut.left = new FormAttachment(0, 0);
    fdlTimeOut.right = new FormAttachment(middle, -margin);
    fdlTimeOut.top = new FormAttachment(wPingType, margin);
    wlTimeOut.setLayoutData(fdlTimeOut);

    wTimeOut = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wlTimeOut.setToolTipText(BaseMessages.getString(PKG, "ActionPing.TimeOut.Tooltip"));
    PropsUi.setLook(wTimeOut);
    wTimeOut.addModifyListener(lsMod);
    FormData fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment(middle, 0);
    fdTimeOut.top = new FormAttachment(wPingType, margin);
    fdTimeOut.right = new FormAttachment(100, 0);
    wTimeOut.setLayoutData(fdTimeOut);

    // Nbr packets to send
    wlNbrPackets = new Label(shell, SWT.RIGHT);
    wlNbrPackets.setText(BaseMessages.getString(PKG, "ActionPing.NrPackets.Label"));
    PropsUi.setLook(wlNbrPackets);
    FormData fdlNbrPackets = new FormData();
    fdlNbrPackets.left = new FormAttachment(0, 0);
    fdlNbrPackets.right = new FormAttachment(middle, -margin);
    fdlNbrPackets.top = new FormAttachment(wTimeOut, margin);
    wlNbrPackets.setLayoutData(fdlNbrPackets);

    wNbrPackets = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNbrPackets);
    wNbrPackets.addModifyListener(lsMod);
    FormData fdNbrPackets = new FormData();
    fdNbrPackets.left = new FormAttachment(middle, 0);
    fdNbrPackets.top = new FormAttachment(wTimeOut, margin);
    fdNbrPackets.right = new FormAttachment(100, 0);
    wNbrPackets.setLayoutData(fdNbrPackets);

    getData();
    setPingType();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void setPingType() {
    wlTimeOut.setEnabled(
        wPingType.getSelectionIndex() == action.isystemPing
            || wPingType.getSelectionIndex() == action.ibothPings);
    wTimeOut.setEnabled(
        wPingType.getSelectionIndex() == action.isystemPing
            || wPingType.getSelectionIndex() == action.ibothPings);
    wlNbrPackets.setEnabled(
        wPingType.getSelectionIndex() == action.iclassicPing
            || wPingType.getSelectionIndex() == action.ibothPings);
    wNbrPackets.setEnabled(
        wPingType.getSelectionIndex() == action.iclassicPing
            || wPingType.getSelectionIndex() == action.ibothPings);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    if (action.getHostname() != null) {
      wHostname.setText(action.getHostname());
    }
    if (action.getNbrPackets() != null) {
      wNbrPackets.setText(action.getNbrPackets());
    } else {
      wNbrPackets.setText("2");
    }

    if (action.getTimeout() != null) {
      wTimeOut.setText(action.getTimeout());
    } else {
      wTimeOut.setText("3000");
    }

    wPingType.select(action.ipingtype);
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
    action.setNbrPackets(wNbrPackets.getText());
    action.setTimeout(wTimeOut.getText());
    action.ipingtype = wPingType.getSelectionIndex();
    if (wPingType.getSelectionIndex() == action.isystemPing) {
      action.pingtype = action.systemPing;
    } else if (wPingType.getSelectionIndex() == action.ibothPings) {
      action.pingtype = action.bothPings;
    } else {
      action.pingtype = action.classicPing;
    }

    dispose();
  }
}
