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

package org.apache.hop.workflow.actions.abort;

import org.apache.hop.core.logging.LogLevel;
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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit a Action Abort object. */
public class ActionAbortDialog extends ActionDialog {
  private static final Class<?> PKG = ActionAbortDialog.class;

  private ActionAbort action;

  private boolean changed;

  private TextVar wMessageAbort;

  private Combo wMessageLogLevel;

  public ActionAbortDialog(
      Shell parent, ActionAbort action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionAbortDialog.ActionName.Label"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionAbortDialog.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();

    // Message line
    Label wlMessageAbort = new Label(shell, SWT.RIGHT);
    wlMessageAbort.setText(BaseMessages.getString(PKG, "ActionAbortDialog.MessageAbort.Label"));
    PropsUi.setLook(wlMessageAbort);
    FormData fdlMessageAbort = new FormData();
    fdlMessageAbort.left = new FormAttachment(0, 0);
    fdlMessageAbort.right = new FormAttachment(middle, -margin);
    fdlMessageAbort.top = new FormAttachment(wSpacer, margin);
    wlMessageAbort.setLayoutData(fdlMessageAbort);

    wMessageAbort = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMessageAbort);
    wMessageAbort.setToolTipText(
        BaseMessages.getString(PKG, "ActionAbortDialog.MessageAbort.Tooltip"));
    wMessageAbort.addModifyListener(e -> action.setChanged());
    FormData fdMessageAbort = new FormData();
    fdMessageAbort.left = new FormAttachment(middle, 0);
    fdMessageAbort.top = new FormAttachment(wlMessageAbort, 0, SWT.CENTER);
    fdMessageAbort.right = new FormAttachment(100, 0);
    wMessageAbort.setLayoutData(fdMessageAbort);

    // Log level of the message
    Label wlMessageLogLevel = new Label(shell, SWT.RIGHT);
    wlMessageLogLevel.setText(
        BaseMessages.getString(PKG, "ActionAbortDialog.MessageLogLevels.Label"));
    PropsUi.setLook(wlMessageLogLevel);
    FormData fdlMessageLogLevel = new FormData();
    fdlMessageLogLevel.left = new FormAttachment(0, 0);
    fdlMessageLogLevel.right = new FormAttachment(middle, -margin);
    fdlMessageLogLevel.top = new FormAttachment(wMessageAbort, margin);
    wlMessageLogLevel.setLayoutData(fdlMessageLogLevel);

    wMessageLogLevel = new Combo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wMessageLogLevel.setItems(LogLevel.getLogLevelDescriptions());
    PropsUi.setLook(wMessageLogLevel);
    wMessageLogLevel.setToolTipText(
        BaseMessages.getString(PKG, "ActionAbortDialog.MessageLogLevels.Tooltip"));
    wMessageLogLevel.addModifyListener(e -> action.setChanged());
    FormData fdMessageLogLevel = new FormData();
    fdMessageLogLevel.left = new FormAttachment(middle, 0);
    fdMessageLogLevel.top = new FormAttachment(wlMessageLogLevel, 0, SWT.CENTER);
    fdMessageLogLevel.right = new FormAttachment(100, 0);
    wMessageLogLevel.setLayoutData(fdMessageLogLevel);

    getData();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getMessageAbort() != null) {
      wMessageAbort.setText(action.getMessageAbort());
    }
    wMessageLogLevel.select(action.getMessageLogLevel().getLevel());
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
    action.setMessageAbort(wMessageAbort.getText());
    if (wMessageLogLevel.getSelectionIndex() != -1) {
      action.setMessageLogLevel(LogLevel.values()[wMessageLogLevel.getSelectionIndex()]);
    }
    dispose();
  }
}
