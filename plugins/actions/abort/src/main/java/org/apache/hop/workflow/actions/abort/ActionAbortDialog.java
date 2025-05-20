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
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit a Action Abort object. */
public class ActionAbortDialog extends ActionDialog {
  private static final Class<?> PKG = ActionAbortDialog.class;

  private ActionAbort action;

  private boolean changed;

  private Text wName;

  private TextVar wMessageAbort;

  private Button wAlwaysLogRows;

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

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    shell.setMinimumSize(400, 200);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = (ModifyEvent e) -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    formLayout.spacing = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionAbortDialog.Title"));

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

    // Message line
    Label wlMessageAbort = new Label(shell, SWT.RIGHT);
    wlMessageAbort.setText(BaseMessages.getString(PKG, "ActionAbortDialog.MessageAbort.Label"));
    PropsUi.setLook(wlMessageAbort);
    FormData fdlMessageAbort = new FormData();
    fdlMessageAbort.left = new FormAttachment(0, 0);
    fdlMessageAbort.right = new FormAttachment(middle, -margin);
    fdlMessageAbort.top = new FormAttachment(wName, margin);
    wlMessageAbort.setLayoutData(fdlMessageAbort);

    wMessageAbort = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMessageAbort);
    wMessageAbort.setToolTipText(
        BaseMessages.getString(PKG, "ActionAbortDialog.MessageAbort.Tooltip"));
    wMessageAbort.addModifyListener(lsMod);
    FormData fdMessageAbort = new FormData();
    fdMessageAbort.left = new FormAttachment(middle, 0);
    fdMessageAbort.top = new FormAttachment(wName, margin);
    fdMessageAbort.right = new FormAttachment(100, 0);
    wMessageAbort.setLayoutData(fdMessageAbort);

    SelectionListener slMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        };
    // Always log rows
    wAlwaysLogRows = new Button(shell, SWT.CHECK);
    wAlwaysLogRows.setSelection(true);
    PropsUi.setLook(wAlwaysLogRows);
    wAlwaysLogRows.setText(BaseMessages.getString(PKG, "ActionAbortDialog.AlwaysLogRows.Label"));
    wAlwaysLogRows.setToolTipText(
        BaseMessages.getString(PKG, "ActionAbortDialog.AlwaysLogRows.Tooltip"));
    wAlwaysLogRows.addSelectionListener(slMod);
    FormData fdAlwaysLogRows = new FormData();
    fdAlwaysLogRows.left = new FormAttachment(middle, 0);
    fdAlwaysLogRows.top = new FormAttachment(wMessageAbort, margin);
    fdAlwaysLogRows.right = new FormAttachment(100, 0);
    wAlwaysLogRows.setLayoutData(fdAlwaysLogRows);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, (Event e) -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, (Event e) -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, 0, null);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getMessageAbort() != null) {
      wMessageAbort.setText(action.getMessageAbort());
    }
    wAlwaysLogRows.setSelection(action.isAlwaysLogRows());

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
    action.setMessageAbort(wMessageAbort.getText());
    action.setAlwaysLogRows(wAlwaysLogRows.getSelection());
    dispose();
  }
}
