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

package org.apache.hop.workflow.actions.createfolder;

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
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the Create Folder action settings. */
public class ActionCreateFolderDialog extends ActionDialog {
  private static final Class<?> PKG = ActionCreateFolder.class;

  private TextVar wFoldername;

  private Button wAbortExists;

  private ActionCreateFolder action;

  private boolean changed;

  public ActionCreateFolderDialog(
      Shell parent, ActionCreateFolder action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionCreateFolder.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionCreateFolder.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();

    ModifyListener lsMod = e -> action.setChanged();

    // Foldername line
    Label wlFoldername = new Label(shell, SWT.RIGHT);
    wlFoldername.setText(BaseMessages.getString(PKG, "ActionCreateFolder.Foldername.Label"));
    PropsUi.setLook(wlFoldername);
    FormData fdlFoldername = new FormData();
    fdlFoldername.left = new FormAttachment(0, 0);
    fdlFoldername.top = new FormAttachment(wSpacer, margin);
    fdlFoldername.right = new FormAttachment(middle, -margin);
    wlFoldername.setLayoutData(fdlFoldername);

    Button wbFoldername = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFoldername);
    wbFoldername.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFoldername = new FormData();
    fdbFoldername.right = new FormAttachment(100, 0);
    fdbFoldername.top = new FormAttachment(wlFoldername, 0, SWT.CENTER);
    wbFoldername.setLayoutData(fdbFoldername);

    wFoldername = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFoldername);
    wFoldername.addModifyListener(lsMod);
    FormData fdFoldername = new FormData();
    fdFoldername.left = new FormAttachment(middle, 0);
    fdFoldername.top = new FormAttachment(wlFoldername, 0, SWT.CENTER);
    fdFoldername.right = new FormAttachment(wbFoldername, -margin);
    wFoldername.setLayoutData(fdFoldername);

    // Whenever something changes, set the tooltip to the expanded version:
    wFoldername.addModifyListener(
        e -> wFoldername.setToolTipText(variables.resolve(wFoldername.getText())));

    wbFoldername.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wFoldername, variables));

    Label wlAbortExists = new Label(shell, SWT.RIGHT);
    wlAbortExists.setText(BaseMessages.getString(PKG, "ActionCreateFolder.FailIfExists.Label"));
    PropsUi.setLook(wlAbortExists);
    FormData fdlAbortExists = new FormData();
    fdlAbortExists.left = new FormAttachment(0, 0);
    fdlAbortExists.top = new FormAttachment(wFoldername, margin);
    fdlAbortExists.right = new FormAttachment(middle, -margin);
    wlAbortExists.setLayoutData(fdlAbortExists);
    wAbortExists = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wAbortExists);
    wAbortExists.setToolTipText(
        BaseMessages.getString(PKG, "ActionCreateFolder.FailIfExists.Tooltip"));
    FormData fdAbortExists = new FormData();
    fdAbortExists.left = new FormAttachment(middle, 0);
    fdAbortExists.top = new FormAttachment(wlAbortExists, 0, SWT.CENTER);
    fdAbortExists.right = new FormAttachment(100, 0);
    wAbortExists.setLayoutData(fdAbortExists);
    wAbortExists.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    getData();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getFolderName() != null) {
      wFoldername.setText(action.getFolderName());
    }
    wAbortExists.setSelection(action.isFailIfFolderExists());
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
    action.setFolderName(wFoldername.getText());
    action.setFailIfFolderExists(wAbortExists.getSelection());
    dispose();
  }
}
