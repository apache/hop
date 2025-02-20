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

package org.apache.hop.workflow.actions.createfile;

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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit the Create File action settings. */
public class ActionCreateFileDialog extends ActionDialog {
  private static final Class<?> PKG = ActionCreateFile.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionCreateFile.Filetype.All")};

  private Text wName;

  private TextVar wFilename;

  private Button wAbortExists;

  private Button wAddFilenameToResult;

  private ActionCreateFile action;

  private boolean changed;

  public ActionCreateFileDialog(
      Shell parent, ActionCreateFile action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionCreateFile.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    shell.setMinimumSize(400, 210);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    Listener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionCreateFile.Title"));

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
    wName.addListener(SWT.Modify, lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Filename line
    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ActionCreateFile.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wName, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wName, 0);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addListener(SWT.Modify, lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wName, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addListener(
        SWT.Modify, e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename, variables, new String[] {"*"}, FILETYPES, true));

    Label wlAbortExists = new Label(shell, SWT.RIGHT);
    wlAbortExists.setText(BaseMessages.getString(PKG, "ActionCreateFile.FailIfExists.Label"));
    PropsUi.setLook(wlAbortExists);
    FormData fdlAbortExists = new FormData();
    fdlAbortExists.left = new FormAttachment(0, 0);
    fdlAbortExists.top = new FormAttachment(wFilename, margin);
    fdlAbortExists.right = new FormAttachment(middle, -margin);
    wlAbortExists.setLayoutData(fdlAbortExists);
    wAbortExists = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wAbortExists);
    wAbortExists.setToolTipText(
        BaseMessages.getString(PKG, "ActionCreateFile.FailIfExists.Tooltip"));
    FormData fdAbortExists = new FormData();
    fdAbortExists.left = new FormAttachment(middle, 0);
    fdAbortExists.top = new FormAttachment(wlAbortExists, 0, SWT.CENTER);
    fdAbortExists.right = new FormAttachment(100, 0);
    wAbortExists.setLayoutData(fdAbortExists);
    wAbortExists.addListener(SWT.Selection, lsMod);

    // Add filenames to result filenames...
    Label wlAddFilenameToResult = new Label(shell, SWT.RIGHT);
    wlAddFilenameToResult.setText(
        BaseMessages.getString(PKG, "ActionCreateFile.AddFilenameToResult.Label"));
    PropsUi.setLook(wlAddFilenameToResult);
    FormData fdlAddFilenameToResult = new FormData();
    fdlAddFilenameToResult.left = new FormAttachment(0, 0);
    fdlAddFilenameToResult.top = new FormAttachment(wlAbortExists, 2 * margin);
    fdlAddFilenameToResult.right = new FormAttachment(middle, -margin);
    wlAddFilenameToResult.setLayoutData(fdlAddFilenameToResult);
    wAddFilenameToResult = new Button(shell, SWT.CHECK);
    wAddFilenameToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionCreateFile.AddFilenameToResult.Tooltip"));
    PropsUi.setLook(wAddFilenameToResult);
    FormData fdAddFilenameToResult = new FormData();
    fdAddFilenameToResult.left = new FormAttachment(middle, 0);
    fdAddFilenameToResult.top = new FormAttachment(wlAddFilenameToResult, 0, SWT.CENTER);
    fdAddFilenameToResult.right = new FormAttachment(100, 0);
    wAddFilenameToResult.setLayoutData(fdAddFilenameToResult);

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
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getFilename() != null) {
      wFilename.setText(action.getFilename());
    }
    wAbortExists.setSelection(action.isFailIfFileExists());
    wAddFilenameToResult.setSelection(action.isAddFilenameToResult());

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
    action.setFilename(wFilename.getText());
    action.setFailIfFileExists(wAbortExists.getSelection());
    action.setAddFilenameToResult(wAddFilenameToResult.getSelection());
    dispose();
  }
}
