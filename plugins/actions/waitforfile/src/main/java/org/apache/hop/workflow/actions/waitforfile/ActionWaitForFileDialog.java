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

package org.apache.hop.workflow.actions.waitforfile;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * This dialog allows you to edit the Wait For File action settings.
 *
 * @author Sven Boden
 * @since 28-01-2007
 */
public class ActionWaitForFileDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionWaitForFile.class; // For Translator

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionWaitForFile.Filetype.All")};

  private Text wName;

  private TextVar wFilename;

  private TextVar wMaximumTimeout;

  private TextVar wCheckCycleTime;

  private Button wSuccessOnTimeout;

  private Button wFileSizeCheck;

  private Button wAddFilenameResult;

  private ActionWaitForFile action;
  private Shell shell;

  private boolean changed;

  public ActionWaitForFileDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionWaitForFile) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionWaitForFile.Name.Default"));
    }
  }

  public IAction open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    props.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionWaitForFile.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionWaitForFile.Name.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Filename line
    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ActionWaitForFile.Filename.Label"));
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wName, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wName, 0);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wName, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename, variables, new String[] {"*"}, FILETYPES, true));

    // Maximum timeout
    Label wlMaximumTimeout = new Label(shell, SWT.RIGHT);
    wlMaximumTimeout.setText(BaseMessages.getString(PKG, "ActionWaitForFile.MaximumTimeout.Label"));
    props.setLook(wlMaximumTimeout);
    FormData fdlMaximumTimeout = new FormData();
    fdlMaximumTimeout.left = new FormAttachment(0, 0);
    fdlMaximumTimeout.top = new FormAttachment(wFilename, margin);
    fdlMaximumTimeout.right = new FormAttachment(middle, -margin);
    wlMaximumTimeout.setLayoutData(fdlMaximumTimeout);
    wMaximumTimeout = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaximumTimeout);
    wMaximumTimeout.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.MaximumTimeout.Tooltip"));
    wMaximumTimeout.addModifyListener(lsMod);
    FormData fdMaximumTimeout = new FormData();
    fdMaximumTimeout.left = new FormAttachment(middle, 0);
    fdMaximumTimeout.top = new FormAttachment(wFilename, margin);
    fdMaximumTimeout.right = new FormAttachment(100, 0);
    wMaximumTimeout.setLayoutData(fdMaximumTimeout);

    // Cycle time
    Label wlCheckCycleTime = new Label(shell, SWT.RIGHT);
    wlCheckCycleTime.setText(BaseMessages.getString(PKG, "ActionWaitForFile.CheckCycleTime.Label"));
    props.setLook(wlCheckCycleTime);
    FormData fdlCheckCycleTime = new FormData();
    fdlCheckCycleTime.left = new FormAttachment(0, 0);
    fdlCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdlCheckCycleTime.right = new FormAttachment(middle, -margin);
    wlCheckCycleTime.setLayoutData(fdlCheckCycleTime);
    wCheckCycleTime = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wCheckCycleTime);
    wCheckCycleTime.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.CheckCycleTime.Tooltip"));
    wCheckCycleTime.addModifyListener(lsMod);
    FormData fdCheckCycleTime = new FormData();
    fdCheckCycleTime.left = new FormAttachment(middle, 0);
    fdCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdCheckCycleTime.right = new FormAttachment(100, 0);
    wCheckCycleTime.setLayoutData(fdCheckCycleTime);

    // Success on timeout
    Label wlSuccessOnTimeout = new Label(shell, SWT.RIGHT);
    wlSuccessOnTimeout.setText(
        BaseMessages.getString(PKG, "ActionWaitForFile.SuccessOnTimeout.Label"));
    props.setLook(wlSuccessOnTimeout);
    FormData fdlSuccessOnTimeout = new FormData();
    fdlSuccessOnTimeout.left = new FormAttachment(0, 0);
    fdlSuccessOnTimeout.top = new FormAttachment(wCheckCycleTime, margin);
    fdlSuccessOnTimeout.right = new FormAttachment(middle, -margin);
    wlSuccessOnTimeout.setLayoutData(fdlSuccessOnTimeout);
    wSuccessOnTimeout = new Button(shell, SWT.CHECK);
    props.setLook(wSuccessOnTimeout);
    wSuccessOnTimeout.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.SuccessOnTimeout.Tooltip"));
    FormData fdSuccessOnTimeout = new FormData();
    fdSuccessOnTimeout.left = new FormAttachment(middle, 0);
    fdSuccessOnTimeout.top = new FormAttachment(wlSuccessOnTimeout, 0, SWT.CENTER);
    fdSuccessOnTimeout.right = new FormAttachment(100, 0);
    wSuccessOnTimeout.setLayoutData(fdSuccessOnTimeout);
    wSuccessOnTimeout.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Check file size
    Label wlFileSizeCheck = new Label(shell, SWT.RIGHT);
    wlFileSizeCheck.setText(BaseMessages.getString(PKG, "ActionWaitForFile.FileSizeCheck.Label"));
    props.setLook(wlFileSizeCheck);
    FormData fdlFileSizeCheck = new FormData();
    fdlFileSizeCheck.left = new FormAttachment(0, 0);
    fdlFileSizeCheck.top = new FormAttachment(wlSuccessOnTimeout, 2 * margin);
    fdlFileSizeCheck.right = new FormAttachment(middle, -margin);
    wlFileSizeCheck.setLayoutData(fdlFileSizeCheck);
    wFileSizeCheck = new Button(shell, SWT.CHECK);
    props.setLook(wFileSizeCheck);
    wFileSizeCheck.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.FileSizeCheck.Tooltip"));
    FormData fdFileSizeCheck = new FormData();
    fdFileSizeCheck.left = new FormAttachment(middle, 0);
    fdFileSizeCheck.top = new FormAttachment(wlFileSizeCheck, 0, SWT.CENTER);
    fdFileSizeCheck.right = new FormAttachment(100, 0);
    wFileSizeCheck.setLayoutData(fdFileSizeCheck);
    wFileSizeCheck.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Add filename to result filenames
    Label wlAddFilenameResult = new Label(shell, SWT.RIGHT);
    wlAddFilenameResult.setText(
        BaseMessages.getString(PKG, "ActionWaitForFile.AddFilenameResult.Label"));
    props.setLook(wlAddFilenameResult);
    FormData fdlAddFilenameResult = new FormData();
    fdlAddFilenameResult.left = new FormAttachment(0, 0);
    fdlAddFilenameResult.top = new FormAttachment(wlFileSizeCheck, 2 * margin);
    fdlAddFilenameResult.right = new FormAttachment(middle, -margin);
    wlAddFilenameResult.setLayoutData(fdlAddFilenameResult);
    wAddFilenameResult = new Button(shell, SWT.CHECK);
    props.setLook(wAddFilenameResult);
    wAddFilenameResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.AddFilenameResult.Tooltip"));
    FormData fdAddFilenameResult = new FormData();
    fdAddFilenameResult.left = new FormAttachment(middle, 0);
    fdAddFilenameResult.top = new FormAttachment(wlAddFilenameResult, 0, SWT.CENTER);
    fdAddFilenameResult.right = new FormAttachment(100, 0);
    wAddFilenameResult.setLayoutData(fdAddFilenameResult);
    wAddFilenameResult.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, 2 * margin, wlAddFilenameResult);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    props.setScreen(winprop);
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wFilename.setText(Const.NVL(action.getFilename(), ""));
    wMaximumTimeout.setText(Const.NVL(action.getMaximumTimeout(), ""));
    wCheckCycleTime.setText(Const.NVL(action.getCheckCycleTime(), ""));
    wSuccessOnTimeout.setSelection(action.isSuccessOnTimeout());
    wFileSizeCheck.setSelection(action.isFileSizeCheck());
    wAddFilenameResult.setSelection(action.isAddFilenameToResult());

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
    action.setMaximumTimeout(wMaximumTimeout.getText());
    action.setCheckCycleTime(wCheckCycleTime.getText());
    action.setSuccessOnTimeout(wSuccessOnTimeout.getSelection());
    action.setFileSizeCheck(wFileSizeCheck.getSelection());
    action.setAddFilenameToResult(wAddFilenameResult.getSelection());
    dispose();
  }
}
