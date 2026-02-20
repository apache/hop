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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the Wait For File action settings. */
public class ActionWaitForFileDialog extends ActionDialog {
  private static final Class<?> PKG = ActionWaitForFile.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionWaitForFile.Filetype.All")};

  private TextVar wFilename;

  private TextVar wMaximumTimeout;

  private TextVar wCheckCycleTime;

  private Button wSuccessOnTimeout;

  private Button wFileSizeCheck;

  private Button wAddFilenameResult;

  private ActionWaitForFile action;

  private boolean changed;

  public ActionWaitForFileDialog(
      Shell parent, ActionWaitForFile action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionWaitForFile.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionWaitForFile.Title"), action);
    shell.setMinimumSize(400, 300);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, margin);
    fdSc.top = new FormAttachment(wSpacer, margin);
    fdSc.right = new FormAttachment(100, -margin);
    fdSc.bottom = new FormAttachment(wCancel, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    // Filename line
    Label wlFilename = new Label(wContent, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ActionWaitForFile.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wSpacer, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(wContent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wlFilename, 0, SWT.CENTER);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename, variables, new String[] {"*"}, FILETYPES, false));

    // Maximum timeout
    Label wlMaximumTimeout = new Label(wContent, SWT.RIGHT);
    wlMaximumTimeout.setText(BaseMessages.getString(PKG, "ActionWaitForFile.MaximumTimeout.Label"));
    PropsUi.setLook(wlMaximumTimeout);
    FormData fdlMaximumTimeout = new FormData();
    fdlMaximumTimeout.left = new FormAttachment(0, 0);
    fdlMaximumTimeout.top = new FormAttachment(wFilename, margin);
    fdlMaximumTimeout.right = new FormAttachment(middle, -margin);
    wlMaximumTimeout.setLayoutData(fdlMaximumTimeout);
    wMaximumTimeout = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaximumTimeout);
    wMaximumTimeout.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.MaximumTimeout.Tooltip"));
    wMaximumTimeout.addModifyListener(lsMod);
    FormData fdMaximumTimeout = new FormData();
    fdMaximumTimeout.left = new FormAttachment(middle, 0);
    fdMaximumTimeout.top = new FormAttachment(wFilename, margin);
    fdMaximumTimeout.right = new FormAttachment(100, 0);
    wMaximumTimeout.setLayoutData(fdMaximumTimeout);

    // Cycle time
    Label wlCheckCycleTime = new Label(wContent, SWT.RIGHT);
    wlCheckCycleTime.setText(BaseMessages.getString(PKG, "ActionWaitForFile.CheckCycleTime.Label"));
    PropsUi.setLook(wlCheckCycleTime);
    FormData fdlCheckCycleTime = new FormData();
    fdlCheckCycleTime.left = new FormAttachment(0, 0);
    fdlCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdlCheckCycleTime.right = new FormAttachment(middle, -margin);
    wlCheckCycleTime.setLayoutData(fdlCheckCycleTime);
    wCheckCycleTime = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCheckCycleTime);
    wCheckCycleTime.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.CheckCycleTime.Tooltip"));
    wCheckCycleTime.addModifyListener(lsMod);
    FormData fdCheckCycleTime = new FormData();
    fdCheckCycleTime.left = new FormAttachment(middle, 0);
    fdCheckCycleTime.top = new FormAttachment(wMaximumTimeout, margin);
    fdCheckCycleTime.right = new FormAttachment(100, 0);
    wCheckCycleTime.setLayoutData(fdCheckCycleTime);

    // Success on timeout
    Label wlSuccessOnTimeout = new Label(wContent, SWT.RIGHT);
    wlSuccessOnTimeout.setText(
        BaseMessages.getString(PKG, "ActionWaitForFile.SuccessOnTimeout.Label"));
    PropsUi.setLook(wlSuccessOnTimeout);
    FormData fdlSuccessOnTimeout = new FormData();
    fdlSuccessOnTimeout.left = new FormAttachment(0, 0);
    fdlSuccessOnTimeout.top = new FormAttachment(wCheckCycleTime, margin);
    fdlSuccessOnTimeout.right = new FormAttachment(middle, -margin);
    wlSuccessOnTimeout.setLayoutData(fdlSuccessOnTimeout);
    wSuccessOnTimeout = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wSuccessOnTimeout);
    wSuccessOnTimeout.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.SuccessOnTimeout.Tooltip"));
    FormData fdSuccessOnTimeout = new FormData();
    fdSuccessOnTimeout.left = new FormAttachment(middle, 0);
    fdSuccessOnTimeout.top = new FormAttachment(wlSuccessOnTimeout, 0, SWT.CENTER);
    fdSuccessOnTimeout.right = new FormAttachment(100, 0);
    wSuccessOnTimeout.setLayoutData(fdSuccessOnTimeout);
    wSuccessOnTimeout.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Check file size
    Label wlFileSizeCheck = new Label(wContent, SWT.RIGHT);
    wlFileSizeCheck.setText(BaseMessages.getString(PKG, "ActionWaitForFile.FileSizeCheck.Label"));
    PropsUi.setLook(wlFileSizeCheck);
    FormData fdlFileSizeCheck = new FormData();
    fdlFileSizeCheck.left = new FormAttachment(0, 0);
    fdlFileSizeCheck.top = new FormAttachment(wlSuccessOnTimeout, margin);
    fdlFileSizeCheck.right = new FormAttachment(middle, -margin);
    wlFileSizeCheck.setLayoutData(fdlFileSizeCheck);
    wFileSizeCheck = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wFileSizeCheck);
    wFileSizeCheck.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.FileSizeCheck.Tooltip"));
    FormData fdFileSizeCheck = new FormData();
    fdFileSizeCheck.left = new FormAttachment(middle, 0);
    fdFileSizeCheck.top = new FormAttachment(wlFileSizeCheck, 0, SWT.CENTER);
    fdFileSizeCheck.right = new FormAttachment(100, 0);
    wFileSizeCheck.setLayoutData(fdFileSizeCheck);
    wFileSizeCheck.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Add filename to result filenames
    Label wlAddFilenameResult = new Label(wContent, SWT.RIGHT);
    wlAddFilenameResult.setText(
        BaseMessages.getString(PKG, "ActionWaitForFile.AddFilenameResult.Label"));
    PropsUi.setLook(wlAddFilenameResult);
    FormData fdlAddFilenameResult = new FormData();
    fdlAddFilenameResult.left = new FormAttachment(0, 0);
    fdlAddFilenameResult.top = new FormAttachment(wlFileSizeCheck, margin);
    fdlAddFilenameResult.right = new FormAttachment(middle, -margin);
    wlAddFilenameResult.setLayoutData(fdlAddFilenameResult);
    wAddFilenameResult = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wAddFilenameResult);
    wAddFilenameResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionWaitForFile.AddFilenameResult.Tooltip"));
    FormData fdAddFilenameResult = new FormData();
    fdAddFilenameResult.left = new FormAttachment(middle, 0);
    fdAddFilenameResult.top = new FormAttachment(wlAddFilenameResult, 0, SWT.CENTER);
    fdAddFilenameResult.right = new FormAttachment(100, 0);
    wAddFilenameResult.setLayoutData(fdAddFilenameResult);
    wAddFilenameResult.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    sc.setContent(wContent);
    wContent.pack();
    sc.setMinSize(wContent.computeSize(SWT.DEFAULT, SWT.DEFAULT));

    getData();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wFilename.setText(Const.nullToEmpty(action.getFilename()));
    wMaximumTimeout.setText(Const.nullToEmpty(action.getMaximumTimeout()));
    wCheckCycleTime.setText(Const.nullToEmpty(action.getCheckCycleTime()));
    wSuccessOnTimeout.setSelection(action.isSuccessOnTimeout());
    wFileSizeCheck.setSelection(action.isFileSizeCheck());
    wAddFilenameResult.setSelection(action.isAddFilenameToResult());
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
    action.setFilename(wFilename.getText());
    action.setMaximumTimeout(wMaximumTimeout.getText());
    action.setCheckCycleTime(wCheckCycleTime.getText());
    action.setSuccessOnTimeout(wSuccessOnTimeout.getSelection());
    action.setFileSizeCheck(wFileSizeCheck.getSelection());
    action.setAddFilenameToResult(wAddFilenameResult.getSelection());
    dispose();
  }
}
