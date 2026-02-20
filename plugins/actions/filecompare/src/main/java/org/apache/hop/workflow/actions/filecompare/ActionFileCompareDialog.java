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

package org.apache.hop.workflow.actions.filecompare;

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

/** This dialog allows you to edit the File compare action settings. */
public class ActionFileCompareDialog extends ActionDialog {
  private static final Class<?> PKG = ActionFileCompare.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionFileCompare.Filetype.All")};

  private TextVar wFilename1;

  private TextVar wFilename2;

  private ActionFileCompare action;
  private Button wAddFilenameResult;

  private boolean changed;

  public ActionFileCompareDialog(
      Shell parent, ActionFileCompare action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionFileCompare.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionFileCompare.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();

    ModifyListener lsMod = e -> action.setChanged();

    // Filename 1 line
    Label wlFilename1 = new Label(shell, SWT.RIGHT);
    wlFilename1.setText(BaseMessages.getString(PKG, "ActionFileCompare.Filename1.Label"));
    PropsUi.setLook(wlFilename1);
    FormData fdlFilename1 = new FormData();
    fdlFilename1.left = new FormAttachment(0, margin);
    fdlFilename1.top = new FormAttachment(wSpacer, margin);
    fdlFilename1.right = new FormAttachment(middle, -margin);
    wlFilename1.setLayoutData(fdlFilename1);
    Button wbFilename1 = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename1);
    wbFilename1.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename1 = new FormData();
    fdbFilename1.right = new FormAttachment(100, -margin);
    fdbFilename1.top = new FormAttachment(wlFilename1, 0, SWT.CENTER);
    wbFilename1.setLayoutData(fdbFilename1);
    wFilename1 = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename1);
    wFilename1.addModifyListener(lsMod);
    FormData fdFilename1 = new FormData();
    fdFilename1.left = new FormAttachment(middle, 0);
    fdFilename1.top = new FormAttachment(wlFilename1, 0, SWT.CENTER);
    fdFilename1.right = new FormAttachment(wbFilename1, -margin);
    wFilename1.setLayoutData(fdFilename1);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename1.addModifyListener(
        e -> wFilename1.setToolTipText(variables.resolve(wFilename1.getText())));

    wbFilename1.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename1, variables, new String[] {"*"}, FILETYPES, false));

    // Filename 2 line
    Label wlFilename2 = new Label(shell, SWT.RIGHT);
    wlFilename2.setText(BaseMessages.getString(PKG, "ActionFileCompare.Filename2.Label"));
    PropsUi.setLook(wlFilename2);
    FormData fdlFilename2 = new FormData();
    fdlFilename2.left = new FormAttachment(0, margin);
    fdlFilename2.top = new FormAttachment(wFilename1, margin);
    fdlFilename2.right = new FormAttachment(middle, -margin);
    wlFilename2.setLayoutData(fdlFilename2);
    Button wbFilename2 = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename2);
    wbFilename2.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename2 = new FormData();
    fdbFilename2.right = new FormAttachment(100, -margin);
    fdbFilename2.top = new FormAttachment(wlFilename2, 0, SWT.CENTER);
    wbFilename2.setLayoutData(fdbFilename2);
    wFilename2 = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename2);
    wFilename2.addModifyListener(lsMod);
    FormData fdFilename2 = new FormData();
    fdFilename2.left = new FormAttachment(middle, 0);
    fdFilename2.top = new FormAttachment(wlFilename2, 0, SWT.CENTER);
    fdFilename2.right = new FormAttachment(wbFilename2, -margin);
    wFilename2.setLayoutData(fdFilename2);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename2.addModifyListener(
        e -> wFilename2.setToolTipText(variables.resolve(wFilename2.getText())));

    wbFilename2.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename2, variables, new String[] {"*"}, FILETYPES, false));

    // Add filename to result filenames
    Label wlAddFilenameResult = new Label(shell, SWT.RIGHT);
    wlAddFilenameResult.setText(
        BaseMessages.getString(PKG, "ActionFileCompare.AddFilenameResult.Label"));
    PropsUi.setLook(wlAddFilenameResult);
    FormData fdlAddFilenameResult = new FormData();
    fdlAddFilenameResult.left = new FormAttachment(0, margin);
    fdlAddFilenameResult.top = new FormAttachment(wbFilename2, margin);
    fdlAddFilenameResult.right = new FormAttachment(middle, -margin);
    wlAddFilenameResult.setLayoutData(fdlAddFilenameResult);
    wAddFilenameResult = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wAddFilenameResult);
    wAddFilenameResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionFileCompare.AddFilenameResult.Tooltip"));
    FormData fdAddFilenameResult = new FormData();
    fdAddFilenameResult.left = new FormAttachment(middle, 0);
    fdAddFilenameResult.top = new FormAttachment(wlAddFilenameResult, 0, SWT.CENTER);
    fdAddFilenameResult.right = new FormAttachment(100, -margin);
    wAddFilenameResult.setLayoutData(fdAddFilenameResult);
    wAddFilenameResult.addSelectionListener(
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
    if (action.getFilename1() != null) {
      wFilename1.setText(action.getFilename1());
    }
    if (action.getFilename2() != null) {
      wFilename2.setText(action.getFilename2());
    }
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
    action.setFilename1(wFilename1.getText());
    action.setFilename2(wFilename2.getText());
    action.setAddFilenameToResult(wAddFilenameResult.getSelection());
    dispose();
  }
}
