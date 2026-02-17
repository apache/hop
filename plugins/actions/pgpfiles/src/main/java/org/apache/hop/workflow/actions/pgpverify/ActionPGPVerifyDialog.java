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

package org.apache.hop.workflow.actions.pgpverify;

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
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This defines a PGP verify action. */
public class ActionPGPVerifyDialog extends ActionDialog {
  private static final Class<?> PKG = ActionPGPVerify.class;

  private static final String[] EXTENSIONS = new String[] {"*"};

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionPGPVerify.Filetype.All")};
  public static final String CONST_SYSTEM_BUTTON_BROWSE = "System.Button.Browse";

  private TextVar wGPGLocation;

  private TextVar wFilename;

  private Button wUseDetachedSignature;

  private Label wlDetachedFilename;

  private Button wbDetachedFilename;

  private TextVar wDetachedFilename;

  private ActionPGPVerify action;

  private boolean changed;

  public ActionPGPVerifyDialog(
      Shell parent, ActionPGPVerify action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionPGPVerify.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionPGPVerify.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    Group wSettings = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "ActionPGPVerify.Settings.Group.Label"));

    FormLayout settingsgroupLayout = new FormLayout();
    settingsgroupLayout.marginWidth = 10;
    settingsgroupLayout.marginHeight = 10;

    wSettings.setLayout(settingsgroupLayout);

    // GPGLocation line
    Label wlGPGLocation = new Label(wSettings, SWT.RIGHT);
    wlGPGLocation.setText(BaseMessages.getString(PKG, "ActionPGPVerify.GPGLocation.Label"));
    PropsUi.setLook(wlGPGLocation);
    FormData fdlGPGLocation = new FormData();
    fdlGPGLocation.left = new FormAttachment(0, 0);
    fdlGPGLocation.top = new FormAttachment(0, margin);
    fdlGPGLocation.right = new FormAttachment(middle, -margin);
    wlGPGLocation.setLayoutData(fdlGPGLocation);

    Button wbGPGLocation = new Button(wSettings, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbGPGLocation);
    wbGPGLocation.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbGPGLocation = new FormData();
    fdbGPGLocation.right = new FormAttachment(100, 0);
    fdbGPGLocation.top = new FormAttachment(0, margin);
    wbGPGLocation.setLayoutData(fdbGPGLocation);

    wGPGLocation = new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wGPGLocation);
    wGPGLocation.addModifyListener(lsMod);
    FormData fdGPGLocation = new FormData();
    fdGPGLocation.left = new FormAttachment(middle, 0);
    fdGPGLocation.top = new FormAttachment(0, margin);
    fdGPGLocation.right = new FormAttachment(wbGPGLocation, -margin);
    wGPGLocation.setLayoutData(fdGPGLocation);

    // Filename line
    Label wlFilename = new Label(wSettings, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ActionPGPVerify.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wGPGLocation, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(wSettings, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wGPGLocation, margin);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wGPGLocation, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    Label wlUseDetachedSignature = new Label(wSettings, SWT.RIGHT);
    wlUseDetachedSignature.setText(
        BaseMessages.getString(PKG, "ActionPGPVerify.useDetachedSignature.Label"));
    PropsUi.setLook(wlUseDetachedSignature);
    FormData fdlUseDetachedSignature = new FormData();
    fdlUseDetachedSignature.left = new FormAttachment(0, 0);
    fdlUseDetachedSignature.top = new FormAttachment(wFilename, margin);
    fdlUseDetachedSignature.right = new FormAttachment(middle, -margin);
    wlUseDetachedSignature.setLayoutData(fdlUseDetachedSignature);
    wUseDetachedSignature = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wUseDetachedSignature);
    wUseDetachedSignature.setToolTipText(
        BaseMessages.getString(PKG, "ActionPGPVerify.useDetachedSignature.Tooltip"));
    FormData fdUseDetachedSignature = new FormData();
    fdUseDetachedSignature.left = new FormAttachment(middle, 0);
    fdUseDetachedSignature.top = new FormAttachment(wFilename, margin);
    fdUseDetachedSignature.right = new FormAttachment(100, -margin);
    wUseDetachedSignature.setLayoutData(fdUseDetachedSignature);
    wUseDetachedSignature.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {

            enableDetachedSignature();
          }
        });

    // DetachedFilename line
    wlDetachedFilename = new Label(wSettings, SWT.RIGHT);
    wlDetachedFilename.setText(
        BaseMessages.getString(PKG, "ActionPGPVerify.DetachedFilename.Label"));
    PropsUi.setLook(wlDetachedFilename);
    FormData fdlDetachedFilename = new FormData();
    fdlDetachedFilename.left = new FormAttachment(0, 0);
    fdlDetachedFilename.top = new FormAttachment(wlUseDetachedSignature, margin);
    fdlDetachedFilename.right = new FormAttachment(middle, -margin);
    wlDetachedFilename.setLayoutData(fdlDetachedFilename);

    wbDetachedFilename = new Button(wSettings, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbDetachedFilename);
    wbDetachedFilename.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbDetachedFilename = new FormData();
    fdbDetachedFilename.right = new FormAttachment(100, 0);
    fdbDetachedFilename.top = new FormAttachment(wlDetachedFilename, 0, SWT.CENTER);
    wbDetachedFilename.setLayoutData(fdbDetachedFilename);

    wDetachedFilename = new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDetachedFilename);
    wDetachedFilename.addModifyListener(lsMod);
    FormData fdDetachedFilename = new FormData();
    fdDetachedFilename.left = new FormAttachment(middle, 0);
    fdDetachedFilename.top = new FormAttachment(wlDetachedFilename, 0, SWT.CENTER);
    fdDetachedFilename.right = new FormAttachment(wbDetachedFilename, -margin);
    wDetachedFilename.setLayoutData(fdDetachedFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wDetachedFilename.addModifyListener(
        e -> wDetachedFilename.setToolTipText(variables.resolve(wDetachedFilename.getText())));

    wbDetachedFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wDetachedFilename, variables, EXTENSIONS, FILETYPES, false));

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));
    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename, variables, EXTENSIONS, FILETYPES, false));

    // Whenever something changes, set the tooltip to the expanded version:
    wGPGLocation.addModifyListener(
        e -> wGPGLocation.setToolTipText(variables.resolve(wGPGLocation.getText())));
    wbGPGLocation.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wGPGLocation, variables, EXTENSIONS, FILETYPES, false));

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(wSpacer, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    fdSettings.bottom = new FormAttachment(wCancel, -margin);
    wSettings.setLayoutData(fdSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    getData();
    focusActionName();
    enableDetachedSignature();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));
    if (action.getGPGLocation() != null) {
      wGPGLocation.setText(action.getGPGLocation());
    }
    if (action.getFilename() != null) {
      wFilename.setText(action.getFilename());
    }
    if (action.getDetachedfilename() != null) {
      wDetachedFilename.setText(action.getDetachedfilename());
    }
    wUseDetachedSignature.setSelection(action.useDetachedfilename());
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
    action.setGPGLocation(wGPGLocation.getText());
    action.setFilename(wFilename.getText());
    action.setDetachedfilename(wDetachedFilename.getText());
    action.setUseDetachedfilename(wUseDetachedSignature.getSelection());
    dispose();
  }

  private void enableDetachedSignature() {
    wlDetachedFilename.setEnabled(wUseDetachedSignature.getSelection());
    wDetachedFilename.setEnabled(wUseDetachedSignature.getSelection());
    wbDetachedFilename.setEnabled(wUseDetachedSignature.getSelection());
  }
}
