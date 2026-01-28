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

package org.apache.hop.workflow.actions.deleteresultfilenames;

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
public class ActionDeleteResultFilenamesDialog extends ActionDialog {
  private static final Class<?> PKG = ActionDeleteResultFilenames.class;

  private Button wSpecifyWildcard;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Label wlWildcardExclude;
  private TextVar wWildcardExclude;

  private ActionDeleteResultFilenames action;

  private boolean changed;

  public ActionDeleteResultFilenamesDialog(
      Shell parent,
      ActionDeleteResultFilenames action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;

    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDeleteResultFilenames.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionDeleteResultFilenames.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    // Specify wildcard?
    Label wlSpecifyWildcard = new Label(shell, SWT.RIGHT);
    wlSpecifyWildcard.setText(
        BaseMessages.getString(PKG, "ActionDeleteResultFilenames.SpecifyWildcard.Label"));
    PropsUi.setLook(wlSpecifyWildcard);
    FormData fdlSpecifyWildcard = new FormData();
    fdlSpecifyWildcard.left = new FormAttachment(0, 0);
    fdlSpecifyWildcard.top = new FormAttachment(wSpacer, margin);
    fdlSpecifyWildcard.right = new FormAttachment(middle, -margin);
    wlSpecifyWildcard.setLayoutData(fdlSpecifyWildcard);
    wSpecifyWildcard = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wSpecifyWildcard);
    wSpecifyWildcard.setToolTipText(
        BaseMessages.getString(PKG, "ActionDeleteResultFilenames.SpecifyWildcard.Tooltip"));
    FormData fdSpecifyWildcard = new FormData();
    fdSpecifyWildcard.left = new FormAttachment(middle, 0);
    fdSpecifyWildcard.top = new FormAttachment(wlSpecifyWildcard, 0, SWT.CENTER);
    fdSpecifyWildcard.right = new FormAttachment(100, 0);
    wSpecifyWildcard.setLayoutData(fdSpecifyWildcard);
    wSpecifyWildcard.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            CheckLimit();
          }
        });

    // Wildcard line
    wlWildcard = new Label(shell, SWT.RIGHT);
    wlWildcard.setText(BaseMessages.getString(PKG, "ActionDeleteResultFilenames.Wildcard.Label"));
    PropsUi.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment(0, 0);
    fdlWildcard.top = new FormAttachment(wlSpecifyWildcard, margin);
    fdlWildcard.right = new FormAttachment(middle, -margin);
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWildcard.setToolTipText(
        BaseMessages.getString(PKG, "ActionDeleteResultFilenames.Wildcard.Tooltip"));
    PropsUi.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(middle, 0);
    fdWildcard.top = new FormAttachment(wlSpecifyWildcard, margin);
    fdWildcard.right = new FormAttachment(100, -margin);
    wWildcard.setLayoutData(fdWildcard);

    // Whenever something changes, set the tooltip to the expanded version:
    wWildcard.addModifyListener(
        e -> wWildcard.setToolTipText(variables.resolve(wWildcard.getText())));

    // wWildcardExclude
    wlWildcardExclude = new Label(shell, SWT.RIGHT);
    wlWildcardExclude.setText(
        BaseMessages.getString(PKG, "ActionDeleteResultFilenames.WildcardExclude.Label"));
    PropsUi.setLook(wlWildcardExclude);
    FormData fdlWildcardExclude = new FormData();
    fdlWildcardExclude.left = new FormAttachment(0, 0);
    fdlWildcardExclude.top = new FormAttachment(wWildcard, margin);
    fdlWildcardExclude.right = new FormAttachment(middle, -margin);
    wlWildcardExclude.setLayoutData(fdlWildcardExclude);
    wWildcardExclude = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWildcardExclude.setToolTipText(
        BaseMessages.getString(PKG, "ActionDeleteResultFilenames.WildcardExclude.Tooltip"));
    PropsUi.setLook(wWildcardExclude);
    wWildcardExclude.addModifyListener(lsMod);
    FormData fdWildcardExclude = new FormData();
    fdWildcardExclude.left = new FormAttachment(middle, 0);
    fdWildcardExclude.top = new FormAttachment(wWildcard, margin);
    fdWildcardExclude.right = new FormAttachment(100, -margin);
    wWildcardExclude.setLayoutData(fdWildcardExclude);
    // Whenever something changes, set the tooltip to the expanded version:
    wWildcardExclude.addModifyListener(
        e -> wWildcardExclude.setToolTipText(variables.resolve(wWildcardExclude.getText())));

    getData();
    CheckLimit();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void CheckLimit() {
    wlWildcard.setEnabled(wSpecifyWildcard.getSelection());
    wWildcard.setEnabled(wSpecifyWildcard.getSelection());
    wlWildcardExclude.setEnabled(wSpecifyWildcard.getSelection());
    wWildcardExclude.setEnabled(wSpecifyWildcard.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    wSpecifyWildcard.setSelection(action.isSpecifyWildcard());
    if (action.getWildcard() != null) {
      wWildcard.setText(action.getWildcard());
    }
    if (action.getWildcardExclude() != null) {
      wWildcardExclude.setText(action.getWildcardExclude());
    }
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
    action.setSpecifyWildcard(wSpecifyWildcard.getSelection());
    action.setWildcard(wWildcard.getText());
    action.setWildcardExclude(wWildcardExclude.getText());

    dispose();
  }
}
