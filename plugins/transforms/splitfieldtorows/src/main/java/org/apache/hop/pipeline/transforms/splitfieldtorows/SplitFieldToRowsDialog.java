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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class SplitFieldToRowsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SplitFieldToRowsMeta.class;

  private ComboVar wSplitField;

  private TextVar wDelimiter;

  private TextVar wValName;

  private Button wInclRownum;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;

  private Button wResetRownum;

  private final SplitFieldToRowsMeta input;

  private Button wDelimiterIsRegex;

  public SplitFieldToRowsDialog(
      Shell parent,
      IVariables variables,
      SplitFieldToRowsMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SplitFieldToRowsDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Typefield line
    Label wlSplitfield = new Label(shell, SWT.RIGHT);
    wlSplitfield.setText(BaseMessages.getString(PKG, "SplitFieldToRowsDialog.SplitField.Label"));
    PropsUi.setLook(wlSplitfield);
    FormData fdlSplitfield = new FormData();
    fdlSplitfield.left = new FormAttachment(0, 0);
    fdlSplitfield.right = new FormAttachment(middle, -margin);
    fdlSplitfield.top = new FormAttachment(wSpacer, margin);
    wlSplitfield.setLayoutData(fdlSplitfield);

    wSplitField = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSplitField.setToolTipText(
        BaseMessages.getString(PKG, "SplitFieldToRowsDialog.UrlField.Tooltip"));
    PropsUi.setLook(wSplitField);
    wSplitField.addModifyListener(lsMod);
    FormData fdSplitfield = new FormData();
    fdSplitfield.left = new FormAttachment(middle, 0);
    fdSplitfield.top = new FormAttachment(wSpacer, margin);
    fdSplitfield.right = new FormAttachment(100, 0);
    wSplitField.setLayoutData(fdSplitfield);
    wSplitField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // do nothing when focus is lost
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            BaseTransformDialog.getFieldsFromPrevious(
                variables, wSplitField, pipelineMeta, transformMeta);
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // Delimiter line
    Label wlDelimiter = new Label(shell, SWT.RIGHT);
    wlDelimiter.setText(BaseMessages.getString(PKG, "SplitFieldToRowsDialog.Delimiter.Label"));
    PropsUi.setLook(wlDelimiter);
    FormData fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment(0, 0);
    fdlDelimiter.right = new FormAttachment(middle, -margin);
    fdlDelimiter.top = new FormAttachment(wSplitField, margin);
    wlDelimiter.setLayoutData(fdlDelimiter);
    wDelimiter = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDelimiter.setText("");
    PropsUi.setLook(wDelimiter);
    wDelimiter.addModifyListener(lsMod);
    FormData fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment(middle, 0);
    fdDelimiter.top = new FormAttachment(wSplitField, margin);
    fdDelimiter.right = new FormAttachment(100, 0);
    wDelimiter.setLayoutData(fdDelimiter);

    // Add File to the result files name
    Label wlDelimiterIsRegex = new Label(shell, SWT.RIGHT);
    wlDelimiterIsRegex.setText(
        BaseMessages.getString(PKG, "SplitFieldToRowsDialog.DelimiterIsRegex.Label"));
    PropsUi.setLook(wlDelimiterIsRegex);
    FormData fdlDelimiterIsRegex = new FormData();
    fdlDelimiterIsRegex.left = new FormAttachment(0, 0);
    fdlDelimiterIsRegex.top = new FormAttachment(wDelimiter, margin);
    fdlDelimiterIsRegex.right = new FormAttachment(middle, -margin);
    wlDelimiterIsRegex.setLayoutData(fdlDelimiterIsRegex);
    wDelimiterIsRegex = new Button(shell, SWT.CHECK);
    wDelimiterIsRegex.setToolTipText(
        BaseMessages.getString(PKG, "SplitFieldToRowsDialog.DelimiterIsRegex.Tooltip"));
    PropsUi.setLook(wDelimiterIsRegex);
    FormData fdDelimiterIsRegex = new FormData();
    fdDelimiterIsRegex.left = new FormAttachment(middle, 0);
    fdDelimiterIsRegex.top = new FormAttachment(wlDelimiterIsRegex, 0, SWT.CENTER);
    fdDelimiterIsRegex.right = new FormAttachment(100, 0);
    wDelimiterIsRegex.setLayoutData(fdDelimiterIsRegex);
    SelectionAdapter lsSelR =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wDelimiterIsRegex.addSelectionListener(lsSelR);

    // ValName line
    Label wlValName = new Label(shell, SWT.RIGHT);
    wlValName.setText(BaseMessages.getString(PKG, "SplitFieldToRowsDialog.NewFieldName.Label"));
    PropsUi.setLook(wlValName);
    FormData fdlValName = new FormData();
    fdlValName.left = new FormAttachment(0, 0);
    fdlValName.right = new FormAttachment(middle, -margin);
    fdlValName.top = new FormAttachment(wDelimiterIsRegex, margin);
    wlValName.setLayoutData(fdlValName);
    wValName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wValName.setText("");
    PropsUi.setLook(wValName);
    wValName.addModifyListener(lsMod);
    FormData fdValName = new FormData();
    fdValName.left = new FormAttachment(middle, 0);
    fdValName.right = new FormAttachment(100, 0);
    fdValName.top = new FormAttachment(wDelimiterIsRegex, margin);
    wValName.setLayoutData(fdValName);

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdditionalFields);
    wAdditionalFields.setText(
        BaseMessages.getString(PKG, "SplitFieldToRowsDialog.wAdditionalFields.Label"));

    FormLayout additionalFieldsgroupLayout = new FormLayout();
    additionalFieldsgroupLayout.marginWidth = 10;
    additionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout(additionalFieldsgroupLayout);

    Label wlInclRownum = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRownum.setText(BaseMessages.getString(PKG, "SplitFieldToRowsDialog.InclRownum.Label"));
    PropsUi.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment(0, 0);
    fdlInclRownum.top = new FormAttachment(wValName, margin);
    fdlInclRownum.right = new FormAttachment(middle, -margin);
    wlInclRownum.setLayoutData(fdlInclRownum);
    wInclRownum = new Button(wAdditionalFields, SWT.CHECK);
    PropsUi.setLook(wInclRownum);
    wInclRownum.setToolTipText(
        BaseMessages.getString(PKG, "SplitFieldToRowsDialog.InclRownum.Tooltip"));
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment(middle, 0);
    fdRownum.top = new FormAttachment(wlInclRownum, 0, SWT.CENTER);
    wInclRownum.setLayoutData(fdRownum);
    wInclRownum.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setIncludeRownum();
            input.setChanged();
          }
        });

    wlInclRownumField = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRownumField.setText(
        BaseMessages.getString(PKG, "SplitFieldToRowsDialog.InclRownumField.Label"));
    PropsUi.setLook(wlInclRownumField);
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment(wInclRownum, margin);
    fdlInclRownumField.top = new FormAttachment(wValName, margin);
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRownumField =
        new TextVar(variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclRownumField);
    wInclRownumField.addModifyListener(lsMod);
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment(wlInclRownumField, margin);
    fdInclRownumField.top = new FormAttachment(wValName, margin);
    fdInclRownumField.right = new FormAttachment(100, 0);
    wInclRownumField.setLayoutData(fdInclRownumField);

    wResetRownum = new Button(wAdditionalFields, SWT.CHECK);
    wResetRownum.setText(BaseMessages.getString(PKG, "SplitFieldToRowsDialog.ResetRownum.Label"));
    PropsUi.setLook(wResetRownum);
    wResetRownum.setToolTipText(
        BaseMessages.getString(PKG, "SplitFieldToRowsDialog.ResetRownum.Tooltip"));
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment(wlInclRownum, margin);
    fdRownum.top = new FormAttachment(wInclRownumField, margin);
    wResetRownum.setLayoutData(fdRownum);
    wResetRownum.addSelectionListener(new ComponentSelectionListener(input));

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment(0, margin);
    fdAdditionalFields.top = new FormAttachment(wValName, margin);
    fdAdditionalFields.right = new FormAttachment(100, -margin);
    fdAdditionalFields.bottom = new FormAttachment(wOk, -margin);
    wAdditionalFields.setLayoutData(fdAdditionalFields);

    // ///////////////////////////////
    // END OF Additional Fields GROUP //
    // ///////////////////////////////

    getData();
    setIncludeRownum();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled(wInclRownum.getSelection());
    wInclRownumField.setEnabled(wInclRownum.getSelection());
    wResetRownum.setEnabled(wInclRownum.getSelection());
  }

  public void getData() {
    wSplitField.setText(Const.NVL(input.getSplitField(), ""));
    wDelimiter.setText(Const.NVL(input.getDelimiter(), ""));
    wValName.setText(Const.NVL(input.getNewFieldname(), ""));
    wInclRownum.setSelection(input.isIncludeRowNumber());
    wDelimiterIsRegex.setSelection(input.isIsDelimiterRegex());
    if (input.getRowNumberField() != null) {
      wInclRownumField.setText(input.getRowNumberField());
    }
    wResetRownum.setSelection(input.isResetRowNumber());
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    input.setSplitField(wSplitField.getText());
    input.setDelimiter(wDelimiter.getText());
    input.setNewFieldname(wValName.getText());
    input.setIncludeRowNumber(wInclRownum.getSelection());
    input.setRowNumberField(wInclRownumField.getText());
    input.setResetRowNumber(wResetRownum.getSelection());
    input.setIsDelimiterRegex(wDelimiterIsRegex.getSelection());
    dispose();
  }
}
