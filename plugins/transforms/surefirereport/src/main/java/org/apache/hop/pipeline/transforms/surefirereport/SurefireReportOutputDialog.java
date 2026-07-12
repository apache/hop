/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.surefirereport;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class SurefireReportOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SurefireReportOutputMeta.class;

  private final SurefireReportOutputMeta input;
  private boolean gotPreviousFields;

  private TextVar wFilename;
  private TextVar wSuiteName;
  private Button wCreateParentFolder;
  private CCombo wTestNameField;
  private CCombo wDurationField;
  private Button wDurationInMilliseconds;
  private CCombo wResultField;
  private CCombo wSystemOutField;
  private CCombo wSystemErrField;
  private CCombo wFailureMessageField;
  private CCombo wFailureTypeField;
  private Button wFailOnTestFailure;

  public SurefireReportOutputDialog(
      Shell parent,
      IVariables variables,
      SurefireReportOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SurefireReportOutputDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Control lastControl = wSpacer;

    // Report filename
    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "SurefireReportOutputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    fdlFilename.top = new FormAttachment(lastControl, margin);
    wlFilename.setLayoutData(fdlFilename);

    wFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFilename.setToolTipText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.Filename.Tooltip"));
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(lastControl, margin);
    fdFilename.right = new FormAttachment(100, 0);
    wFilename.setLayoutData(fdFilename);
    lastControl = wFilename;

    // Suite name
    Label wlSuiteName = new Label(shell, SWT.RIGHT);
    wlSuiteName.setText(BaseMessages.getString(PKG, "SurefireReportOutputDialog.SuiteName.Label"));
    PropsUi.setLook(wlSuiteName);
    FormData fdlSuiteName = new FormData();
    fdlSuiteName.left = new FormAttachment(0, 0);
    fdlSuiteName.right = new FormAttachment(middle, -margin);
    fdlSuiteName.top = new FormAttachment(lastControl, margin);
    wlSuiteName.setLayoutData(fdlSuiteName);

    wSuiteName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSuiteName.setToolTipText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.SuiteName.Tooltip"));
    PropsUi.setLook(wSuiteName);
    wSuiteName.addModifyListener(lsMod);
    FormData fdSuiteName = new FormData();
    fdSuiteName.left = new FormAttachment(middle, 0);
    fdSuiteName.top = new FormAttachment(lastControl, margin);
    fdSuiteName.right = new FormAttachment(100, 0);
    wSuiteName.setLayoutData(fdSuiteName);
    lastControl = wSuiteName;

    // Create parent folder
    Label wlCreateParentFolder = new Label(shell, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    fdlCreateParentFolder.top = new FormAttachment(lastControl, margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);

    wCreateParentFolder = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wCreateParentFolder);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.CreateParentFolder.Tooltip"));
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment(middle, 0);
    fdCreateParentFolder.top = new FormAttachment(wlCreateParentFolder, 0, SWT.CENTER);
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener(new ComponentSelectionListener(input));
    lastControl = wlCreateParentFolder;

    // Fields group
    Group wFields = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wFields);
    wFields.setText(BaseMessages.getString(PKG, "SurefireReportOutputDialog.Fields.Group.Label"));
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 10;
    fieldsLayout.marginHeight = 10;
    wFields.setLayout(fieldsLayout);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(lastControl, margin * 2);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    Control fieldLast = null;

    fieldLast =
        addFieldRow(
            wFields, fieldLast, "SurefireReportOutputDialog.TestNameField.Label", lsMod, true);
    wTestNameField = (CCombo) fieldLast;

    fieldLast =
        addFieldRow(
            wFields, fieldLast, "SurefireReportOutputDialog.DurationField.Label", lsMod, false);
    wDurationField = (CCombo) fieldLast;

    Label wlDurationMs = new Label(wFields, SWT.RIGHT);
    wlDurationMs.setText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.DurationInMilliseconds.Label"));
    PropsUi.setLook(wlDurationMs);
    FormData fdlDurationMs = new FormData();
    fdlDurationMs.left = new FormAttachment(0, 0);
    fdlDurationMs.right = new FormAttachment(middle, -margin);
    fdlDurationMs.top = new FormAttachment(fieldLast, margin);
    wlDurationMs.setLayoutData(fdlDurationMs);

    wDurationInMilliseconds = new Button(wFields, SWT.CHECK);
    PropsUi.setLook(wDurationInMilliseconds);
    wDurationInMilliseconds.setToolTipText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.DurationInMilliseconds.Tooltip"));
    FormData fdDurationMs = new FormData();
    fdDurationMs.left = new FormAttachment(middle, 0);
    fdDurationMs.top = new FormAttachment(wlDurationMs, 0, SWT.CENTER);
    wDurationInMilliseconds.setLayoutData(fdDurationMs);
    wDurationInMilliseconds.addSelectionListener(new ComponentSelectionListener(input));
    fieldLast = wlDurationMs;

    fieldLast =
        addFieldRow(
            wFields, fieldLast, "SurefireReportOutputDialog.ResultField.Label", lsMod, false);
    wResultField = (CCombo) fieldLast;
    wResultField.setToolTipText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.ResultField.Tooltip"));

    fieldLast =
        addFieldRow(
            wFields, fieldLast, "SurefireReportOutputDialog.SystemOutField.Label", lsMod, false);
    wSystemOutField = (CCombo) fieldLast;

    fieldLast =
        addFieldRow(
            wFields, fieldLast, "SurefireReportOutputDialog.SystemErrField.Label", lsMod, false);
    wSystemErrField = (CCombo) fieldLast;

    fieldLast =
        addFieldRow(
            wFields,
            fieldLast,
            "SurefireReportOutputDialog.FailureMessageField.Label",
            lsMod,
            false);
    wFailureMessageField = (CCombo) fieldLast;

    fieldLast =
        addFieldRow(
            wFields, fieldLast, "SurefireReportOutputDialog.FailureTypeField.Label", lsMod, false);
    wFailureTypeField = (CCombo) fieldLast;

    Label wlFailOnTestFailure = new Label(wFields, SWT.RIGHT);
    wlFailOnTestFailure.setText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.FailOnTestFailure.Label"));
    PropsUi.setLook(wlFailOnTestFailure);
    FormData fdlFailOnTestFailure = new FormData();
    fdlFailOnTestFailure.left = new FormAttachment(0, 0);
    fdlFailOnTestFailure.right = new FormAttachment(middle, -margin);
    fdlFailOnTestFailure.top = new FormAttachment(fieldLast, margin);
    wlFailOnTestFailure.setLayoutData(fdlFailOnTestFailure);

    wFailOnTestFailure = new Button(wFields, SWT.CHECK);
    PropsUi.setLook(wFailOnTestFailure);
    wFailOnTestFailure.setToolTipText(
        BaseMessages.getString(PKG, "SurefireReportOutputDialog.FailOnTestFailure.Tooltip"));
    FormData fdFailOnTestFailure = new FormData();
    fdFailOnTestFailure.left = new FormAttachment(middle, 0);
    fdFailOnTestFailure.top = new FormAttachment(wlFailOnTestFailure, 0, SWT.CENTER);
    wFailOnTestFailure.setLayoutData(fdFailOnTestFailure);
    wFailOnTestFailure.addSelectionListener(new ComponentSelectionListener(input));

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private CCombo addFieldRow(
      Group parent, Control above, String labelKey, ModifyListener lsMod, boolean first) {
    Label wl = new Label(parent, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    if (above == null) {
      fdl.top = new FormAttachment(0, margin);
    } else {
      fdl.top = new FormAttachment(above, margin);
    }
    wl.setLayoutData(fdl);

    CCombo w = new CCombo(parent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(w);
    w.addModifyListener(lsMod);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    if (above == null) {
      fd.top = new FormAttachment(0, margin);
    } else {
      fd.top = new FormAttachment(above, margin);
    }
    fd.right = new FormAttachment(100, 0);
    w.setLayoutData(fd);
    w.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // no-op
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            getFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });
    return w;
  }

  private void getFields() {
    if (gotPreviousFields) {
      return;
    }
    try {
      String[] fields = new String[0];
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        fields = r.getFieldNames();
      }
      gotPreviousFields = true;
      setComboValues(wTestNameField, fields);
      setComboValues(wDurationField, fields);
      setComboValues(wResultField, fields);
      setComboValues(wSystemOutField, fields);
      setComboValues(wSystemErrField, fields);
      setComboValues(wFailureMessageField, fields);
      setComboValues(wFailureTypeField, fields);
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SurefireReportOutputDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "SurefireReportOutputDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void setComboValues(CCombo combo, String[] fields) {
    if (combo == null) {
      return;
    }
    String current = combo.getText();
    combo.setItems(fields);
    if (!Utils.isEmpty(current)) {
      combo.setText(current);
    }
  }

  public void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wFilename.setText(Const.NVL(input.getFilename(), ""));
    wSuiteName.setText(Const.NVL(input.getSuiteName(), ""));
    wCreateParentFolder.setSelection(input.isCreateParentFolder());
    wTestNameField.setText(Const.NVL(input.getTestNameField(), ""));
    wDurationField.setText(Const.NVL(input.getDurationField(), ""));
    wDurationInMilliseconds.setSelection(input.isDurationInMilliseconds());
    wResultField.setText(Const.NVL(input.getResultField(), ""));
    wSystemOutField.setText(Const.NVL(input.getSystemOutField(), ""));
    wSystemErrField.setText(Const.NVL(input.getSystemErrField(), ""));
    wFailureMessageField.setText(Const.NVL(input.getFailureMessageField(), ""));
    wFailureTypeField.setText(Const.NVL(input.getFailureTypeField(), ""));
    wFailOnTestFailure.setSelection(input.isFailOnTestFailure());
    wTransformName.selectAll();
    wTransformName.setFocus();
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
    transformName = wTransformName.getText();
    input.setFilename(wFilename.getText());
    input.setSuiteName(wSuiteName.getText());
    input.setCreateParentFolder(wCreateParentFolder.getSelection());
    input.setTestNameField(wTestNameField.getText());
    input.setDurationField(wDurationField.getText());
    input.setDurationInMilliseconds(wDurationInMilliseconds.getSelection());
    input.setResultField(wResultField.getText());
    input.setSystemOutField(wSystemOutField.getText());
    input.setSystemErrField(wSystemErrField.getText());
    input.setFailureMessageField(wFailureMessageField.getText());
    input.setFailureTypeField(wFailureTypeField.getText());
    input.setFailOnTestFailure(wFailOnTestFailure.getSelection());
    dispose();
  }
}
